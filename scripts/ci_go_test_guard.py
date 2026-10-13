#!/usr/bin/env python3
"""CI Go Test Guard.

Validates `go test -json` JSONL output against mandatory required tests.

Requirements & Rules:
- Receives input JSONL file path and one or more --require 'package:TestName'.
- Empty requires list is rejected.
- Tracks events per (Package, Test).
- Must fail (exit non-zero) if:
  * A required test did not have both 'run' and 'pass' events, or had 'skip'/'fail'.
  * A required test or any of its descendant subtests has a 'skip' or 'fail' action, or ran without terminal pass.
  * Malformed/truncated JSON, invalid event types, or unknown actions exist.
  * ANY package appearing in the run fails or is missing a terminal 'pass' action.
  * Only test list or package cached output is present (cached pass without explicit test 'run' is rejected).
- Diagnostics do not print raw Output or unvalidated file paths/specs that may leak secrets or control chars.
- Validates identifiers without arbitrary control characters; supports Unicode subtest identifiers.
- Diagnostics contain only safe identifiers and counts.
- Exits 0 on success, non-zero on failure.
"""

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

VALID_GO_TEST_ACTIONS = {
    "start",
    "run",
    "pause",
    "cont",
    "pass",
    "bench",
    "fail",
    "output",
    "skip",
}


def is_safe_identifier(val: str) -> bool:
    """Check if identifier contains no control characters or newlines."""
    if not isinstance(val, str) or not val:
        return False
    for ch in val:
        # Reject control characters (ord < 32 or 127 <= ord <= 159)
        o = ord(ch)
        if o < 32 or (127 <= o <= 159):
            return False
    return True


def parse_require_spec(spec: str) -> Tuple[str, str]:
    """Parse 'package:TestName' into (package, test_name).

    Does not echo raw input on error to prevent leaking secrets/control chars.
    """
    if not isinstance(spec, str):
        raise ValueError("Requirement specification must be a string.")
    parts = spec.strip().split(":", 1)
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise ValueError("Invalid requirement format (expected 'package:TestName').")
    pkg, test_name = parts[0], parts[1]
    if not is_safe_identifier(pkg) or not is_safe_identifier(test_name):
        raise ValueError("Requirement contains invalid or control characters.")
    return pkg, test_name


def verify_test_log(input_path: str, requires: List[str]) -> Tuple[bool, List[str]]:
    """Parse and verify go test -json output.

    Returns (success: bool, diagnostics: List[str]).
    Diagnostics contain only safe identifiers and counts.
    """
    diagnostics: List[str] = []

    if not requires:
        return False, ["No required tests specified. At least one required test must be provided."]

    # Parse required targets
    req_targets: Set[Tuple[str, str]] = set()
    req_packages: Set[str] = set()
    for idx, req in enumerate(requires):
        try:
            pkg, test_name = parse_require_spec(req)
            req_targets.add((pkg, test_name))
            req_packages.add(pkg)
        except ValueError:
            # Fixed message without echoing raw spec
            diagnostics.append(f"Invalid requirement specification at index {idx}")

    if diagnostics:
        return False, diagnostics

    p = Path(input_path)
    if not p.is_file():
        # Fixed message without echoing raw file path/basename
        return False, ["Input file not found or not accessible"]

    # Tracking data structures:
    all_seen_packages: Set[str] = set()
    pkg_terminal_actions: Dict[str, List[str]] = defaultdict(list)
    test_actions: Dict[Tuple[str, str], Set[str]] = defaultdict(set)
    pkg_tests: Dict[str, Set[str]] = defaultdict(set)

    malformed_lines_count = 0
    invalid_event_count = 0
    total_lines = 0

    try:
        with p.open("r", encoding="utf-8", errors="replace") as f:
            for line in f:
                total_lines += 1
                line_str = line.strip()
                if not line_str:
                    continue
                try:
                    event = json.loads(line_str)
                except Exception:
                    malformed_lines_count += 1
                    continue

                if not isinstance(event, dict):
                    invalid_event_count += 1
                    continue

                action = event.get("Action")
                pkg = event.get("Package")
                test = event.get("Test")

                # Validate types
                if not isinstance(action, str) or action not in VALID_GO_TEST_ACTIONS:
                    invalid_event_count += 1
                    continue

                if not isinstance(pkg, str) or not is_safe_identifier(pkg):
                    invalid_event_count += 1
                    continue

                if test is not None:
                    if not isinstance(test, str) or not is_safe_identifier(test):
                        invalid_event_count += 1
                        continue

                all_seen_packages.add(pkg)

                if test is None:
                    # Package-level terminal event
                    if action in ("pass", "fail", "skip"):
                        pkg_terminal_actions[pkg].append(action)
                else:
                    # Test-level event
                    pkg_tests[pkg].add(test)
                    test_actions[(pkg, test)].add(action)
    except Exception as e:
        return False, [f"Error reading input file: {type(e).__name__}"]

    if malformed_lines_count > 0:
        diagnostics.append(f"Found {malformed_lines_count} malformed JSON lines out of {total_lines}")

    if invalid_event_count > 0:
        diagnostics.append(f"Found {invalid_event_count} invalid event records (type error or unknown action)")

    # (3) ALL packages that appeared in the log must have terminal pass
    for pkg in sorted(all_seen_packages):
        terminals = pkg_terminal_actions.get(pkg, [])
        if not terminals:
            diagnostics.append(f"Package missing terminal pass event: {pkg}")
        elif "fail" in terminals:
            diagnostics.append(f"Package failed: {pkg}")
        elif "pass" not in terminals:
            diagnostics.append(f"Package incomplete terminal status: {pkg}")

    # (1) Check each required test target
    for pkg, test_name in sorted(req_targets):
        actions = test_actions.get((pkg, test_name), set())

        if "run" not in actions:
            if not actions:
                diagnostics.append(f"Required test missing: {pkg}:{test_name} (never executed)")
            else:
                diagnostics.append(f"Required test missing 'run' event: {pkg}:{test_name}")

        # Must not have skip or fail
        if "skip" in actions:
            diagnostics.append(f"Required test skipped: {pkg}:{test_name}")
        if "fail" in actions:
            diagnostics.append(f"Required test failed: {pkg}:{test_name}")

        if "pass" not in actions and "skip" not in actions and "fail" not in actions:
            diagnostics.append(f"Required test missing terminal pass: {pkg}:{test_name}")

        # (2) Check subtests (descendants): any test starting with test_name + "/"
        prefix = test_name + "/"
        for t in pkg_tests.get(pkg, []):
            if t.startswith(prefix):
                sub_actions = test_actions.get((pkg, t), set())
                if "fail" in sub_actions:
                    diagnostics.append(f"Descendant subtest failed: {pkg}:{t}")
                if "skip" in sub_actions:
                    diagnostics.append(f"Descendant subtest skipped: {pkg}:{t}")
                if "run" in sub_actions and "pass" not in sub_actions and "fail" not in sub_actions and "skip" not in sub_actions:
                    diagnostics.append(f"Descendant subtest incomplete (run without pass): {pkg}:{t}")

    success = len(diagnostics) == 0
    return success, diagnostics


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Verify go test -json output against required tests and package health."
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Path to go test -json JSONL output file",
    )
    parser.add_argument(
        "--require",
        action="append",
        default=[],
        help="Required test spec in 'package:TestName' format. Can be specified multiple times.",
    )

    args = parser.parse_args(argv)

    if not args.require:
        print("Error: At least one --require 'package:TestName' must be specified.", file=sys.stderr)
        return 2

    success, diagnostics = verify_test_log(args.input, args.require)
    if not success:
        print("ci_go_test_guard verification FAILED:", file=sys.stderr)
        for diag in diagnostics:
            print(f"  - {diag}", file=sys.stderr)
        return 1

    print(f"ci_go_test_guard verification PASSED ({len(args.require)} required tests verified).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
