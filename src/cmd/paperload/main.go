// paperload preflights manifests and runs the bounded synthetic telemetry role.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"iot-platform/internal/paperload"
)

const help = `paperload — bounded P8 manifest preflight

Usage:
  paperload --manifest FILE [--output-dir DIR] --preflight [--official-opt-in]
  paperload --manifest FILE [--output-dir DIR] --run [--official-opt-in]

--preflight / --dry-run validates and prints estimates only. It does not dial
network, start Docker/simulators, create artifacts, or inspect private files.
--run supports telemetry synthetic-load, viewer-only, or control-only REST role.
Viewer/control require a private role_jwts identity file; control uses HTTPS only.
--official-opt-in is required for profile=official.
`

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("paperload", flag.ContinueOnError)
	fs.SetOutput(stderr)
	manifestPath := fs.String("manifest", "", "public workload manifest JSON file")
	outputDir := fs.String("output-dir", "", "fresh workload result output directory (created only by --run)")
	preflight := fs.Bool("preflight", false, "validate and print proposed estimates; no network or writes")
	dryRun := fs.Bool("dry-run", false, "alias for --preflight")
	runFlag := fs.Bool("run", false, "execute selected workload")
	official := fs.Bool("official-opt-in", false, "explicitly permit official profile preflight")
	fs.Usage = func() { _, _ = fmt.Fprint(stdout, help) }
	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		return 2
	}
	if fs.NArg() != 0 {
		_, _ = fmt.Fprintln(stderr, "unexpected positional argument")
		return 2
	}
	if *manifestPath == "" {
		_, _ = fmt.Fprintln(stderr, "--manifest is required")
		return 2
	}
	selectedModes := 0
	if *preflight || *dryRun {
		selectedModes++
	}
	if *runFlag {
		selectedModes++
	}
	if selectedModes != 1 {
		_, _ = fmt.Fprintln(stderr, "select exactly one of --preflight/--dry-run or --run")
		return 2
	}
	file, err := os.Open(*manifestPath)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "manifest could not be read")
		return 2
	}
	defer func() { _ = file.Close() }()
	manifest, err := paperload.Decode(file)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return 2
	}
	estimate, err := paperload.Validate(manifest, *official)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return 2
	}
	if *runFlag {
		if *outputDir == "" {
			_, _ = fmt.Fprintln(stderr, "--output-dir is required for --run")
			return 2
		}
		if len(manifest.Roles) != 1 || (manifest.Roles[0] != "telemetry" && manifest.Roles[0] != "viewer" && manifest.Roles[0] != "control") {
			_, _ = fmt.Fprintln(stderr, "only one telemetry, viewer, or control role is implemented")
			return 3
		}
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		var result any
		var status string
		var runErr error
		switch manifest.Roles[0] {
		case "control":
			v, e := paperload.RunControl(ctx, manifest, *manifestPath, *outputDir, func(event paperload.ControlPhaseEvent) error {
				data, marshalErr := json.Marshal(event)
				if marshalErr != nil {
					return marshalErr
				}
				_, writeErr := fmt.Fprintf(stdout, "P8_PHASE %s\n", data)
				return writeErr
			})
			result, status, runErr = v, v.Status, e
		case "viewer":
			v, e := paperload.RunViewer(ctx, manifest, *manifestPath, *outputDir)
			result, status, runErr = v, v.Status, e
		case "telemetry":
			v, e := paperload.RunTelemetry(ctx, manifest, *manifestPath, *outputDir, func(event paperload.PhaseEvent) error {
				data, marshalErr := json.Marshal(event)
				if marshalErr != nil {
					return marshalErr
				}
				_, writeErr := fmt.Fprintf(stdout, "P8_PHASE %s\n", data)
				return writeErr
			})
			result, status, runErr = v, v.Status, e
		}
		if runErr != nil {
			_, _ = fmt.Fprintln(stderr, "paperload runtime failed; see safe workload artifact")
			return 1
		}
		if status != "COMPLETE" {
			return 4
		}
		encoder := json.NewEncoder(stdout)
		encoder.SetIndent("", "  ")
		if encoder.Encode(result) != nil {
			return 1
		}
		return 0
	}
	result := struct {
		Status         string             `json:"status"`
		Implementation string             `json:"implementation_status"`
		Experiment     string             `json:"experiment"`
		Arm            string             `json:"arm"`
		Profile        string             `json:"profile"`
		Mode           string             `json:"mode"`
		RunID          string             `json:"run_id"`
		Estimate       paperload.Estimate `json:"estimate"`
	}{Status: "PREFLIGHT_ONLY_NOT_RUN", Implementation: "P8.4b telemetry, P8.4c viewer, P8.4.10 control-only REST producer",
		Experiment: manifest.Experiment, Arm: manifest.Arm, Profile: manifest.Profile, Mode: manifest.Mode,
		RunID: manifest.RunID, Estimate: estimate}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(result); err != nil {
		_, _ = fmt.Fprintln(stderr, "preflight output failed")
		return 1
	}
	return 0
}
