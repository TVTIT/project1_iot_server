#!/usr/bin/env python3
"""Unit tests for task28_disable_client_probe helper semantics and error handling."""
import socket
import unittest
from unittest.mock import Mock, patch
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent))

from task28_disable_client_probe import exact_connack_code, NativeTargetConnection


class TestProbeSemantics(unittest.TestCase):
    def test_exact_connack_code_success(self):
        mock_sock = Mock()
        mock_sock.recv.side_effect = [b'\x20\x02\x00\x00']
        code = exact_connack_code(mock_sock)
        self.assertEqual(code, 0)

    def test_exact_connack_code_unauthorized(self):
        mock_sock = Mock()
        mock_sock.recv.side_effect = [b'\x20\x02\x00\x05']
        code = exact_connack_code(mock_sock)
        self.assertEqual(code, 5)

    def test_exact_connack_code_chunks(self):
        mock_sock = Mock()
        mock_sock.recv.side_effect = [b'\x20\x02', b'\x00\x04']
        code = exact_connack_code(mock_sock)
        self.assertEqual(code, 4)

    def test_exact_connack_code_premature_eof(self):
        mock_sock = Mock()
        mock_sock.recv.side_effect = [b'\x20\x02', b'']
        with self.assertRaises(EOFError):
            exact_connack_code(mock_sock)

    def test_exact_connack_code_invalid_header(self):
        mock_sock = Mock()
        mock_sock.recv.side_effect = [b'\x30\x02\x00\x00']
        with self.assertRaises(RuntimeError):
            exact_connack_code(mock_sock)

    def test_wait_for_disconnect_eof(self):
        target = object.__new__(NativeTargetConnection)
        target.sock = Mock()
        target.sock.recv.return_value = b''
        disc, detail, elapsed = target.wait_for_disconnect(timeout_sec=1.0)
        self.assertTrue(disc)
        self.assertIn('EOF', detail)

    def test_wait_for_disconnect_reset(self):
        target = object.__new__(NativeTargetConnection)
        target.sock = Mock()
        target.sock.recv.side_effect = ConnectionResetError('Connection reset by peer')
        disc, detail, elapsed = target.wait_for_disconnect(timeout_sec=1.0)
        self.assertTrue(disc)
        self.assertIn('transport reset', detail)

    def test_wait_for_disconnect_timeout(self):
        target = object.__new__(NativeTargetConnection)
        target.sock = Mock()
        target.sock.recv.side_effect = socket.timeout()
        disc, detail, elapsed = target.wait_for_disconnect(timeout_sec=0.1)
        self.assertFalse(disc)
        self.assertIn('timeout', detail)


if __name__ == '__main__':
    unittest.main()
