import http.client
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer

from stage2_response_loss import ResponseLossRelay


class RelayTests(unittest.TestCase):
    def test_upstream_response_consumed_but_client_receives_eof(self):
        class Upstream(BaseHTTPRequestHandler):
            def do_POST(self):
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b'{"secret_returned":true,"password":"unit-only-secret"}')

            def log_message(self, *_):
                pass

        upstream = HTTPServer(('127.0.0.1', 0), Upstream)
        thread = threading.Thread(target=upstream.serve_forever, daemon=True)
        thread.start()
        try:
            with ResponseLossRelay(f'http://127.0.0.1:{upstream.server_port}') as relay:
                client = http.client.HTTPConnection('127.0.0.1', relay.port, timeout=3)
                client.request('POST', '/credential')
                with self.assertRaises(http.client.RemoteDisconnected):
                    client.getresponse()
                client.close()
                self.assertTrue(relay.completed.wait(3))
                self.assertEqual(relay.status, 200)
                self.assertTrue(relay.secret_observed)
                self.assertEqual(relay.secret, 'unit-only-secret')
        finally:
            upstream.shutdown()
            upstream.server_close()
            thread.join(3)
