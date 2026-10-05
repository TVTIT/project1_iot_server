"""Owned one-request transport sink. Response bytes never reach the caller.

Only status/booleans may be reported. The upstream secret stays in memory for
the old-password rejection oracle; it must never be serialized as evidence.
"""
import http.client
import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlsplit


class ResponseLossRelay:
    def __init__(self, upstream, tls_context=None, timeout=45):
        self.upstream = urlsplit(upstream)
        self.tls_context = tls_context
        self.timeout = timeout
        self.completed = threading.Event()
        self.status = None
        self.secret = None
        self.secret_observed = False
        self.error = None
        owner = self

        class Sink(BaseHTTPRequestHandler):
            def do_POST(self):
                connection = None
                try:
                    length = int(self.headers.get('Content-Length', '0'))
                    if length > 65536:
                        raise ValueError('request bound')
                    body = self.rfile.read(length)
                    headers = {k: v for k, v in self.headers.items()
                               if k.lower() not in ('host', 'connection')}
                    if owner.upstream.scheme == 'https':
                        connection = http.client.HTTPSConnection(
                            owner.upstream.hostname, owner.upstream.port,
                            context=owner.tls_context, timeout=owner.timeout)
                    else:
                        connection = http.client.HTTPConnection(
                            owner.upstream.hostname, owner.upstream.port,
                            timeout=owner.timeout)
                    connection.request('POST', self.path, body=body, headers=headers)
                    response = connection.getresponse()
                    payload = json.loads(response.read(65537))
                    owner.status = response.status
                    owner.secret = payload.get('password')
                    owner.secret_observed = (payload.get('secret_returned') is True
                                             and isinstance(owner.secret, str))
                    # Deliberately send NO status line, headers, or body downstream.
                    self.close_connection = True
                except Exception as error:
                    owner.error = type(error).__name__
                    self.close_connection = True
                finally:
                    if connection:
                        connection.close()
                    owner.completed.set()

            def log_message(self, *_):
                pass

        self.server = HTTPServer(('127.0.0.1', 0), Sink)
        self.port = self.server.server_port
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *_):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(self.timeout + 2)
        if self.thread.is_alive():
            raise RuntimeError('response-loss relay cleanup timeout')
