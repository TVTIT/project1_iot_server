#!/usr/bin/env python3
"""Owned HTTP credential acceptance; native regressions remain independent."""
import os

from stage2_mosquitto_runtime import ENV, ROOT, run
from task26_harness_helpers import require

NGINX = 'nginx@sha256:30f1c0d78e0ad60901648be663a710bdadf19e4c10ac6782c235200619158284'


def verify_selector(output):
    require(b'--- PASS: TestCredentialHTTPIsolated (' in output and
            b'--- SKIP:' not in output and b'--- FAIL:' not in output,
            'HTTP selector skipped, failed or selected zero tests')


def qualify_network(spike, inspected):
    bindings = inspected['HostConfig']['PortBindings']
    require(set(bindings) == {'8883/tcp', '8884/tcp'}, 'only gated listeners published')
    require(all(b['HostIp'] == '127.0.0.1' for values in bindings.values() for b in values),
            'fixture host bindings must be loopback')
    source = spike.setup('cat /fixture/broker.conf').decode()
    require('listener 18884 127.0.0.1' in source and source.count('listener ') == 1,
            'broker private listener only')
    print('PASS isolated network: loopback private broker, only gated TLS host bindings', flush=True)


def start_proxy(directory, network, name, backend, publish=False, labels=()):
    config = directory / 'nginx.conf'
    config.write_text('pid /tmp/nginx.pid; events {} http { access_log off; '
                      'error_log /dev/stderr warn; client_body_temp_path /tmp/body; '
                      'proxy_temp_path /tmp/proxy; fastcgi_temp_path /tmp/fastcgi; '
                      'uwsgi_temp_path /tmp/uwsgi; scgi_temp_path /tmp/scgi; server { listen 8443 ssl; '
                      'ssl_certificate /tls/server.crt; ssl_certificate_key /tls/server.key; '
                      'client_max_body_size 2m; resolver 127.0.0.11 ipv6=off valid=1s; '
                      'location / { set $backend "' + backend + ':8080"; '
                      'proxy_pass http://$backend; proxy_read_timeout 60s; '
                      'proxy_set_header Authorization $http_authorization; } } }')
    config.chmod(0o600)
    run(['docker', 'run', '-d', *labels, '--name', name, '--network', network, *(['-p', '127.0.0.1::8443'] if publish else []), '--user', f'{os.getuid()}:{os.getgid()}',
         '--read-only', '--cap-drop=ALL', '--security-opt=no-new-privileges:true',
         '--tmpfs', '/tmp', '--mount', f'type=bind,src={config},dst=/etc/nginx/nginx.conf,readonly',
         '--mount', f'type=bind,src={directory}/server.crt,dst=/tls/server.crt,readonly',
         '--mount', f'type=bind,src={directory}/server.key,dst=/tls/server.key,readonly',
                      '--entrypoint', 'nginx', NGINX, '-e', '/dev/stderr', '-g', 'daemon off; master_process off;'], capture=True)


def main():
    from task266_provision import main as fixture
    fixture(standalone=True)
    fixture(http=True)
    # These are real service/native fault seams, not HTTP or SQL wire-loss tests.
    for options in ({}, {'rotate': True}, {'revoke': True}, {'startup': True}, {'composition': True}):
        fixture(**options)


if __name__ == '__main__':
    main()
