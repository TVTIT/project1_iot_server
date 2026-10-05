"""Shared bounded MQTT framing and current-source builds for isolated fixtures."""
import json
import os
from pathlib import Path
import struct
import time
import uuid

from stage2_mosquitto_runtime import ENV, ROOT, mqtt_connection, run

CONTROL = '$CONTROL/dynamic-security/v1'
RESPONSE = CONTROL + '/response'


def require(condition, category):
    if not condition:
        raise RuntimeError(category)


def build_helper(name, directory):
    sources = {'snapshot': 'task260_revoke_reconcile_snapshot.go',
               'gate': 'task260_ingress_gate.go'}
    if name not in sources:
        raise ValueError('unknown fixture helper')
    output = Path(directory).resolve() / name
    require(not output.exists(), 'helper output must be fresh')
    source = Path(__file__).with_name(sources[name]).resolve()
    run(['go', '-C', str(ROOT / 'src'), 'build', '-o', str(output), str(source)],
        env=dict(ENV, CGO_ENABLED='0'), timeout=120)
    output.chmod(0o700)
    print('PASS current-source build: ' + name, flush=True)
    return output


def text(value):
    value = value.encode()
    return struct.pack('!H', len(value)) + value


class MQTT:
    """Bounded MQTT 3.1.1; logical correlated replies, never PUBACK proof."""
    def __init__(self, spike, user, password):
        self.s = mqtt_connection(spike.port, spike.d / 'ca.crt', user, password)
        self.s.settimeout(2)
        self.pid = 0

    def close(self):
        self.s.close()

    def send(self, header, body):
        require(len(body) <= 16384, 'request size bound')
        remaining = bytearray()
        size = len(body)
        while True:
            digit = size % 128
            size //= 128
            remaining.append(digit | (128 if size else 0))
            if not size:
                break
        self.s.sendall(bytes([header]) + remaining + body)

    def exact(self, n):
        data = bytearray()
        while len(data) < n:
            chunk = self.s.recv(n - len(data))
            if not chunk:
                raise EOFError('connection closed')
            data.extend(chunk)
        return bytes(data)

    def packet(self):
        header = self.exact(1)[0]
        size = 0
        for i in range(4):
            digit = self.exact(1)[0]
            size += (digit & 127) << (7 * i)
            if not digit & 128:
                require(size <= 65536, 'response size bound')
                return header, self.exact(size)
        raise RuntimeError('invalid frame')

    def subscribe(self, topic, allowed=True):
        self.pid += 1
        pid = struct.pack('!H', self.pid)
        self.send(0x82, pid + text(topic) + b'\x00')
        header, body = self.packet()
        require(header == 0x90 and body == pid + (b'\x00' if allowed else b'\x80'),
                'exact SUBACK mismatch')

    def publish(self, topic, payload):
        self.send(0x30, text(topic) + payload)

    def message(self):
        header, body = self.packet()
        require(header == 0x30, 'unexpected/retained response frame')
        n = struct.unpack('!H', body[:2])[0]
        return body[2:2+n].decode(), body[2+n:]

    def request(self, command, *, discard=False, error=False, **fields):
        correlation = uuid.uuid4().hex
        payload = json.dumps({'commands': [dict(command=command,
                             correlationData=correlation, **fields)]}).encode()
        self.publish(CONTROL, payload)
        deadline = time.monotonic() + 3
        for _ in range(16):
            self.s.settimeout(max(.01, deadline - time.monotonic()))
            topic, raw = self.message()
            require(topic == RESPONSE, 'wrong response topic')
            responses = json.loads(raw).get('responses', [])
            require(len(responses) <= 16, 'response cardinality bound')
            for response in responses:
                if response.get('correlationData') != correlation:
                    continue
                require(response.get('command') == command, 'response command mismatch')
                require(bool(response.get('error')) == error, 'plugin operation error mismatch')
                self.s.settimeout(2)
                if discard:
                    raise TimeoutError('injected lost application response')
                return response.get('data', {})
            require(time.monotonic() < deadline, 'response deadline')
        raise TimeoutError('response flood bound')
