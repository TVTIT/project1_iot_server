import test from 'node:test';
import assert from 'node:assert/strict';
import { ReconnectCatchupClient } from '../p7_reconnect_client.mjs';

const target = { gateway_id: 'gw1', sensor_id: 's1' };
const entity = 'urn:ngsi-ld:Sensor:gw1:s1';
const raw = (values) => ({ id: entity, type: 'Sensor', measurement: { type: 'Property', values }, '@context': [] });

function harness(history = raw([[1.25, '2026-10-27T10:00:00.000000Z']]), status = 200) {
  const sockets = [];
  const calls = [];
  class WS {
    constructor(url, protocols) { this.url = url; this.protocols = protocols; this.readyState = 0; sockets.push(this); }
    send(data) { this.sent = JSON.parse(data); }
    close() { this.readyState = 3; this.onclose?.({}); }
    open() { this.readyState = 1; this.onopen?.({}); }
    message(data) { this.onmessage?.({ data: JSON.stringify(data) }); }
  }
  const fetch = async (url, options) => { calls.push({ url, options }); return { status, ok: status === 200, json: async () => history }; };
  const client = new ReconnectCatchupClient({ WebSocket: WS, fetch, maxLive: 2 });
  return { client, sockets, calls };
}

async function subscribe(client, sockets) {
  const p = client.connect({ url: 'wss://api.test/v1/ws', token: 'secret', target });
  const socket = sockets.at(-1); socket.open();
  socket.message({ protocol_version: 1, message_type: 'ack', request_id: socket.sent.request_id, action: 'subscribe', targets: [target] });
  await p;
}

test('connect waits for subscribe ACK; reconnect then performs bounded raw catch-up', async () => {
  const { client, sockets, calls } = harness();
  const connecting = client.connect({ url: 'wss://api.test/v1/ws', token: 'secret', target });
  sockets[0].open();
  sockets[0].message({ protocol_version: 1, message_type: 'ack', request_id: sockets[0].sent.request_id, action: 'subscribe', targets: [target] });
  await connecting;
  assert.equal(client.phase, 'live');
  client.disconnect();
  const connectingAgain = client.connect({ url: 'wss://api.test/v1/ws', token: 'secret', target });
  sockets[1].open();
  sockets[1].message({ protocol_version: 1, message_type: 'ack', request_id: sockets[1].sent.request_id, action: 'subscribe', targets: [target] });
  await connectingAgain;
  const result = await client.catchUp({ entityId: entity, from: '2026-10-27T10:00:00Z', to: '2026-10-27T10:01:00Z', overlapMs: 1000 });
  assert.equal(result.status, 'caught_up');
  assert.equal(result.rows[0].value, 1.25);
  assert.equal(calls[0].options.headers.Authorization, 'Bearer secret');
  assert.equal(calls[0].options.headers.Accept, 'application/ld+json');
  assert.match(calls[0].url, /\/v1\/digital-twins\/urn%3Angsi-ld%3ASensor%3Agw1%3As1\/temporal\?attrs=measurement&timerel=between/);
  assert.equal(client.phase, 'caught_up');
});

test('live buffer is bounded and deduplicates live message/boot/sequence identity', async () => {
  const { client, sockets } = harness();
  const p = client.connect({ url: 'wss://api.test/v1/ws', token: 'x', target }); sockets[0].open();
  sockets[0].message({ protocol_version: 1, message_type: 'ack', request_id: sockets[0].sent.request_id, action: 'subscribe', targets: [target] }); await p;
  const event = (sequence) => ({ protocol_version: 1, message_type: 'telemetry', gateway_id: 'gw1', sensor_id: 's1', boot_id: 'b', first_sequence: String(sequence), sample_count: 1, measured_at: '2026-10-27T10:00:00Z', sample_interval_us: 1000, samples: [sequence] });
  sockets[0].message(event(1)); sockets[0].message(event(1)); sockets[0].message(event(2)); sockets[0].message(event(3));
  assert.equal(client.liveRows.length, 2);
  assert.deepEqual(client.liveRows.map(x => x.identity), ['b:2', 'b:3']);
});

test('history tuples with same timestamp survive; late live arrival requires refresh, never cursor claim', async () => {
  const { client, sockets } = harness(raw([[1, '2026-10-27T10:00:00Z'], [2, '2026-10-27T10:00:00Z']]));
  await subscribe(client, sockets);
  client.noteLateBatch();
  const result = await client.catchUp({ entityId: entity, from: '2026-10-27T10:00:00Z', to: '2026-10-27T10:01:00Z' });
  assert.deepEqual(result.rows.map(x => x.value), [1, 2]);
  assert.equal(result.needsResync, true);
  assert.equal(result.historyExactlyOnce, false);
});

test('rejects malformed bounds/history and auth denial without stale success', async () => {
  const { client, sockets } = harness(raw([]), 403);
  await subscribe(client, sockets);
  await assert.rejects(client.catchUp({ entityId: entity, from: 'bad', to: '2026-10-27T10:01:00Z' }), /invalid_window/);
  await assert.rejects(client.catchUp({ entityId: entity, from: '2026-10-27T10:00:00Z', to: '2026-10-27T10:01:00Z' }), /history_http_403/);
  assert.equal(client.phase, 'failed');
  assert.deepEqual(client.rows, []);
});

test('rejects malformed temporal tuples and 401 without retaining previous rows', async () => {
  const { client, sockets } = harness(raw([[1, 'not-a-time']]));
  await subscribe(client, sockets);
  client.rows = [{ value: 99, timestamp: 'stale' }];
  await assert.rejects(client.catchUp({ entityId: entity, from: '2026-10-27T10:00:00Z', to: '2026-10-27T10:01:00Z' }), /invalid_history_tuple/);
  assert.deepEqual(client.rows, []);

  const denied = harness(raw([]), 401);
  await subscribe(denied.client, denied.sockets);
  await assert.rejects(denied.client.catchUp({ entityId: entity, from: '2026-10-27T10:00:00Z', to: '2026-10-27T10:01:00Z' }), /history_http_401/);
  assert.equal(denied.client.phase, 'failed');
});

test('preserves PostgreSQL sub-millisecond history and microsecond live timestamps', async () => {
  const history = raw([[1, '2026-10-27T10:00:00.123456Z'], [2, '2026-10-27T10:00:00.123789Z']]);
  const { client, sockets } = harness(history);
  await subscribe(client, sockets);
  const result = await client.catchUp({ entityId: entity, from: '2026-10-27T10:00:00Z', to: '2026-10-27T10:01:00Z' });
  assert.deepEqual(result.rows.map(row => row.timestamp), ['2026-10-27T10:00:00.123456Z', '2026-10-27T10:00:00.123789Z']);
  const event = { gateway_id: 'gw1', sensor_id: 's1', boot_id: 'b', first_sequence: '1', sample_count: 2, measured_at: '2026-10-27T10:00:00.123456Z', sample_interval_us: 333, samples: [1, 2] };
  sockets[0].message({ protocol_version: 1, message_type: 'telemetry', ...event });
  assert.deepEqual(client.liveRows.map(row => row.timestamp), ['2026-10-27T10:00:00.123456Z', '2026-10-27T10:00:00.123789Z']);
});

test('connect timeout closes its socket; stale sockets and catch-up responses cannot overwrite a replacement', async () => {
  const sockets = [];
  let resolveFetch;
  class WS {
    constructor() { this.readyState = 0; sockets.push(this); }
    send(data) { this.sent = JSON.parse(data); }
    close() { this.readyState = 3; this.onclose?.({}); }
    open() { this.readyState = 1; this.onopen?.({}); }
    message(data) { this.onmessage?.({ data: JSON.stringify(data) }); }
  }
  const client = new ReconnectCatchupClient({ WebSocket: WS, fetch: () => new Promise(resolve => { resolveFetch = resolve; }), connectTimeoutMs: 15 });
  const timedOut = client.connect({ url: 'wss://api.test/v1/ws', token: 'secret', target });
  await assert.rejects(timedOut, /websocket_ack_timeout/);
  assert.equal(sockets[0].readyState, 3);
  const connecting = client.connect({ url: 'wss://api.test/v1/ws', token: 'new-token', target });
  sockets[1].open(); sockets[1].message({ message_type: 'ack', request_id: sockets[1].sent.request_id, action: 'subscribe' }); await connecting;
  const pending = client.catchUp({ entityId: entity, from: '2026-10-27T10:00:00Z', to: '2026-10-27T10:01:00Z' });
  client.disconnect();
  const replacement = client.connect({ url: 'wss://api.test/v1/ws', token: 'new-token', target });
  sockets[2].open(); sockets[2].message({ message_type: 'ack', request_id: sockets[2].sent.request_id, action: 'subscribe' }); await replacement;
  sockets[0].message({ message_type: 'telemetry', gateway_id: 'gw1', sensor_id: 's1', measured_at: '2026-10-27T10:00:00Z', sample_interval_us: 1000, samples: [8] });
  resolveFetch({ status: 200, ok: true, json: async () => raw([[9, '2026-10-27T10:00:00Z']]) });
  await assert.rejects(pending, /stale_catchup/);
  assert.deepEqual(client.liveRows, []);
  assert.deepEqual(client.rows, []);
  assert.equal(client.phase, 'live');
});

test('other-target and malformed-count events set resync without entering bounded live state', async () => {
  const { client, sockets } = harness();
  await subscribe(client, sockets);
  const base = { gateway_id: 'gw1', sensor_id: 's1', boot_id: 'b', first_sequence: '1', sample_count: 1, measured_at: '2026-10-27T10:00:00Z', sample_interval_us: 1, samples: [1] };
  sockets[0].message({ message_type: 'telemetry', ...base, gateway_id: 'other' });
  sockets[0].message({ message_type: 'telemetry', ...base, sample_count: 2 });
  assert.deepEqual(client.liveRows, []);
  assert.equal(client.needsResync, true);
});

test('superseded catch-up rejection cannot clear replacement successful rows', async () => {
  const sockets = [];
  const pendingFetches = [];
  class WS {
    constructor() { this.readyState = 0; sockets.push(this); }
    send(data) { this.sent = JSON.parse(data); }
    open() { this.readyState = 1; this.onopen?.({}); }
    close() { this.readyState = 3; this.onclose?.({}); }
    message(data) { this.onmessage?.({ data: JSON.stringify(data) }); }
  }
  const client = new ReconnectCatchupClient({ WebSocket: WS, fetch: () => new Promise((resolve, reject) => pendingFetches.push({ resolve, reject })) });
  const connect = async () => { const p = client.connect({ url: 'wss://api.test/v1/ws', token: 'same', target }); const socket = sockets.at(-1); socket.open(); socket.message({ message_type: 'ack', request_id: socket.sent.request_id, action: 'subscribe' }); await p; return socket; };
  const oldSocket = await connect();
  const oldCatchup = client.catchUp({ entityId: entity, from: '2026-10-27T10:00:00Z', to: '2026-10-27T10:01:00Z' });
  oldSocket.close();
  const newSocket = await connect();
  const currentCatchup = client.catchUp({ entityId: entity, from: '2026-10-27T10:00:00Z', to: '2026-10-27T10:01:00Z' });
  pendingFetches[1].resolve({ status: 200, ok: true, json: async () => raw([[22, '2026-10-27T10:00:00Z']]) });
  await currentCatchup;
  pendingFetches[0].reject(new Error('old request failed'));
  await assert.rejects(oldCatchup, /old request failed/);
  assert.deepEqual(client.rows.map(row => row.value), [22]);
  assert.equal(client.phase, 'caught_up');
});

test('denied replacement subscription clears prior namespace and identity data', async () => {
  const { client, sockets } = harness();
  await subscribe(client, sockets);
  client.rows = [{ value: 10, timestamp: '2026-10-27T10:00:00Z' }];
  client.liveRows = [{ value: 10, timestamp: '2026-10-27T10:00:00Z' }];
  client.liveIdentities.add('old-user:batch');
  client.liveIdentityOrder.push('old-user:batch');
  client.needsResync = true;
  const otherTarget = { gateway_id: 'gw2', sensor_id: 's1' };
  const denied = client.connect({ url: 'wss://api.test/v1/ws', token: 'other-user-token', target: otherTarget });
  const socket = sockets.at(-1); socket.open(); socket.message({ message_type: 'error', request_id: socket.sent.request_id });
  await assert.rejects(denied, /subscribe_denied/);
  assert.deepEqual(client.rows, []);
  assert.deepEqual(client.liveRows, []);
  assert.equal(client.liveIdentities.size, 0);
  assert.deepEqual(client.liveIdentityOrder, []);
  assert.equal(client.needsResync, false);
});
