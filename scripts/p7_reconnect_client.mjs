const MAX_WINDOW_MS = 5 * 60_000;
const MAX_OVERLAP_MS = 30_000;

/** Small browser/Node-injectable P7 reconnect demonstration. History has no durable batch identity. */
export class ReconnectCatchupClient {
  constructor({ WebSocket = globalThis.WebSocket, fetch = globalThis.fetch, maxLive = 256, connectTimeoutMs = 10_000 }) {
    if (!Number.isInteger(maxLive) || maxLive < 1) throw new Error('invalid_max_live');
    if (!Number.isInteger(connectTimeoutMs) || connectTimeoutMs < 1 || connectTimeoutMs > 60_000) throw new Error('invalid_connect_timeout');
    this.WebSocket = WebSocket;
    this.fetch = fetch;
    this.maxLive = maxLive;
    this.connectTimeoutMs = connectTimeoutMs;
    this.generation = 0;
    this.catchupGeneration = 0;
    this.phase = 'idle';
    this.liveRows = [];
    this.liveIdentities = new Set();
    this.liveIdentityOrder = [];
    this.rows = [];
    this.needsResync = false;
  }

  connect({ url, token, target }) {
    if (!this.WebSocket || !url || !token || !target?.gateway_id || !target?.sensor_id) return Promise.reject(new Error('invalid_connection'));
    const generation = ++this.generation;
    this.resetData();
    this.pendingConnectCancel?.();
    const previous = this.socket;
    if (previous) { previous.onopen = previous.onmessage = previous.onerror = previous.onclose = null; previous.close(); }
    this.phase = 'connecting';
    this.token = token;
    const socket = new this.WebSocket(url, ['iot.telemetry.v1', `bearer.${token}`]);
    this.socket = socket;
    const requestID = `p7-${Date.now()}-${Math.random().toString(36).slice(2)}`;
    this.target = { gateway_id: target.gateway_id, sensor_id: target.sensor_id };
    return new Promise((resolve, reject) => {
      let settled = false;
      const current = () => generation === this.generation && this.socket === socket;
      const timeout = setTimeout(() => { if (settled) return; finish(); if (current()) { this.phase = 'failed'; socket.close(); } reject(new Error('websocket_ack_timeout')); }, this.connectTimeoutMs);
      const finish = () => { settled = true; clearTimeout(timeout); if (this.pendingConnectCancel === cancel) this.pendingConnectCancel = null; };
      const cancel = () => { if (!settled) { finish(); reject(new Error('websocket_superseded')); } };
      this.pendingConnectCancel = cancel;
      socket.onopen = () => { if (current()) socket.send(JSON.stringify({ protocol_version: 1, request_id: requestID, action: 'subscribe', targets: [target] })); };
      socket.onmessage = event => {
        if (!current()) return;
        let msg;
        try { msg = JSON.parse(event.data); } catch { this.phase = 'failed'; if (!settled) { finish(); reject(new Error('invalid_ws_payload')); } return; }
        if (!settled && msg.message_type === 'ack' && msg.request_id === requestID && msg.action === 'subscribe') {
          finish(); this.phase = 'live'; resolve({ status: 'subscribed', requestId: requestID }); return;
        }
        if (!settled && msg.message_type === 'error' && msg.request_id === requestID) {
          finish(); this.resetData(); this.phase = 'failed'; reject(new Error('subscribe_denied')); return;
        }
        if (msg.message_type === 'telemetry') this.acceptLive(msg);
        if (msg.message_type === 'gap') this.needsResync = true;
      };
      socket.onerror = () => { if (current() && !settled) { finish(); this.phase = 'failed'; reject(new Error('websocket_error')); } };
      socket.onclose = () => { if (!current()) return; if (!settled) { finish(); this.phase = 'failed'; reject(new Error('websocket_closed_before_ack')); } else if (this.phase === 'live') this.phase = 'disconnected'; };
    });
  }

  acceptLive(message) {
    if (message.gateway_id !== this.target?.gateway_id || message.sensor_id !== this.target?.sensor_id || !Array.isArray(message.samples) || !message.samples.length || message.samples.length !== message.sample_count || message.samples.length > this.maxLive || !message.samples.every(Number.isFinite) || !Number.isSafeInteger(message.sample_interval_us) || message.sample_interval_us <= 0 || !/^[0-9]+$/.test(String(message.first_sequence ?? ''))) {
      this.needsResync = true; return;
    }
    const identity = message.message_id ? `message:${message.message_id}` : `${message.boot_id}:${message.first_sequence}`;
    if (identity !== 'undefined:undefined' && this.liveIdentities.has(identity)) return;
    if (identity !== 'undefined:undefined') {
      this.liveIdentities.add(identity);
      this.liveIdentityOrder.push(identity);
      while (this.liveIdentityOrder.length > this.maxLive) this.liveIdentities.delete(this.liveIdentityOrder.shift());
    }
    let measured;
    try { measured = epochNanoseconds(message.measured_at); } catch { this.needsResync = true; return; }
    for (let i = 0; i < message.samples.length; i++) this.liveRows.push({ value: message.samples[i], timestamp: formatNanoseconds(measured + BigInt(i) * BigInt(message.sample_interval_us) * 1000n), identity, sampleIntervalUS: message.sample_interval_us });
    if (this.liveRows.length > this.maxLive) this.liveRows.splice(0, this.liveRows.length - this.maxLive);
  }

  noteLateBatch() { this.needsResync = true; }
  resetData() {
    this.rows = [];
    this.liveRows = [];
    this.liveIdentities.clear();
    this.liveIdentityOrder = [];
    this.needsResync = false;
  }
  disconnect() { ++this.generation; this.pendingConnectCancel?.(); const socket = this.socket; this.socket = null; if (socket) { socket.onopen = socket.onmessage = socket.onerror = socket.onclose = null; socket.close(); } if (this.phase !== 'failed') this.phase = 'disconnected'; }

  async catchUp({ entityId, from, to, overlapMs = 0 }) {
    const start = Date.parse(from), end = Date.parse(to);
    if (!entityId || !Number.isFinite(start) || !Number.isFinite(end) || end <= start || end - start > MAX_WINDOW_MS || !Number.isInteger(overlapMs) || overlapMs < 0 || overlapMs > MAX_OVERLAP_MS) throw new Error('invalid_window');
    if (this.phase !== 'live' || !this.socket || this.socket.readyState !== 1) throw new Error('reconnect_and_subscribe_required');
    this.phase = 'catching_up'; this.rows = [];
    const generation = this.generation, catchupGeneration = ++this.catchupGeneration;
    const requestedTarget = { ...this.target }, requestedToken = this.token;
    const isCurrent = () => generation === this.generation && catchupGeneration === this.catchupGeneration && requestedTarget.gateway_id === this.target?.gateway_id && requestedTarget.sensor_id === this.target?.sensor_id && requestedToken === this.token && this.phase === 'catching_up';
    try {
      // Preserve caller RFC3339 precision when no overlap; Date.toISOString() truncates sub-ms digits.
      const timeAt = overlapMs ? new Date(Math.max(0, start - overlapMs)).toISOString() : from;
      const q = new URLSearchParams({ attrs: 'measurement', timerel: 'between', timeAt, endTimeAt: to });
      const response = await this.fetch(`/v1/digital-twins/${encodeURIComponent(entityId)}/temporal?${q}`, { headers: { Authorization: `Bearer ${this.token ?? ''}`, Accept: 'application/ld+json' } });
      if (response.status === 401 || response.status === 403) throw new Error(`history_http_${response.status}`);
      if (!response.ok) throw new Error(`history_http_${response.status}`);
      const body = await response.json();
      if (!isCurrent()) throw new Error('stale_catchup');
      if (body?.id !== entityId || body?.measurement?.type !== 'Property' || !Array.isArray(body.measurement.values)) throw new Error('invalid_history_payload');
      const rows = body.measurement.values.map(tuple => {
        if (!Array.isArray(tuple) || tuple.length !== 2 || !Number.isFinite(tuple[0]) || typeof tuple[1] !== 'string' || !Number.isFinite(Date.parse(tuple[1]))) throw new Error('invalid_history_tuple');
        return { value: tuple[0], timestamp: tuple[1], source: 'history' };
      });
      this.rows = rows;
      this.phase = 'caught_up';
      const needsResync = this.needsResync;
      this.needsResync = false;
      return { status: 'caught_up', rows: [...rows], needsResync, historyExactlyOnce: false, policy: 'REST replaces requested finite window; raw tuples retained including equal timestamps; bounded overlap only' };
    } catch (error) {
      if (isCurrent()) {
        this.rows = [];
        this.phase = 'failed';
      }
      throw error;
    }
  }
}

function epochNanoseconds(value) {
  if (typeof value !== 'string' || !/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?(?:Z|[+-]\d\d:\d\d)$/.test(value)) throw new Error('invalid_timestamp');
  const ms = Date.parse(value);
  if (!Number.isFinite(ms)) throw new Error('invalid_timestamp');
  const fraction = value.match(/\.(\d+)/)?.[1] ?? '';
  const nanos = BigInt((fraction + '000000000').slice(0, 9));
  const parsedFraction = BigInt((fraction.slice(0, 3).padEnd(3, '0')) || '0') * 1_000_000n;
  return BigInt(ms) * 1_000_000n + nanos - parsedFraction;
}

function formatNanoseconds(value) {
  const ms = value / 1_000_000n;
  const withinSecond = ((value % 1_000_000_000n) + 1_000_000_000n) % 1_000_000_000n;
  const fraction = withinSecond.toString().padStart(9, '0').slice(0, 6);
  return `${new Date(Number(ms)).toISOString().replace(/\.\d{3}Z$/, '')}.${fraction}Z`;
}
