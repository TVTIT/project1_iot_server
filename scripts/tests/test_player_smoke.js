// scripts/tests/test_player_smoke.js
// Focused headless state machine tests for player.js behavior
const fs = require('fs');
const path = require('path');
const vm = require('vm');
const assert = require('assert');

function createMockElement(id) {
  const listeners = {};
  return {
    id,
    value: '',
    textContent: '',
    disabled: false,
    src: '',
    currentTime: 0,
    paused: true,
    ended: false,
    listeners,
    addEventListener(event, fn) {
      listeners[event] = listeners[event] || [];
      listeners[event].push(fn);
    },
    async trigger(event, ...args) {
      if (listeners[event]) {
        for (const fn of listeners[event]) {
          await fn(...args);
        }
      }
    },
    pause() { this.paused = true; },
    load() {},
    play() { this.paused = false; return Promise.resolve(); },
    removeAttribute(attr) { if (attr === 'src') this.src = ''; }
  };
}

function setupMockEnvironment(mockFetch) {
  const elements = {
    'playback-form': createMockElement('playback-form'),
    'api-origin': createMockElement('api-origin'),
    'gateway-id': createMockElement('gateway-id'),
    'media-id': createMockElement('media-id'),
    'user-jwt': createMockElement('user-jwt'),
    'status-box': createMockElement('status-box'),
    'video-player': createMockElement('video-player'),
    'btn-fetch-play': createMockElement('btn-fetch-play'),
    'btn-seek-test': createMockElement('btn-seek-test'),
    'btn-stop': createMockElement('btn-stop'),
  };

  const sandbox = {
    document: {
      getElementById: (id) => elements[id] || null,
    },
    window: {
      location: { origin: 'https://example.com', protocol: 'https:' },
    },
    fetch: mockFetch,
    AbortController: global.AbortController,
    URL: global.URL,
    atob: global.atob,
    setTimeout: global.setTimeout,
    clearTimeout: global.clearTimeout,
    Date: global.Date,
    console: global.console,
  };

  const code = fs.readFileSync(path.join(__dirname, '../../docs/examples/media-playback/player.js'), 'utf8');
  vm.createContext(sandbox);
  vm.runInContext(code, sandbox);

  return elements;
}

// Helper to create synthetic JWT
function createTestJwt(role) {
  const h = Buffer.from(JSON.stringify({ alg: 'HS256' })).toString('base64url');
  const p = Buffer.from(JSON.stringify({ role, sub: 'user-1' })).toString('base64url');
  return `${h}.${p}.sig`;
}

async function runTests() {
  console.log('Running player.js mock smoke tests...');

  // Test 1: Terminal 401 clears state and triggers NO retry
  {
    let fetchCount = 0;
    const mockFetch = async () => {
      fetchCount++;
      return {
        status: 401,
        ok: false,
        json: async () => ({ error: { code: 'unauthorized', message: 'unauthorized' } }),
      };
    };

    const el = setupMockEnvironment(mockFetch);
    el['api-origin'].value = 'https://api.example.com';
    el['gateway-id'].value = 'gw_01';
    el['media-id'].value = '00000000-0000-0000-0000-000000000001';
    el['user-jwt'].value = createTestJwt('authenticated');

    await el['playback-form'].trigger('submit', { preventDefault() {} });

    assert.strictEqual(fetchCount, 1, '401 must trigger exactly 1 request without retries');
    assert.strictEqual(el['video-player'].src, '', 'video src must remain empty on 401');
    assert.strictEqual(el['btn-seek-test'].disabled, true, 'controls must be disabled on 401');
    assert.ok(el['status-box'].textContent.includes('No retry'), 'status must state no retry');
    console.log('✓ Test 1 Passed: Terminal 401 stops immediately with no retry');
  }

  // Test 2: Terminal 404 clears state and triggers NO retry
  {
    let fetchCount = 0;
    const mockFetch = async () => {
      fetchCount++;
      return {
        status: 404,
        ok: false,
        json: async () => ({ error: { code: 'not_found', message: 'resource not found' } }),
      };
    };

    const el = setupMockEnvironment(mockFetch);
    el['api-origin'].value = 'https://api.example.com';
    el['gateway-id'].value = 'gw_01';
    el['media-id'].value = '00000000-0000-0000-0000-000000000001';
    el['user-jwt'].value = createTestJwt('authenticated');

    await el['playback-form'].trigger('submit', { preventDefault() {} });

    assert.strictEqual(fetchCount, 1, '404 must trigger exactly 1 request without retries');
    assert.strictEqual(el['video-player'].src, '', 'video src must remain empty on 404');
    assert.strictEqual(el['btn-seek-test'].disabled, true, 'controls must be disabled on 404');
    assert.ok(el['status-box'].textContent.includes('No retry'), 'status must state no retry');
    console.log('✓ Test 2 Passed: Terminal 404 stops immediately with no retry');
  }

  // Test 3: Stop-mid-grant invalidates in-flight response so no stale URL attaches
  {
    let resolveInFlight;
    const mockFetch = () => {
      return new Promise((resolve) => {
        resolveInFlight = resolve;
      });
    };

    const el = setupMockEnvironment(mockFetch);
    el['api-origin'].value = 'https://api.example.com';
    el['gateway-id'].value = 'gw_01';
    el['media-id'].value = '00000000-0000-0000-0000-000000000001';
    el['user-jwt'].value = createTestJwt('authenticated');

    // Start request
    const submitPromise = el['playback-form'].trigger('submit', { preventDefault() {} });
    assert.strictEqual(el['btn-stop'].disabled, false, 'Stop must be clickable during issuance');

    // User clicks stop while fetch is in flight
    await el['btn-stop'].trigger('click');

    // In-flight fetch now resolves with valid grant
    const validUrl = 'https://playback.example.com/storage/v1/object/sign/media-video/00000000-0000-0000-0000-000000000001.mp4?token=a.b.c';
    const futureDate = new Date(Date.now() + 120000).toISOString().replace(/\.\d+Z$/, 'Z');
    resolveInFlight({
      status: 200,
      ok: true,
      json: async () => ({ url: validUrl, expires_at: futureDate }),
    });

    await submitPromise;

    // Stale grant must NOT be attached to video
    assert.strictEqual(el['video-player'].src, '', 'Stale in-flight grant must not attach after stop');
    assert.strictEqual(el['btn-seek-test'].disabled, true, 'Buttons must remain disabled after stop');
    assert.ok(el['status-box'].textContent.includes('stopped'), 'Status must reflect stopped state');
    console.log('✓ Test 3 Passed: Stop-mid-grant prevents stale URL attachment');
  }

  // Test 4: Rejects non-HTTPS origin (even localhost without https)
  {
    let fetchCalled = false;
    const mockFetch = async () => { fetchCalled = true; return {}; };
    const el = setupMockEnvironment(mockFetch);
    el['api-origin'].value = 'http://localhost:8080';
    el['gateway-id'].value = 'gw_01';
    el['media-id'].value = '00000000-0000-0000-0000-000000000001';
    el['user-jwt'].value = createTestJwt('authenticated');

    await el['playback-form'].trigger('submit', { preventDefault() {} });
    assert.strictEqual(fetchCalled, false, 'Non-HTTPS origin must be rejected before fetch');
    assert.ok(el['status-box'].textContent.includes('pure HTTPS'), 'Status must report pure HTTPS required');
    console.log('✓ Test 4 Passed: Non-HTTPS API origin rejected');
  }

  // Test 5: Rejects service_role key and clears visible JWT on accept
  {
    let fetchCalled = false;
    const mockFetch = async () => { fetchCalled = true; return {}; };
    const el = setupMockEnvironment(mockFetch);
    el['api-origin'].value = 'https://api.example.com';
    el['gateway-id'].value = 'gw_01';
    el['media-id'].value = '00000000-0000-0000-0000-000000000001';
    el['user-jwt'].value = createTestJwt('service_role');

    await el['playback-form'].trigger('submit', { preventDefault() {} });
    assert.strictEqual(fetchCalled, false, 'service_role key must be rejected');
    assert.ok(el['status-box'].textContent.includes('service_role key forbidden'));

    // Now with valid authenticated user JWT
    el['user-jwt'].value = createTestJwt('authenticated');
    await el['playback-form'].trigger('submit', { preventDefault() {} });
    assert.strictEqual(el['user-jwt'].value, '', 'Visible user JWT input must be cleared upon acceptance');
    console.log('✓ Test 5 Passed: service_role rejected and visible JWT input cleared');
  }

  // Test 6: Stale async refresh does not reset a newer active session
  {
    let resolveRefresh;
    let callCount = 0;
    const mockFetch = async () => {
      callCount++;
      if (callCount === 1) {
        // Initial grant
        const validUrl = 'https://playback.example.com/storage/v1/object/sign/media-video/00000000-0000-0000-0000-000000000001.mp4?token=a.b.c';
        const futureDate = new Date(Date.now() + 120000).toISOString().replace(/\.\d+Z$/, 'Z');
        return {
          status: 200,
          ok: true,
          json: async () => ({ url: validUrl, expires_at: futureDate }),
        };
      }
      if (callCount === 2) {
        // Refresh request in flight
        return new Promise((resolve) => {
          resolveRefresh = resolve;
        });
      }
      // New session grant
      const validUrl2 = 'https://playback.example.com/storage/v1/object/sign/media-video/00000000-0000-0000-0000-000000000002.mp4?token=x.y.z';
      const futureDate2 = new Date(Date.now() + 120000).toISOString().replace(/\.\d+Z$/, 'Z');
      return {
        status: 200,
        ok: true,
        json: async () => ({ url: validUrl2, expires_at: futureDate2 }),
      };
    };

    const el = setupMockEnvironment(mockFetch);
    el['api-origin'].value = 'https://api.example.com';
    el['gateway-id'].value = 'gw_01';
    el['media-id'].value = '00000000-0000-0000-0000-000000000001';
    el['user-jwt'].value = createTestJwt('authenticated');

    // 1. Initial play
    await el['playback-form'].trigger('submit', { preventDefault() {} });
    assert.ok(el['video-player'].src.includes('00000000-0000-0000-0000-000000000001'), 'Initial session attached');

    // 2. Trigger error recovery refresh
    const errPromise = el['video-player'].trigger('error');

    // 3. While refresh is in flight, user starts a brand new session (media-id 2)
    el['media-id'].value = '00000000-0000-0000-0000-000000000002';
    el['user-jwt'].value = createTestJwt('authenticated');
    await el['playback-form'].trigger('submit', { preventDefault() {} });
    assert.ok(el['video-player'].src.includes('00000000-0000-0000-0000-000000000002'), 'Newer session attached');

    // 4. Stale refresh now fails
    resolveRefresh({ status: 500, ok: false });
    await errPromise;

    // Newer session must NOT be reset by stale failed refresh
    assert.ok(el['video-player'].src.includes('00000000-0000-0000-0000-000000000002'), 'Newer session must not be destroyed by stale refresh');
    assert.strictEqual(el['btn-seek-test'].disabled, false, 'Newer session controls remain enabled');
    console.log('✓ Test 6 Passed: Stale async refresh does not reset newer active session');
  }

  // Test 7: Strict compact token parsing in validateInputs and validateReturnedUrl
  {
    const el = setupMockEnvironment(async () => ({}));
    el['api-origin'].value = 'https://api.example.com';
    el['gateway-id'].value = 'gw_01';
    el['media-id'].value = '00000000-0000-0000-0000-000000000001';

    // Invalid non-compact JWTs
    for (const badJwt of ['bad.jwt', 'a.b.c.d', 'a b.c.d', 'a.b.c;evil', 'a..c']) {
      el['user-jwt'].value = badJwt;
      await el['playback-form'].trigger('submit', { preventDefault() {} });
      assert.ok(el['status-box'].textContent.includes('compact 3-part format'), `Must reject non-compact JWT: ${badJwt}`);
    }
    console.log('✓ Test 7 Passed: Strict compact token format enforced');
  }

  // A near-expiry seek must apply the target to the replacement source, not
  // seek the old source and later restore the pre-seek position.
  {
    let fetchCount = 0;
    const mediaId = '00000000-0000-0000-0000-000000000001';
    const el = setupMockEnvironment(async () => {
      fetchCount++;
      const expiresAt = new Date(Date.now() + (fetchCount === 1 ? 10000 : 120000)).toISOString();
      return {
        status: 200,
        ok: true,
        json: async () => ({
          url: `https://playback.example.com/storage/v1/object/sign/media-video/${mediaId}.mp4?token=a.b.c${fetchCount}`,
          expires_at: expiresAt,
        }),
      };
    });
    el['gateway-id'].value = 'gw_01';
    el['media-id'].value = mediaId;
    el['user-jwt'].value = createTestJwt('authenticated');
    await el['playback-form'].trigger('submit', { preventDefault() {} });

    const video = el['video-player'];
    video.currentTime = 12;
    video.load = () => { video.currentTime = 0; };
    await el['btn-seek-test'].trigger('click');
    assert.strictEqual(fetchCount, 2);
    await video.trigger('loadedmetadata');
    assert.strictEqual(video.currentTime, 17, 'Refresh must preserve the requested seek offset');
    console.log('✓ Test 8 Passed: A near-expiry seek restores the requested new position');
  }

  console.log('All player.js mock smoke tests PASSED!');
}

runTests().catch((err) => {
  console.error('Test failure:', err);
  process.exit(1);
});
