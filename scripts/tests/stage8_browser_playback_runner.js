// scripts/tests/stage8_browser_playback_runner.js
// Headless Chromium automation for Stage 8.7 Slice C browser media playback.
// Reads JSON config from process.stdin only; never accepts JWT or URLs via CLI args.
'use strict';

const fs = require('fs');
const path = require('path');

function resolvePlaywright() {
  const customDir = process.env.STAGE8_PLAYWRIGHT_DIR || process.env.PLAYWRIGHT_PATH || process.env.NODE_PATH;
  if (customDir) {
    try {
      return require(customDir);
    } catch (_e) {}
    try {
      return require(path.join(customDir, 'playwright-core'));
    } catch (_e) {}
  }
  try {
    return require('playwright-core');
  } catch (_e) {}
  try {
    return require('playwright');
  } catch (_e) {}
  return null;
}

async function readStdin() {
  return new Promise((resolve, reject) => {
    let data = '';
    process.stdin.setEncoding('utf8');
    process.stdin.on('data', (chunk) => { data += chunk; });
    process.stdin.on('end', () => resolve(data));
    process.stdin.on('error', reject);
  });
}

async function main() {
  const rawInput = await readStdin();
  if (!rawInput.trim()) {
    throw new Error('missing_stdin_json_config');
  }
  const config = JSON.parse(rawInput);
  const { demoUrl, apiOrigin, playbackOrigin, gatewayId, mediaId, token, targetSeekTime } = config;

  if (!demoUrl || !apiOrigin || !playbackOrigin || !gatewayId || !mediaId || !token) {
    throw new Error('incomplete_config_parameters');
  }

  const chromeBin = process.env.STAGE8_CHROMIUM_PATH || process.env.CHROME_BIN;
  if (!chromeBin || !fs.existsSync(chromeBin)) {
    throw new Error(`chromium_executable_missing: ${chromeBin || 'not_set'}`);
  }

  const playwright = resolvePlaywright();
  if (!playwright) {
    throw new Error('playwright_core_module_not_found');
  }

  const seekTarget = typeof targetSeekTime === 'number' ? targetSeekTime : 25.0;
  const expectedPlaybackOrigin = new URL(playbackOrigin).origin;
  const expectedPlaybackPath = `/storage/v1/object/sign/media-video/${mediaId}.mp4`;

  const browser = await playwright.chromium.launch({
    executablePath: chromeBin,
    headless: true,
    args: [
      '--no-sandbox',
      '--disable-dev-shm-usage',
      '--disable-gpu',
      '--no-first-run',
      '--autoplay-policy=no-user-gesture-required',
    ],
  });

  try {
    const context = await browser.newContext({
      ignoreHTTPSErrors: false, // Strict TLS validation against NSS DB in disposable HOME
    });

    const page = await context.newPage();

    // CDP Network throttling: 350 KB/s (~2.8 Mbps) download rate.
    // Video is ~2.3 Mbps, so it buffers progressively and won't pre-buffer 25 seconds ahead.
    const cdp = await context.newCDPSession(page);
    await cdp.send('Network.enable');
    await cdp.send('Network.emulateNetworkConditions', {
      offline: false,
      downloadThroughput: 350 * 1024,
      uploadThroughput: 500 * 1024,
      latency: 20,
    });

    const playbackResponses = [];
    const requestTimes = new WeakMap();
    page.on('request', (request) => {
      const url = new URL(request.url());
      if (url.origin === expectedPlaybackOrigin && url.pathname === expectedPlaybackPath) {
        requestTimes.set(request, performance.now());
      }
    });
    page.on('response', (response) => {
      const request = response.request();
      if (requestTimes.has(request)) {
        const headers = response.headers();
        const reqHeaders = request.headers();
        playbackResponses.push({
          status: response.status(),
          contentRange: headers['content-range'] || null,
          requestRange: reqHeaders['range'] || null,
          requestStartedAt: requestTimes.get(request),
        });
      }
    });

    // 1. Navigate to demo page on API origin
    const navResp = await page.goto(demoUrl, { waitUntil: 'load', timeout: 15000 });
    if (!navResp || navResp.status() !== 200) {
      throw new Error(`demo_page_load_failed_status_${navResp ? navResp.status() : 'null'}`);
    }

    // 2. Fill form inputs
    await page.fill('#api-origin', apiOrigin);
    await page.fill('#gateway-id', gatewayId);
    await page.fill('#media-id', mediaId);
    await page.fill('#user-jwt', token);

    // 3. Initiate playback
    const t0 = performance.now();
    await page.click('#btn-fetch-play');

    // 4. Wait for video to start playing and decode frames
    await page.waitForFunction(() => {
      const v = document.getElementById('video-player');
      return v && !v.paused && v.currentTime > 0.5 && (v.webkitDecodedFrameCount || 0) > 5;
    }, null, { timeout: 20000 });
    const startupSeconds = Math.round((performance.now() - t0) / 10) / 100;

    // 5. Inspect pre-seek state & prove seek target is outside all buffered ranges
    const preSeek = await page.evaluate((targetTime) => {
      const v = document.getElementById('video-player');
      const ranges = [];
      let isTargetBuffered = false;
      for (let i = 0; i < v.buffered.length; i++) {
        const s = v.buffered.start(i);
        const e = v.buffered.end(i);
        ranges.push([Math.round(s * 100) / 100, Math.round(e * 100) / 100]);
        if (targetTime >= s && targetTime <= e) {
          isTargetBuffered = true;
        }
      }
      return {
        currentTime: Math.round(v.currentTime * 100) / 100,
        readyState: v.readyState,
        videoWidth: v.videoWidth,
        videoHeight: v.videoHeight,
        decodedFrames: v.webkitDecodedFrameCount || 0,
        bufferedRanges: ranges,
        isTargetBuffered,
      };
    }, seekTarget);

    if (preSeek.isTargetBuffered) {
      throw new Error(`target_time_${seekTarget}_already_buffered_${JSON.stringify(preSeek.bufferedRanges)}`);
    }

    // 6. Perform unbuffered seek to seekTarget
    const seekT0 = performance.now();

    await page.evaluate((targetTime) => {
      const v = document.getElementById('video-player');
      v.currentTime = targetTime;
    }, seekTarget);

    // Assigning currentTime alone does not prove that any new frame has played.
    await page.waitForFunction(({ targetTime, oldFrames }) => {
      const v = document.getElementById('video-player');
      return v && !v.seeking && !v.paused && v.currentTime > targetTime + 0.1 &&
        (v.webkitDecodedFrameCount || 0) > oldFrames;
    }, { targetTime: seekTarget, oldFrames: preSeek.decodedFrames }, { timeout: 20000 });
    const seekSeconds = Math.round((performance.now() - seekT0) / 10) / 100;

    const timeImmediatelyAfterSeek = await page.evaluate(() => document.getElementById('video-player').currentTime);
    const framesImmediatelyAfterSeek = await page.evaluate(() => document.getElementById('video-player').webkitDecodedFrameCount || 0);

    // 8. Observe sustained playback over ~1.5s
    await page.waitForTimeout(1500);

    const sustained = await page.evaluate(() => {
      const v = document.getElementById('video-player');
      return {
        currentTime: Math.round(v.currentTime * 100) / 100,
        paused: v.paused,
        decodedFrames: v.webkitDecodedFrameCount || 0,
      };
    });

    if (sustained.currentTime <= timeImmediatelyAfterSeek || sustained.paused) {
      throw new Error(`playback_did_not_sustain_after_seek: initial=${timeImmediatelyAfterSeek}, sustained=${sustained.currentTime}, paused=${sustained.paused}`);
    }

    // 9. Verify 206 Range request after seek on playback endpoint
    const range206 = playbackResponses.find((r) => {
      const offset = /^bytes=([1-9]\d*)-\d*$/.exec(r.requestRange || '');
      return r.requestStartedAt >= seekT0 && r.status === 206 && offset &&
        r.contentRange?.startsWith(`bytes ${offset[1]}-`);
    });
    if (!range206) {
      throw new Error('missing_nonzero_206_request_after_seek');
    }

    // 10. Verify Seek +5s button from demo page UI
    const beforeBtnTime = sustained.currentTime;
    await page.click('#btn-seek-test');
    await page.waitForTimeout(500);
    const afterBtnTime = await page.evaluate(() => Math.round(document.getElementById('video-player').currentTime * 10) / 10);
    const btnSeekWorked = afterBtnTime >= Math.round((beforeBtnTime + 4.0) * 10) / 10;

    const result = {
      status: 'PASS',
      startup_seconds: startupSeconds,
      seek_seconds: seekSeconds,
      pre_seek: preSeek,
      unbuffered_proof: {
        targetSeekTime: seekTarget,
        outside_all_buffered_ranges: !preSeek.isTargetBuffered,
      },
      seek_range_response: {
        status: range206.status,
        content_range: range206.contentRange,
        request_range_nonzero: true,
        origin_matched_playback: true,
      },
      post_seek: {
        currentTime_after_seek: Math.round(timeImmediatelyAfterSeek * 100) / 100,
        currentTime_sustained: sustained.currentTime,
        playing_sustained: !sustained.paused,
        decodedFrames_sustained: sustained.decodedFrames,
        frames_advanced: sustained.decodedFrames > framesImmediatelyAfterSeek,
      },
      seek_button_verified: btnSeekWorked,
    };

    console.log(JSON.stringify(result));
    await context.close();
    await browser.close();
    process.exit(0);
  } catch (err) {
    // Playwright exceptions may contain the signed URL and are not safe to print.
    const safeCode = /^[a-z0-9_]+$/.test(err.message) ? err.message : 'browser_playback_check_failed';
    console.error(JSON.stringify({ status: 'FAIL', error: safeCode }));
    try {
      await browser.close();
    } catch (_e) {}
    process.exit(1);
  }
}

main().catch(() => {
  console.error(JSON.stringify({ status: 'FAIL', error: 'browser_playback_setup_failed' }));
  process.exit(1);
});
