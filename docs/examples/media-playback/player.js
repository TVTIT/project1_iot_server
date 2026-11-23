// player.js - Minimal playback client for Stage 8.6 URL issuance
(function () {
  'use strict';

  // In-memory state only: never written to storage or cookies
  let memoryState = null;
  let activeSessionId = 0;
  let isRefreshing = false;
  let pendingSeekTime = null;
  let hasRecoveredMediaError = false;
  let currentAbortController = null;

  const form = document.getElementById('playback-form');
  const apiOriginInput = document.getElementById('api-origin');
  const gatewayIdInput = document.getElementById('gateway-id');
  const mediaIdInput = document.getElementById('media-id');
  const userJwtInput = document.getElementById('user-jwt');
  const statusBox = document.getElementById('status-box');
  const videoPlayer = document.getElementById('video-player');
  const btnFetchPlay = document.getElementById('btn-fetch-play');
  const btnSeekTest = document.getElementById('btn-seek-test');
  const btnStop = document.getElementById('btn-stop');

  // Set default HTTPS origin if loaded over HTTPS
  if (typeof window !== 'undefined' && window.location && window.location.origin && window.location.protocol === 'https:') {
    apiOriginInput.value = window.location.origin;
  }

  function setStatus(msg) {
    statusBox.textContent = msg;
  }

  function resetState() {
    activeSessionId++;
    if (currentAbortController) {
      currentAbortController.abort();
      currentAbortController = null;
    }
    memoryState = null;
    isRefreshing = false;
    pendingSeekTime = null;
    hasRecoveredMediaError = false;

    videoPlayer.pause();
    videoPlayer.removeAttribute('src');
    videoPlayer.load();

    btnSeekTest.disabled = true;
    btnStop.disabled = true;
    btnFetchPlay.disabled = false;
  }

  function isNearExpiry(expEpoch) {
    return expEpoch - Date.now() < 20000;
  }

  function validateInputs(origin, gatewayId, mediaId, jwt) {
    try {
      const u = new URL(origin);
      if (u.protocol !== 'https:' || u.username !== '' || u.password !== '' ||
          (u.pathname !== '/' && u.pathname !== '') || u.search !== '' || u.hash !== '') {
        setStatus('Error: API origin must be a pure HTTPS origin.');
        return false;
      }
    } catch (_e) {
      setStatus('Error: Invalid API origin URL.');
      return false;
    }

    if (!/^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$/.test(gatewayId) || gatewayId === 'backend_service') {
      setStatus('Error: Invalid Gateway ID format.');
      return false;
    }

    if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(mediaId)) {
      setStatus('Error: Media ID must be a canonical 36-char lowercase UUID.');
      return false;
    }

    if (!jwt) {
      setStatus('Error: User JWT is required.');
      return false;
    }

    if (!/^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$/.test(jwt)) {
      setStatus('Error: Malformed JWT (must be compact 3-part format).');
      return false;
    }

    const parts = jwt.split('.');

    try {
      const payload = JSON.parse(atob(parts[1].replace(/-/g, '+').replace(/_/g, '/')));
      if (payload && payload.role === 'service_role') {
        setStatus('Error: service_role key forbidden. Must be user JWT.');
        return false;
      }
    } catch (_e) {
      setStatus('Error: Cannot parse JWT claims.');
      return false;
    }

    return true;
  }

  function validateReturnedUrl(rawUrl, expectedMediaId, rawExpiresAt) {
    try {
      const u = new URL(rawUrl);
      if (u.protocol !== 'https:') return 'URL must be HTTPS';
      if (u.username !== '' || u.password !== '') return 'URL must not contain userinfo';
      if (u.hash !== '') return 'URL must not contain fragment';
      if (u.pathname !== `/storage/v1/object/sign/media-video/${expectedMediaId}.mp4`) {
        return 'URL path does not match expected media clip';
      }
      const params = Array.from(u.searchParams.entries());
      const token = u.searchParams.get('token') || '';
      if (params.length !== 1 || params[0][0] !== 'token' || !/^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$/.test(token)) {
        return 'URL must contain exactly one 3-part token query parameter';
      }
    } catch (_e) {
      return 'Returned grant URL is malformed';
    }

    if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$/.test(rawExpiresAt)) {
      return 'expires_at must be UTC RFC3339 ending in Z';
    }
    const exp = Date.parse(rawExpiresAt);
    if (isNaN(exp) || exp <= Date.now()) {
      return 'expires_at must be in the future';
    }

    return null;
  }

  async function fetchPlaybackGrant(origin, gatewayId, mediaId, jwt, sessionId) {
    if (currentAbortController) {
      currentAbortController.abort();
    }
    const controller = new AbortController();
    currentAbortController = controller;
    const timeoutId = setTimeout(() => controller.abort(), 10000);

    const endpoint = `${new URL(origin).origin}/v1/gateways/${encodeURIComponent(gatewayId)}/media/${encodeURIComponent(mediaId)}/playback-url`;

    try {
      const res = await fetch(endpoint, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${jwt}`, 'Accept': 'application/json' },
        cache: 'no-store',
        redirect: 'error',
        credentials: 'omit',
        signal: controller.signal,
      });

      if (sessionId !== activeSessionId) return null;

      if (res.status === 401 || res.status === 404) {
        resetState();
        setStatus(res.status === 401 ? 'Auth failed (401). No retry.' : 'Clip not found or unauthorized (404). No retry.');
        return null;
      }

      if (res.status === 502 || res.status === 503 || !res.ok) {
        setStatus(`Server error (${res.status}).`);
        return null;
      }

      const body = await res.json();
      if (sessionId !== activeSessionId) return null;

      const valErr = validateReturnedUrl(body.url, mediaId, body.expires_at);
      if (valErr) {
        setStatus(`Grant validation error: ${valErr}`);
        return null;
      }

      return {
        url: body.url,
        expiresAt: Date.parse(body.expires_at),
      };
    } catch (err) {
      if (sessionId !== activeSessionId) return null;
      setStatus(err.name === 'AbortError' ? 'Request timed out or cancelled.' : 'Network error fetching playback URL.');
      return null;
    } finally {
      clearTimeout(timeoutId);
      if (currentAbortController === controller) {
        currentAbortController = null;
      }
    }
  }

  videoPlayer.addEventListener('loadedmetadata', () => {
    if (pendingSeekTime !== null) {
      const seekTo = pendingSeekTime;
      pendingSeekTime = null;
      videoPlayer.currentTime = seekTo;
    }
  });

  videoPlayer.addEventListener('error', async () => {
    if (!memoryState || hasRecoveredMediaError || isRefreshing) return;
    hasRecoveredMediaError = true;
    setStatus('Media error encountered. Attempting single recovery refresh...');
    await refreshAndApply(true);
  });

  async function refreshAndApply(isErrorRecovery, seekTo = videoPlayer.currentTime) {
    if (!memoryState || isRefreshing) return false;
    isRefreshing = true;

    const sessionId = activeSessionId;
    const wasPlaying = isErrorRecovery || (!videoPlayer.paused && !videoPlayer.ended);

    setStatus('Refreshing signed URL...');
    const grant = await fetchPlaybackGrant(
      memoryState.origin, memoryState.gatewayId, memoryState.mediaId, memoryState.jwt, sessionId
    );
    if (sessionId !== activeSessionId) {
      return false;
    }
    isRefreshing = false;

    if (!grant) {
      if (isErrorRecovery) {
        resetState();
        setStatus('Media error recovery failed. Playback stopped.');
      }
      return false;
    }

    memoryState.url = grant.url;
    memoryState.expiresAt = grant.expiresAt;
    pendingSeekTime = seekTo;

    videoPlayer.src = grant.url;
    videoPlayer.load();
    if (wasPlaying) {
      videoPlayer.play().catch(() => {});
    }
    setStatus('Playback URL refreshed.');
    return true;
  }

  form.addEventListener('submit', async (e) => {
    e.preventDefault();

    const origin = apiOriginInput.value.trim();
    const gatewayId = gatewayIdInput.value.trim();
    const mediaId = mediaIdInput.value.trim();
    const jwt = userJwtInput.value.trim();

    if (!validateInputs(origin, gatewayId, mediaId, jwt)) return;

    userJwtInput.value = '';

    resetState();
    const sessionId = activeSessionId;
    btnFetchPlay.disabled = true;
    btnStop.disabled = false;
    setStatus('Requesting playback URL...');

    const grant = await fetchPlaybackGrant(origin, gatewayId, mediaId, jwt, sessionId);
    if (sessionId !== activeSessionId) return;
    btnFetchPlay.disabled = false;
    if (!grant) {
      btnStop.disabled = true;
      return;
    }

    memoryState = { origin, gatewayId, mediaId, jwt, url: grant.url, expiresAt: grant.expiresAt };
    hasRecoveredMediaError = false;

    btnSeekTest.disabled = false;
    btnStop.disabled = false;

    videoPlayer.src = grant.url;
    videoPlayer.load();
    videoPlayer.play().catch(() => {});
    setStatus('Playback started.');
  });

  btnSeekTest.addEventListener('click', async () => {
    if (!memoryState) return;
    const seekTo = videoPlayer.currentTime + 5;
    if (isNearExpiry(memoryState.expiresAt)) {
      if (!(await refreshAndApply(false, seekTo))) return;
    } else {
      videoPlayer.currentTime = seekTo;
    }
    setStatus(`Requested seek to ${seekTo.toFixed(1)}s.`);
  });

  btnStop.addEventListener('click', () => {
    resetState();
    setStatus('Playback stopped and memory cleared.');
  });
})();
