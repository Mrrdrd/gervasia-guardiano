/* Gervàsia guardiano: watcher and speaker for the cloud guardian runs.
   Load in the Home Assistant tab:  await import('/local/guardiano_poller.js?v=1')  (or a <script> tag)
   Then: await __G.init({runId: <number>})  once, and  await __G.mon(30000)  in a loop.
   Version 2, Nov 17 2026: test mode (localStorage guardiano_test = 1 makes typed requests count as spoken to the ReSpeaker).
   Version 3, Nov 18 2026: test environment. Requests on the "Gervàsia Test" pipeline arrive with test: true, in batches of
   their own. While such a batch is being handled (until the next __G.mon call) every action is sandboxed: nothing
   reaches the house, speech and actions are written to input_text.guardiano_test_cloud instead.
   Version 4: read-only services (get_..., search) really run in the sandbox. */
(function () {
  if (window.__G && window.__G.v === 4) return;
  const G = (window.__G = { v: 4 });
  const H = () => document.querySelector('home-assistant').hass;
  const WS = (msg) => H().connection.sendMessagePromise(msg);
  const NY = (t) => new Date(t).toLocaleTimeString('it-IT', { timeZone: 'America/New_York', hour12: false });
  const clean = (s, n) => String(s == null ? '' : s).replace(/[?&=]/g, ' ').slice(0, n || 400);
  const MICS = ['switch.esp32_s3_box_3_63bb38_mute', 'switch.respeaker_salotto_microphone_mute'];
  const SATS = {
    'assist_satellite.esp32_s3_box_3_63bb38_assist_satellite': { sonos: 'switch.esp32_s3_box_3_63bb38_risposte_sul_sonos', mp: 'media_player.esp32_s3_box_3_63bb38', announce: false },
    'assist_satellite.respeaker_salotto_assist_satellite': { sonos: 'switch.respeaker_salotto_risposte_sul_sonos', mp: 'media_player.respeaker_salotto_media_player', announce: true },
  };
  const DEFAULT_SAT = 'assist_satellite.respeaker_salotto_assist_satellite';
  const WATCH_SCRIPTS = ['script.guarda_su_pc_worker', 'script.return_to_pc', 'script.esegui_routine_worker', 'script.tv_italia'];
  const MA_ERR = /ERROR_BUFFERING|ERROR_PLAYBACK_FAILED|disconnected prematurely/;
  const TEST_OUT = 'input_text.guardiano_test_cloud';
  const isTestPipe = (p) => /test/i.test((p && p.name) || '');

  G.opts = { runId: null, share: true, oldMs: 120000 };
  G.seen = new Set(); G.prepped = new Set(); G.queue = []; G.waiters = [];
  G.hotUntil = 0; G.music = null; G.spoken = []; G.maSeen = new Set(); G.handoffSent = false; G.errors = 0;

  function push(ev) { G.queue.push(ev); const w = G.waiters.splice(0); w.forEach((f) => f()); }
  function persistSeen() {
    if (!G.opts.share) return;
    try { localStorage.setItem('guardiano_seen', JSON.stringify([...G.seen].slice(-60))); } catch (e) {}
  }

  /* ---------- handoff between runs ---------- */
  G.owner = () => { try { return JSON.parse(localStorage.getItem('guardiano_owner') || 'null'); } catch (e) { return null; } };
  G.takeOver = () => { localStorage.setItem('guardiano_owner', JSON.stringify({ id: G.opts.runId, t: Date.now() })); return true; };
  G.isOwner = () => { const o = G.owner(); return !!o && o.id === G.opts.runId; };
  function handoffTick() {
    if (!G.opts.share || !G.opts.runId) return;
    const o = G.owner();
    if (o && o.id === G.opts.runId) { o.t = Date.now(); localStorage.setItem('guardiano_owner', JSON.stringify(o)); }
    else if (o && o.id > G.opts.runId && !G.handoffSent) { G.handoffSent = true; push({ kind: 'handoff', newer: o.id }); }
  }

  /* ---------- voice runs ---------- */
  function summarise(p, run, ev) {
    const get = (t) => ev.find((e) => e.type === t);
    const rs = get('run-start'), stt = get('stt-end'), ist = get('intent-start'), ie = get('intent-end');
    const heard = (stt && stt.data && stt.data.stt_output && stt.data.stt_output.text) || (ist && ist.data && ist.data.intent_input) || '';
    const out = ie && ie.data && ie.data.intent_output;
    const reply = out && out.response && out.response.speech && out.response.speech.plain ? out.response.speech.plain.speech : '';
    const tools = [];
    ev.forEach((e) => {
      const d = (e.data && e.data.chat_log_delta) || {};
      (d.tool_calls || []).forEach((tc) => tools.push(clean(tc.tool_name + ' ' + JSON.stringify(tc.tool_args), 200)));
      if (d.role === 'tool_result') tools.push('risultato ' + d.tool_name + ': ' + clean(JSON.stringify(d.tool_result), 200));
    });
    let typed = !stt && !!heard, sat = (rs && rs.data && rs.data.satellite_id) || null;
    const test = isTestPipe(p);
    if (test && typed) { typed = false; sat = sat || DEFAULT_SAT; }
    try { if (typed && localStorage.getItem('guardiano_test') === '1') { typed = false; sat = DEFAULT_SAT; } } catch (e) {}
    return {
      kind: 'voice', id: run.pipeline_run_id, time: NY(run.timestamp), assistant: p.name,
      satellite: sat, typed, test,
      heard: clean(heard, 300), reply: clean(reply, 500), tools,
      errors: ev.filter((e) => e.type === 'error').map((e) => (e.data && e.data.code) || 'error'),
      intentEndMs: ie ? Date.parse(ie.timestamp) : null, replyLen: reply.length,
    };
  }
  function isOwnEcho(heard) {
    const h = (heard || '').toLowerCase().replace(/[^\p{L}\p{N} ]/gu, '').trim();
    if (!h) return false;
    return G.spoken.some((s) => Date.now() - s.t < 30000 && (s.text.includes(h) || h.includes(s.text)));
  }
  async function pollRuns() {
    for (const p of G.pipes) {
      const lst = await WS({ type: 'assist_pipeline/pipeline_debug/list', pipeline_id: p.id });
      for (const run of lst.pipeline_runs || []) {
        const rid = run.pipeline_run_id;
        if (G.seen.has(rid)) continue;
        const det = await WS({ type: 'assist_pipeline/pipeline_debug/get', pipeline_id: p.id, pipeline_run_id: rid });
        const ev = det.events || [];
        const types = ev.map((e) => e.type);
        const done = types.includes('intent-end') || types.includes('error') || types.includes('run-end');
        const s = summarise(p, run, ev);
        if (s.errors.length && s.errors.every((c) => c === 'duplicate_wake_up_detected') && !s.heard) { G.seen.add(rid); persistSeen(); continue; }
        if (isOwnEcho(s.heard)) { G.seen.add(rid); persistSeen(); continue; }
        if (!done) {
          if (s.heard && !G.prepped.has(rid)) { G.prepped.add(rid); push(Object.assign({}, s, { stage: 'in corso', reply: '', tools: [] })); }
          continue;
        }
        G.seen.add(rid); persistSeen();
        if (!s.heard && !s.errors.filter((c) => c !== 'stt-no-text-recognized').length) continue; // empty follow-up listening
        push(Object.assign(s, { stage: 'finita' }));
      }
    }
  }

  /* ---------- music, scripts, drive ---------- */
  G.onState = (e) => {
    const d = e.data, id = d.entity_id || '', n = d.new_state, o = d.old_state;
    if (id.startsWith('assist_satellite.')) { G.hotUntil = Date.now() + 25000; return; }
    if (id === 'media_player.library_vera' && n) {
      const a = n.attributes || {};
      if (n.state === 'playing') {
        G.music = { title: a.media_title, artist: a.media_artist, content_id: a.media_content_id, duration: a.media_duration,
          position: a.media_position, at: Date.parse(a.media_position_updated_at || n.last_updated) };
      } else if (o && o.state === 'playing' && G.music) {
        const m = G.music, pos = m.position != null ? m.position + (Date.now() - m.at) / 1000 : null;
        push({ kind: 'music_stop', time: NY(Date.now()), to: n.state, title: clean(m.title, 120), artist: clean(m.artist, 80),
          content_id: clean(m.content_id, 200), position_s: pos && Math.round(pos), duration_s: m.duration && Math.round(m.duration),
          near_end: !!(pos && m.duration && m.duration - pos < 15) });
        G.music = null;
      }
      return;
    }
    if (WATCH_SCRIPTS.includes(id) && o && n && o.state === 'on' && n.state === 'off') { scriptDone(id); return; }
    if (id === 'input_text.drive_richiesta' && n) push({ kind: 'drive', time: NY(Date.now()), state: clean(n.state, 300) });
  };
  async function scriptDone(id) {
    const item = id.split('.')[1];
    let err = '';
    try {
      const l = await WS({ type: 'trace/list', domain: 'script', item_id: item });
      const last = l.sort((a, b) => (a.timestamp.start < b.timestamp.start ? 1 : -1))[0];
      if (last) err = last.error || (last.script_execution && !['finished', 'cancelled'].includes(last.script_execution) ? last.script_execution : '');
    } catch (e) { err = 'trace non leggibile'; }
    push({ kind: 'script_end', time: NY(Date.now()), script: id, error: clean(err, 300) });
  }
  async function pollMusicAssistant() {
    try {
      const h = H();
      const r = await fetch('/api/hassio/addons/d5369777_music_assistant/logs', { headers: { Authorization: 'Bearer ' + h.auth.data.access_token } });
      if (!r.ok) return;
      const lines = (await r.text()).split('\n').filter((l) => MA_ERR.test(l)).slice(-20);
      const fresh = lines.filter((l) => !G.maSeen.has(l));
      lines.forEach((l) => G.maSeen.add(l));
      if (fresh.length && G.maReady) push({ kind: 'music_errors', lines: fresh.map((l) => clean(l.replace(/\x1b\[[0-9;]*m/g, ''), 200)) });
      G.maReady = true;
    } catch (e) {}
  }

  /* ---------- test sandbox ---------- */
  G.sandbox = false; G.sandboxLog = [];
  const ts = () => new Date().toLocaleTimeString('it-IT', { hour12: false });
  function testWrite(text) {
    return G.realSend({ type: 'call_service', domain: 'input_text', service: 'set_value',
      service_data: { value: (ts() + ' ' + text).slice(0, 255) }, target: { entity_id: TEST_OUT } }).catch(() => {});
  }
  function installSandbox() {
    const c = H().connection;
    if (c.__gSandbox) { G.realSend = c.__gRealSend; return; }
    G.realSend = c.__gRealSend = c.sendMessagePromise.bind(c);
    c.sendMessagePromise = (msg) => {
      if (G.sandbox && msg && msg.type === 'call_service' && !/^(get_|search$)/.test(msg.service || '')) {
        const what = msg.domain + '.' + msg.service + ' ' + JSON.stringify(msg.target || {}) + ' ' + JSON.stringify(msg.service_data || {});
        G.sandboxLog.push({ t: Date.now(), what });
        testWrite('AZIONE ' + what);
        return Promise.resolve({ context: { id: 'test' }, response: null });
      }
      return G.realSend(msg);
    };
    const realFetch = window.fetch.bind(window);
    window.fetch = (u, o) => {
      const url = String((u && u.url) || u);
      if (G.sandbox && /\/api\/services\//.test(url) && !/\/api\/services\/[^/]+\/(get_|search)/.test(url) && o && String(o.method || '').toUpperCase() === 'POST') {
        G.sandboxLog.push({ t: Date.now(), what: 'REST ' + url });
        testWrite('AZIONE ' + url.replace(/.*\/api\/services\//, '') + ' ' + String(o.body || '').slice(0, 150));
        return Promise.resolve(new Response('[]', { status: 200, headers: { 'Content-Type': 'application/json' } }));
      }
      return realFetch(u, o);
    };
    c.__gSandbox = true;
  }

  /* ---------- main loop ---------- */
  async function loop() {
    let lastMA = 0;
    while (!G.stopped) {
      try {
        handoffTick();
        await pollRuns();
        if (Date.now() - lastMA > 60000) { lastMA = Date.now(); await pollMusicAssistant(); }
        G.errors = 0;
      } catch (e) { G.errors++; if (G.errors === 5) push({ kind: 'poller_error', error: clean(e && e.message, 200) }); }
      await new Promise((r) => setTimeout(r, Date.now() < G.hotUntil ? 500 : 5000));
    }
  }

  G.init = async (opts) => {
    Object.assign(G.opts, opts || {});
    if (G.opts.share) { try { JSON.parse(localStorage.getItem('guardiano_seen') || '[]').forEach((x) => G.seen.add(x)); } catch (e) {} }
    const pl = await WS({ type: 'assist_pipeline/pipeline/list' });
    G.pipes = pl.pipelines;
    G.voicePipe = G.pipes.find((p) => /gerv/i.test(p.name)) || G.pipes[0];
    for (const p of G.pipes) {
      const l = await WS({ type: 'assist_pipeline/pipeline_debug/list', pipeline_id: p.id });
      (l.pipeline_runs || []).forEach((r) => { if (Date.now() - Date.parse(r.timestamp) > G.opts.oldMs) G.seen.add(r.pipeline_run_id); });
    }
    installSandbox();
    if (!G.unsub) G.unsub = await H().connection.subscribeEvents(G.onState, 'state_changed');
    if (!G.looping) { G.looping = true; loop(); }
    return 'pronto: ' + G.pipes.map((p) => p.name).join(', ');
  };

  /* Wait for events: returns a JSON string (cleaned of ? & =), [] after waitMs with nothing new. */
  G.mon = (waitMs) => new Promise((res) => {
    G.sandbox = false; // the previous batch is done
    const out = () => {
      // test events and real events never share a batch, so the sandbox covers exactly the test ones
      const first = G.queue[0];
      const isT = (e) => !!(e && e.test);
      let n = 0;
      while (n < G.queue.length && isT(G.queue[n]) === isT(first)) n++;
      const batch = G.queue.splice(0, n);
      G.sandbox = isT(first);
      res(JSON.stringify(batch));
    };
    if (G.queue.length) return out();
    const t = setTimeout(out, Math.min(waitMs || 30000, 30000));
    G.waiters.push(() => { clearTimeout(t); setTimeout(out, 300); });
  });
  window.__mon = G.mon;

  /* ---------- speaking ----------
     G.say(text, voiceEvent): speaks as Gervàsia from where the request came, after her reply has finished,
     with both microphones muted and restored as they were. voiceEvent can be null (then the ReSpeaker or the Sonos). */
  G.tts = async (text) => {
    const p = G.voicePipe;
    const r = await H().callApi('POST', 'tts_get_url', { engine_id: p.tts_engine, message: text, language: p.tts_language,
      options: { voice: p.tts_voice, preferred_format: 'mp3' } });
    return r.url;
  };
  G.say = async (text, v) => {
    const h = H();
    if (G.sandbox || (v && v.test)) {
      const w = v && v.intentEndMs ? v.intentEndMs + (v.replyLen || 0) * 70 + 1500 - Date.now() : 0;
      if (w > 0) await new Promise((r) => setTimeout(r, Math.min(w, 20000)));
      G.spoken.push({ t: Date.now(), text: text.toLowerCase().replace(/[^\p{L}\p{N} ]/gu, '') });
      await testWrite(text);
      return 'detto (test)';
    }
    const sat = (v && v.satellite && SATS[v.satellite]) ? v.satellite : DEFAULT_SAT;
    const cfg = SATS[sat];
    const wait = v && v.intentEndMs ? v.intentEndMs + (v.replyLen || 0) * 70 + 1500 - Date.now() : 0;
    const url = await G.tts(text);
    if (wait > 0) await new Promise((r) => setTimeout(r, Math.min(wait, 20000)));
    const before = MICS.map((m) => (h.states[m] || {}).state);
    await h.callService('switch', 'turn_on', {}, { entity_id: MICS });
    G.spoken.push({ t: Date.now(), text: text.toLowerCase().replace(/[^\p{L}\p{N} ]/gu, '') });
    let how = '';
    try {
      if ((h.states[cfg.sonos] || {}).state === 'on') { await h.callService('script', 'gervasio_annuncio', { url }); how = 'sonos'; }
      else if (cfg.announce) { await h.callService('assist_satellite', 'announce', { message: text, preannounce: false }, { entity_id: sat }); how = 'satellite'; }
      else { await h.callService('media_player', 'play_media', { media_content_id: url, media_content_type: 'music', announce: true }, { entity_id: cfg.mp }); how = 'media_player'; }
    } finally {
      setTimeout(() => {
        const off = MICS.filter((m, i) => before[i] !== 'on');
        if (off.length) H().callService('switch', 'turn_off', {}, { entity_id: off });
      }, text.length * 80 + 4000);
    }
    return 'detto (' + how + ')';
  };

  G.stop = () => { G.stopped = true; if (G.unsub) { G.unsub(); G.unsub = null; } };
})();
