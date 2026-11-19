/* Gervàsia guardiano: watcher and speaker for the cloud guardian runs.
   Load in the Home Assistant tab:  await import('/local/guardiano_poller.js?v=1')  (or a <script> tag)
   Then: await __G.init({runId: <number>})  once, and  await __G.mon(30000)  in a loop.
   Version 2, Nov 17 2026: test mode (localStorage guardiano_test = 1 makes typed requests count as spoken to the ReSpeaker).
   Version 3, Nov 18 2026: test environment. Requests on the "Gervàsia Test" pipeline arrive with test: true, in batches of
   their own. While such a batch is being handled (until the next __G.mon call) every action is sandboxed: nothing
   reaches the house, speech and actions are written to input_text.guardiano_test_cloud instead.
   Version 4: read-only services (get_..., search) really run in the sandbox.
   Version 5: dedicated test runs. __G.init({runId, share: false, testOnly: true, out: 'input_text.x'}) sees only test voice
   events, keeps the sandbox always on, writes to its own helper, marks localStorage guardiano_test_ready = runId when
   ready and returns {kind: "end"} from __G.mon once localStorage guardiano_test_end equals its runId. Production runs
   (testOnly false) now ignore test requests entirely.
   Version 6: test runs poll every 0.5 s, as a live run does while a satellite is active.
   Version 7: __G.mon returns at most about 900 characters (the javascript tool cuts longer results, which lost
   events and made runs re-read and repeat them); long events are trimmed (truncated: true), the rest stays queued.
   Version 8, closer to the add-on: voice events carry failed: true when her reply is an obvious failure ("non posso",
   "non riesco", errors...), and __G.q holds one-line read helpers so a run reads the house in one short call.
   Version 9: helpers return at most 450 characters, so one read plus its answer stays under the tool's cut, and
   __G.again() returns the last batch __G.mon delivered, for a run that lost it to a cut result (no 30 s wait).
   Version 10: microphones are muted and restored by script.guardiano_microfoni inside Home Assistant, not by a timer
   in this page: on Nov 18 the page dropped mid announcement and both microphones stayed muted for an hour.
   Version 11, Nov 19: PREFETCH. Voice events carry "dati": the reads the request's words call for, already done
   (lights, microphones, log, TV time, fix list, music, Netflix, ReSpeaker), so the guardian can act and speak in one
   round instead of reading first. Tested Nov 19 on 15 questions: 6.3 s average against 9.2 s for version 10.
   The fix list helper shortens each title to 40 characters so more items fit. */
(function () {
  if (window.__G && window.__G.v === 11) return;
  const G = (window.__G = { v: 11 });
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
  const OBVIOUS = /non posso|non riesco|non ho accesso|non ho (?:uno |gli |lo |un )?strument|non trovo|non è possibile|non sono in grado|insufficient funds|si è verificato un errore/i;

  G.opts = { runId: null, share: true, oldMs: 120000, testOnly: false, out: TEST_OUT };
  G.seen = new Set(); G.prepped = new Set(); G.queue = []; G.waiters = [];
  G.hotUntil = 0; G.music = null; G.spoken = []; G.maSeen = new Set(); G.handoffSent = false; G.errors = 0;

  function push(ev) {
    if (G.opts.testOnly && !(ev && (ev.test || ev.kind === 'end'))) return; // a test run never sees real events
    G.queue.push(ev); const w = G.waiters.splice(0); w.forEach((f) => f()); }
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
      failed: OBVIOUS.test(reply) || ev.some((e) => e.type === 'error' && !['stt-no-text-recognized', 'duplicate_wake_up_detected'].includes(e.data && e.data.code)),
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
        if (!!s.test !== !!G.opts.testOnly) { G.seen.add(rid); continue; } // tests only to test runs, real only to production
        if (s.errors.length && s.errors.every((c) => c === 'duplicate_wake_up_detected') && !s.heard) { G.seen.add(rid); persistSeen(); continue; }
        if (isOwnEcho(s.heard)) { G.seen.add(rid); persistSeen(); continue; }
        if (!done) {
          if (s.heard && !G.prepped.has(rid)) { G.prepped.add(rid); const dati = await prefetch(s.heard); if (dati) G.preSent.add(rid); push(Object.assign({}, s, { stage: 'in corso', reply: '', tools: [] }, dati ? { dati } : {})); }
          continue;
        }
        G.seen.add(rid); persistSeen();
        if (!s.heard && !s.errors.filter((c) => c !== 'stt-no-text-recognized').length) continue; // empty follow-up listening
        if (!G.preSent.has(rid) && s.heard) { const dati = await prefetch(s.heard); if (dati) s.dati = dati.slice(0, 350); }
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
      service_data: { value: (ts() + ' ' + text).slice(0, 255) }, target: { entity_id: G.opts.out || TEST_OUT } }).catch(() => {});
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
        if (G.opts.testOnly) {
          try { if (!G.endSent && localStorage.getItem('guardiano_test_end') === String(G.opts.runId)) { G.endSent = true; push({ kind: 'end' }); } } catch (e) {}
        } else if (Date.now() - lastMA > 60000) { lastMA = Date.now(); await pollMusicAssistant(); }
        G.errors = 0;
      } catch (e) { G.errors++; if (G.errors === 5) push({ kind: 'poller_error', error: clean(e && e.message, 200) }); }
      // a test run polls fast all the time: typed tests move no satellite, which is what makes a live run poll fast
      await new Promise((r) => setTimeout(r, (G.opts.testOnly || Date.now() < G.hotUntil) ? 500 : 5000));
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
    if (G.opts.testOnly) { G.sandbox = true; try { localStorage.setItem('guardiano_test_ready', String(G.opts.runId)); } catch (e) {} }
    return 'pronto: ' + G.pipes.map((p) => p.name).join(', ');
  };

  /* Wait for events: returns a JSON string (cleaned of ? & =), [] after waitMs with nothing new. */
  G.mon = (waitMs) => new Promise((res) => {
    G.sandbox = !!G.opts.testOnly; // the previous batch is done (a test run stays sandboxed throughout)
    const out = () => {
      // test events and real events never share a batch, so the sandbox covers exactly the test ones
      const first = G.queue[0];
      const isT = (e) => !!(e && e.test);
      // the javascript tool cuts results at about 1000 characters: return only what fits, the rest stays queued
      const LIMIT = 900;
      const fit = (e) => {
        let j = JSON.stringify(e);
        if (j.length <= LIMIT) return e;
        const c = Object.assign({}, e, { tools: (e.tools || []).slice(-3).map((t) => String(t).slice(0, 120)), truncated: true });
        if (JSON.stringify(c).length > LIMIT) c.reply = String(c.reply || '').slice(0, 250);
        if (JSON.stringify(c).length > LIMIT) c.tools = (c.tools || []).slice(-1).map((t) => String(t).slice(0, 80));
        return c;
      };
      const batch = [];
      let size = 2;
      while (G.queue.length && isT(G.queue[0]) === isT(first)) {
        const e = fit(G.queue[0]);
        const len = JSON.stringify(e).length + 1;
        if (batch.length && size + len > LIMIT) break;
        batch.push(e); size += len; G.queue.shift();
      }
      G.sandbox = !!G.opts.testOnly || isT(first);
      if (batch.length) G.lastBatch = { t: Date.now(), batch };
      res(JSON.stringify(batch));
    };
    if (G.queue.length) return out();
    const t = setTimeout(out, Math.min(waitMs || 30000, 30000));
    G.waiters.push(() => { clearTimeout(t); setTimeout(out, 300); });
  });
  window.__mon = G.mon;
  /* The last batch __G.mon returned (and how many seconds ago), for a run whose result was cut. */
  G.again = () => (G.lastBatch ? JSON.stringify({ seconds_ago: Math.round((Date.now() - G.lastBatch.t) / 1000), events: G.lastBatch.batch }) : '[]');

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
      // no wait for her reply here: test timings measure the guardian only (same as the add-on)
      G.spoken.push({ t: Date.now(), text: text.toLowerCase().replace(/[^\p{L}\p{N} ]/gu, '') });
      await testWrite(text);
      return 'detto (test)';
    }
    const sat = (v && v.satellite && SATS[v.satellite]) ? v.satellite : DEFAULT_SAT;
    const cfg = SATS[sat];
    const wait = v && v.intentEndMs ? v.intentEndMs + (v.replyLen || 0) * 70 + 1500 - Date.now() : 0;
    const url = await G.tts(text);
    if (wait > 0) await new Promise((r) => setTimeout(r, Math.min(wait, 20000)));
    // which microphones to switch back on at the end: those that were live before we spoke. While an earlier
    // announcement is still muting them, keep its list (they read as muted because of us, not of a person).
    const secs = Math.ceil((text.length * 80 + 4000) / 1000);
    if (!(G.micUntil > Date.now())) G.micRestore = MICS.filter((m) => (h.states[m] || {}).state !== 'on');
    G.micUntil = Date.now() + secs * 1000;
    await h.callService('script', 'turn_on', { variables: { secondi: secs, ripristina: G.micRestore } }, { entity_id: 'script.guardiano_microfoni' });
    await new Promise((r) => setTimeout(r, 300));
    G.spoken.push({ t: Date.now(), text: text.toLowerCase().replace(/[^\p{L}\p{N} ]/gu, '') });
    let how = '';
    try {
      if ((h.states[cfg.sonos] || {}).state === 'on') { await h.callService('script', 'gervasio_annuncio', { url }); how = 'sonos'; }
      else if (cfg.announce) { await h.callService('assist_satellite', 'announce', { message: text, preannounce: false }, { entity_id: sat }); how = 'satellite'; }
      else { await h.callService('media_player', 'play_media', { media_content_id: url, media_content_type: 'music', announce: true }, { entity_id: cfg.mp }); how = 'media_player'; }
    } finally {
      // nothing to do here: script.guardiano_microfoni restores the microphones itself
    }
    return 'detto (' + how + ')';
  };

  /* ---------- prefetch: the reads a request's words call for, done before the guardian sees it ---------- */
  G.preSent = new Set();
  const PRE = [
    [/\blog\b|error/i, () => G.q.errors(5)],
    [/luc[ei]\b|lampad/i, () => G.q.on('light')],
    [/microfon|\bmut[oi]\b/i, () => G.q.mics()],
    [/offline|non disponibil|dispositivi/i, () => G.q.unavailable()],
    [/(tv|televisore|tele)\b.*(acces|tempo)|(acces|tempo).*(tv|televisore)/i, () => G.q.onTime('switch.tv_power')],
    [/lista|sistemare|da fare/i, () => G.q.todo()],
    [/musica|canzone|brano|partita la/i, () => G.q.log('media_player.library_vera', 1440)],
    [/volume|sonos/i, () => G.q.states(['media_player.library_vera'])],
    [/netflix/i, () => G.q.traces('guarda_su_pc_worker', 3)],
    [/respeaker|parlato da sol|box\b/i, () => G.q.log('assist_satellite.respeaker_salotto_assist_satellite', 720)],
  ];
  async function prefetch(heard) {
    const hits = PRE.filter(([re]) => re.test(heard || '')).slice(0, 2);
    if (!hits.length) return '';
    const r = await Promise.all(hits.map(([, f]) => Promise.resolve().then(f).catch(() => '')));
    return clean(r.filter(Boolean).join(' || '), 500);
  }

  /* ---------- read helpers: each returns one short string (under 850 characters, cleaned of ? & =) ---------- */
  const cut = (s) => clean(s, 450);
  const nm = (id) => { const s = H().states[id]; return (s && s.attributes.friendly_name) || id; };
  const hm = (t) => new Date(t).toLocaleTimeString('it-IT', { timeZone: 'America/New_York', hour12: false }).slice(0, 5);
  G.q = {
    // system log entries of the last N minutes (default 5)
    errors: async (min) => {
      const since = Date.now() / 1000 - 60 * (min || 5);
      const l = (await WS({ type: 'system_log/list' })).filter((r) => (r.timestamp || 0) >= since);
      return cut(l.length ? l.map((r) => r.level + ' ' + r.name + ' x' + r.count + ': ' + String(r.message).slice(0, 90)).join(' | ') : 'nessun errore negli ultimi ' + (min || 5) + ' minuti');
    },
    // what is on in a domain (default light), grouped by room: Cync and other hubs expose the same lamp several
    // times (room, group, bulb), so count rooms and lamps, never raw entities
    on: async (domain) => {
      const st = H().states;
      const all = Object.values(st).filter((s) => s.entity_id.startsWith((domain || 'light') + '.') && s.state === 'on');
      if (!G.reg) {
        const [ents, devs, areas] = await Promise.all([WS({ type: 'config/entity_registry/list' }), WS({ type: 'config/device_registry/list' }), WS({ type: 'config/area_registry/list' })]);
        G.reg = { ents: Object.fromEntries(ents.map((e) => [e.entity_id, e])), devs: Object.fromEntries(devs.map((d) => [d.id, d])), areas: Object.fromEntries(areas.map((a) => [a.area_id, a.name])) };
      }
      const R = G.reg, rooms = {}, leds = [];
      all.forEach((s) => {
        if (/screen|led|ring/i.test(s.entity_id)) { leds.push(s.attributes.friendly_name); return; }
        const e = R.ents[s.entity_id] || {}, d = R.devs[e.device_id] || {};
        const room = R.areas[e.area_id || d.area_id] || d.name_by_user || d.name || s.attributes.friendly_name;
        (rooms[room] = rooms[room] || new Set()).add(d.id || s.entity_id);
      });
      const names = Object.keys(rooms);
      return cut(names.length ? 'accese in ' + names.length + ' posti: ' + names.map((r) => r + (rooms[r].size > 1 ? ' (' + rooms[r].size + ' dispositivi)' : '')).join(', ') + (leds.length ? ' | spie e schermi, non luci: ' + leds.join(', ') : '') : 'nessuna accesa');
    },
    unavailable: () => {
      const l = Object.values(H().states).filter((s) => s.state === 'unavailable' && !/^(sensor|binary_sensor|button|update|event|number|select)\./.test(s.entity_id));
      return cut(l.length + ' non disponibili: ' + l.map((s) => s.attributes.friendly_name || s.entity_id).join(', '));
    },
    todo: async (id) => {
      const r = await WS({ type: 'todo/item/list', entity_id: id || 'todo.gervasio_da_sistemare' });
      const open = (r.items || []).filter((i) => i.status !== 'completed');
      return cut(open.length + ' aperte: ' + open.map((i) => String(i.summary).slice(0, 40)).join(' | '));
    },
    // minutes on and sessions since local midnight (or the last N hours)
    onTime: async (id, hours) => {
      const now = new Date(), start = hours ? new Date(Date.now() - hours * 3600e3) : new Date(now.getFullYear(), now.getMonth(), now.getDate());
      const h = await WS({ type: 'history/history_during_period', start_time: start.toISOString(), entity_ids: [id], minimal_response: true, no_attributes: true });
      const l = (h[id] || []).map((x) => ({ s: x.s, t: (x.lu || x.lc) * 1000 }));
      let mins = 0, sess = [];
      l.forEach((x, i) => { if (x.s === 'on') { const end = (l[i + 1] && l[i + 1].t) || Date.now(); mins += (end - x.t) / 60000; sess.push(hm(x.t) + '-' + (l[i + 1] ? hm(end) : 'ora')); } });
      return cut(nm(id) + ': ' + Math.round(mins) + ' minuti accesa in ' + sess.length + ' volte (' + sess.join(', ') + '), ora ' + ((H().states[id] || {}).state));
    },
    // logbook of one entity, last N minutes (default 720), with who caused each change
    log: async (id, min) => {
      const r = await WS({ type: 'logbook/get_events', start_time: new Date(Date.now() - 60000 * (min || 720)).toISOString(), entity_ids: [id] });
      return cut(r.slice(-12).map((e) => hm(e.when * 1000) + ' ' + (e.state || e.message) + (e.context_user_id ? ' (persona)' : e.context_entity_id ? ' (da ' + e.context_entity_id + ')' : e.context_event_type ? ' (' + e.context_event_type + ')' : '')).join(' | ') || 'niente nel registro');
    },
    traces: async (script, n) => {
      const l = (await WS({ type: 'trace/list', domain: 'script', item_id: script })).sort((a, b) => (a.timestamp.start < b.timestamp.start ? 1 : -1)).slice(0, n || 2);
      return cut(l.map((t) => hm(t.timestamp.start) + ' ' + t.state + (t.script_execution ? ' ' + t.script_execution : '') + (t.error ? ' errore: ' + t.error : '') + (t.timestamp.finish ? ' durata ' + Math.round((Date.parse(t.timestamp.finish) - Date.parse(t.timestamp.start)) / 1000) + ' s' : '')).join(' | ') || 'nessuna traccia');
    },
    mics: () => cut(MICS.map((m) => nm(m) + ': ' + ((H().states[m] || {}).state === 'on' ? 'microfono MUTO' : 'microfono attivo')).join(' | ')),
    // compact states with volume and title, for a few entities
    states: (ids) => cut([].concat(ids).map((id) => { const s = H().states[id]; if (!s) return id + ' non trovato'; const a = s.attributes; return (a.friendly_name || id) + ': ' + s.state + (a.volume_level != null ? ' vol ' + Math.round(a.volume_level * 100) + '%' : '') + (a.is_volume_muted ? ' muto' : '') + (a.media_title ? ' "' + a.media_title + '"' : ''); }).join(' | ')),
    // find entities by words in the id or name
    find: (text) => { const w = String(text).toLowerCase(); return cut(Object.values(H().states).filter((s) => (s.entity_id + ' ' + (s.attributes.friendly_name || '')).toLowerCase().includes(w)).slice(0, 15).map((s) => s.entity_id + '=' + s.state).join(' | ')); },
  };

  G.stop = () => { G.stopped = true; if (G.unsub) { G.unsub(); G.unsub = null; } };
})();
