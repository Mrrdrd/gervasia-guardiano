/* Test driver for the "Gervàsia Test" pipeline: sends questions one at a time and times the guardian's answer,
   read from input_text.guardiano_test_legacy. Paste into a Home Assistant tab, then __auto(runId, tag).
   Nov 18 2026: every pipeline call is unsubscribed when it ends (left open, they were re-sent after a reconnection).
   Nov 19 2026: waits run on a Web Worker clock (Chrome throttles timers in a background tab to once a minute, which
   froze the driver); the 20 s timeout is armed before subscribing (a subscribe that never answered hung it); the
   state subscription is re-made after a reconnection; a run id guards against two driver loops at once. */
(function () {
  const H = () => document.querySelector('home-assistant').hass;
  const T = (window.__T = window.__T || {});
  T.pipeline = '01m7qhkh29yjqas2187y83vqch';
  T.out = 'input_text.guardiano_test_legacy';
  T.W = []; T.L = T.L || {};
  const wk = new Worker(URL.createObjectURL(new Blob(['onmessage=e=>setTimeout(()=>postMessage(e.data.id),e.data.ms)'])));
  const pend = {}; let nid = 0;
  wk.onmessage = (e) => { const f = pend[e.data]; delete pend[e.data]; if (f) f(); };
  const S = (T.sleep = (ms) => new Promise((r) => { const id = ++nid; pend[id] = r; wk.postMessage({ id, ms }); }));
  T.ask = (text) => new Promise((resolve) => {
    let reply = '', done = false, unsub = null;
    const finish = () => { if (done) return; done = true; if (unsub) unsub().catch(() => {}); resolve(reply); };
    S(20000).then(finish);
    H().connection.subscribeMessage((e) => {
      if (e.type === 'intent-end') { try { reply = e.data.intent_output.response.speech.plain.speech; } catch (x) {} }
      if (e.type === 'run-end' || e.type === 'error') finish();
    }, { type: 'assist_pipeline/run', start_stage: 'intent', end_stage: 'intent', input: { text }, pipeline: T.pipeline })
      .then((u) => { unsub = u; if (done) u().catch(() => {}); }).catch(finish);
  });
  T.start = async () => {
    if (T.sub) { try { await T.sub(); } catch (e) {} }
    T.sub = await H().connection.subscribeEvents((e) => {
      const d = e.data; if (d.entity_id === T.out && d.new_state) T.W.push({ t: Date.now(), v: d.new_state.state });
    }, 'state_changed');
    if (!T.rc) { T.rc = true; H().connection.addEventListener('ready', () => { T.sub = null; T.start(); }); }
  };
  T.run = async (tag, questions) => {
    const me = (T.active = tag + Date.now());
    await T.start(); T.L[tag] = [];
    for (const q of questions) {
      if (T.active !== me) return;
      const t0 = Date.now(); const reply = await T.ask(q); const tR = Date.now();
      let first = null;
      while (!first && Date.now() - t0 < 75000) { await S(500); first = T.W.find((w) => w.t > t0 && !/^\S+ AZIONE/.test(w.v)); }
      if (first) { const tq = Date.now(); while (true) { const last = Math.max(t0, ...T.W.map((w) => w.t)); if (Date.now() - last > 10000 || Date.now() - tq > 60000) break; await S(500); } }
      T.L[tag].push({ q, t0, reply, reply_s: (tR - t0) / 1000, first_s: first ? Math.round((first.t - t0) / 100) / 10 : null,
        writes: T.W.filter((w) => w.t > t0).map((w) => ({ s: Math.round((w.t - t0) / 100) / 10, v: w.v })) });
      try { localStorage.setItem('bench_' + tag, JSON.stringify(T.L[tag])); } catch (e) {}
      T.W = T.W.filter((w) => w.t > Date.now() - 1000); await S(2000);
    }
    T.L[tag + '_done'] = true;
  };
  window.__auto = async (rid, tag, qs) => {
    const t0 = Date.now(); window.__autoState = tag + ' waiting';
    while (localStorage.getItem('guardiano_test_ready') !== String(rid)) await S(1000);
    while (true) { const s = H().states[T.out]; if (s && /sandbox_check/.test(s.state) && new Date(s.last_changed).getTime() > t0) break; await S(1000); }
    await S(5000); window.__autoState = tag + ' running';
    await T.run(tag, qs || window.__Q22);
    await S(15000); localStorage.setItem('guardiano_test_end', String(rid)); window.__autoState = tag + ' ended';
  };
})();
