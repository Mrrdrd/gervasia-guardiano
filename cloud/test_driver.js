/* Test driver for the "Gervàsia Test" pipeline: sends questions one at a time and times the guardian's answer,
   read from input_text.guardiano_test_legacy. Paste into a Home Assistant tab, then: await __T.run('tag')
   Fix, Nov 18 2026: every pipeline call is unsubscribed when it ends. Left open, the websocket library re-sent
   all of them after a reconnection, which replayed old questions to the guardian as new requests. */
(function () {
  const H = () => document.querySelector('home-assistant').hass;
  const T = (window.__T = window.__T || {});
  T.pipeline = '01m7qhkh29yjqas2187y83vqch';
  T.out = 'input_text.guardiano_test_legacy';
  T.W = []; T.L = {};
  T.ask = (text) => new Promise(async (resolve) => {
    let reply = '', done = false, unsub = null;
    const finish = () => { if (done) return; done = true; if (unsub) unsub().catch(() => {}); resolve(reply); };
    unsub = await H().connection.subscribeMessage((e) => {
      if (e.type === 'intent-end') { try { reply = e.data.intent_output.response.speech.plain.speech; } catch (x) {} }
      if (e.type === 'run-end' || e.type === 'error') finish();
    }, { type: 'assist_pipeline/run', start_stage: 'intent', end_stage: 'intent', input: { text }, pipeline: T.pipeline });
    if (done) unsub().catch(() => {});
    setTimeout(finish, 20000);
  });
  T.start = async () => {
    if (!T.sub) T.sub = await H().connection.subscribeEvents((e) => {
      const d = e.data; if (d.entity_id === T.out && d.new_state) T.W.push({ t: Date.now(), v: d.new_state.state });
    }, 'state_changed');
  };
  T.run = async (tag, questions) => {
    await T.start();
    T.L[tag] = [];
    for (const q of questions) {
      const t0 = Date.now();
      const reply = await T.ask(q);
      const tR = Date.now();
      let first = null;
      for (let k = 0; k < 150 && !first; k++) { await new Promise((r) => setTimeout(r, 500)); first = T.W.find((w) => w.t > t0 && !/^\S+ AZIONE/.test(w.v)); }
      if (first) { const tq = Date.now(); for (let k = 0; k < 120; k++) { const last = Math.max(t0, ...T.W.map((w) => w.t)); if (Date.now() - last > 10000 || Date.now() - tq > 60000) break; await new Promise((r) => setTimeout(r, 500)); } }
      T.L[tag].push({ q, reply, reply_s: (tR - t0) / 1000, first_s: first ? Math.round((first.t - t0) / 100) / 10 : null,
        writes: T.W.filter((w) => w.t > t0).map((w) => ({ s: Math.round((w.t - t0) / 100) / 10, v: w.v })) });
      try { localStorage.setItem('bench_' + tag, JSON.stringify(T.L[tag])); } catch (e) {}
      T.W = T.W.filter((w) => w.t > Date.now() - 1000);
      await new Promise((r) => setTimeout(r, 2000));
    }
    T.L[tag + '_done'] = true;
  };
})();
