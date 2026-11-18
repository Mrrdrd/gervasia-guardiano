"""Gervàsia guardiano.

Watches every voice request that Home Assistant's voice pipelines handle, and for each
finished request asks a Claude model whether the fast assistant served the person well.
The model can read the house (states, logbook, history, script traces, system log) and,
outside shadow mode, act on it (call services, speak from the speakers, notify, add to-dos).

Everything specific to one home lives outside this file:
  * instructions_file: the guardian's instructions (plain text, read at every decision)
  * impostazioni.json next to it: which switches and scripts belong to which satellite
"""

import asyncio
import collections
import datetime as dt
import json
import os
import random
import time
import traceback

import aiohttp
import anthropic

OPTIONS_PATH = "/data/options.json"
HA_WS = "ws://supervisor/core/websocket"
HA_API = "http://supervisor/core/api"
TOKEN = os.environ.get("SUPERVISOR_TOKEN", "")

# Services the guardian may never call, whatever the instructions say.
BLOCKED_DOMAINS = {
    "homeassistant", "hassio", "update", "backup", "lock", "alarm_control_panel",
    "shell_command", "recorder", "system_log", "logger", "zha", "zwave_js", "esphome",
    "person", "auth", "cloud", "python_script", "pyscript",
}

HOT_WINDOW_S = 25
HOT_POLL_S = 0.5
IDLE_POLL_S = 5
MAX_TOOL_TURNS = 10
READ_TOOLS = {"get_states", "get_history", "get_logbook", "get_traces", "reconstruct"}
PREP_RULES = ("\n\nPREPARATION. Gervàsia is answering this request right now and you do not see her reply yet. "
              "Prepare the one or two short Italian sentences you would speak, as Gervàsia, if her reply turns out "
              "useless or wrong: open with the comic handoff line of 3.3, then the real answer. Use the read tools only "
              "if the context is not enough. If the request needs an action in the house (turning something on, "
              "playing something) rather than an answer, or it is chatter not meant for her, reply exactly NESSUNA. "
              "Reply with only the sentences, nothing else.")
REPORT_QUIET_S = 30 * 60
HOLD_LINES = ["Un attimo, controllo.", "Aspetta, do un'occhiata.", "Un secondo, verifico.",
              "Fammi controllare un attimo.", "Dammi un momento, guardo."]


def log(*a):
    print(dt.datetime.now().strftime("%H:%M:%S"), *a, flush=True)


def load_options():
    with open(OPTIONS_PATH, encoding="utf-8") as f:
        return json.load(f)


def hhmmss(w):
    if isinstance(w, (int, float)):
        return dt.datetime.fromtimestamp(w).astimezone().strftime("%H:%M:%S")
    return str(w or "")[11:19]


def compact_logbook(rows, limit):
    """Logbook rows as short tuples: time, entity, new state or message, cause."""
    out = []
    for r in rows:
        e = str(r.get("entity_id") or r.get("domain") or "")
        if e.startswith(("sensor.", "binary_sensor.", "assist_satellite.", "update.", "sun.")):
            continue
        cause = "persona" if r.get("context_user_id") else (r.get("context_entity_id") or r.get("context_event_type") or "")
        out.append([hhmmss(r.get("when")), e, (r.get("state") or r.get("message") or "")[:60], cause])
    return out[-limit:]


def short(v, n=4000):
    s = v if isinstance(v, str) else json.dumps(v, ensure_ascii=False, default=str)
    return s if len(s) <= n else s[:n] + "...[troncato]"


class HA:
    """Minimal Home Assistant client: one websocket for commands and events, REST for the rest."""

    def __init__(self, session):
        self.s = session
        self.ws = None
        self.next_id = 1
        self.pending = {}
        self.event_handlers = {}
        self.reader = None

    async def connect(self):
        self.ws = await self.s.ws_connect(HA_WS, heartbeat=30, max_msg_size=0)
        msg = await self.ws.receive_json()
        if msg.get("type") != "auth_required":
            raise RuntimeError("unexpected hello " + str(msg))
        await self.ws.send_json({"type": "auth", "access_token": TOKEN})
        msg = await self.ws.receive_json()
        if msg.get("type") != "auth_ok":
            raise RuntimeError("auth failed " + str(msg))
        self.reader = asyncio.create_task(self._read())
        log("connected to Home Assistant", msg.get("ha_version"))

    async def _read(self):
        async for m in self.ws:
            if m.type != aiohttp.WSMsgType.TEXT:
                continue
            data = json.loads(m.data)
            for msg in data if isinstance(data, list) else [data]:
                mid = msg.get("id")
                if msg.get("type") == "event" and mid in self.event_handlers:
                    try:
                        self.event_handlers[mid](msg["event"])
                    except Exception:
                        traceback.print_exc()
                elif mid in self.pending:
                    fut = self.pending.pop(mid)
                    if not fut.done():
                        if msg.get("success", True):
                            fut.set_result(msg.get("result"))
                        else:
                            fut.set_exception(RuntimeError(json.dumps(msg.get("error"))))
        for fut in self.pending.values():
            if not fut.done():
                fut.set_exception(RuntimeError("websocket closed"))
        self.pending.clear()

    async def call(self, payload, timeout=30):
        mid = self.next_id
        self.next_id += 1
        fut = asyncio.get_running_loop().create_future()
        self.pending[mid] = fut
        await self.ws.send_json({"id": mid, **payload})
        return await asyncio.wait_for(fut, timeout)

    async def subscribe(self, event_type, handler):
        mid = self.next_id
        self.event_handlers[mid] = handler
        await self.call({"type": "subscribe_events", "event_type": event_type})
        # call() used the next id; register the handler under the id actually used
        self.event_handlers[mid] = handler

    async def rest(self, method, path, body=None):
        async with self.s.request(method, HA_API + path, json=body,
                                  headers={"Authorization": "Bearer " + TOKEN}) as r:
            text = await r.text()
            try:
                return r.status, json.loads(text)
            except Exception:
                return r.status, text

    async def service(self, domain, service, data=None, target=None):
        payload = {"type": "call_service", "domain": domain, "service": service,
                   "service_data": data or {}}
        if target:
            payload["target"] = target
        return await self.call(payload)

    async def state(self, entity_id):
        st, js = await self.rest("GET", "/states/" + entity_id)
        return js if st == 200 else None


class Guardian:
    def __init__(self, ha, opts):
        self.ha = ha
        self.opts = opts
        self.claude = anthropic.AsyncAnthropic(api_key=opts["anthropic_api_key"])
        self.seen = set()
        self.history = collections.deque(maxlen=15)
        self.hot_until = 0.0
        self.spoken = collections.deque(maxlen=10)   # (time, text) said by the guardian
        self.pipelines = {}
        self.lock = asyncio.Lock()
        self.speak_lock = asyncio.Lock()
        self.users = {}
        self.preps = {}   # pipeline_run_id -> task preparing an answer while Gervàsia is still answering
        self.reported = collections.deque(maxlen=30)   # (time, kind, text) alerts and to-dos already raised
        self.shadow = bool(opts.get("shadow_mode", True))

    # ---------- settings ----------
    def settings(self):
        path = os.path.join(os.path.dirname(self.opts["instructions_file"]), "impostazioni.json")
        try:
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}

    def instructions(self):
        try:
            with open(self.opts["instructions_file"], encoding="utf-8") as f:
                return f.read()
        except Exception as e:
            return "Istruzioni non trovate (%s). Non agire: limitati a descrivere cosa faresti." % e

    # ---------- watching ----------
    def on_state(self, event):
        eid = (event.get("data") or {}).get("entity_id", "")
        if eid.startswith("assist_satellite."):
            self.hot_until = time.monotonic() + HOT_WINDOW_S

    async def load_pipelines(self):
        res = await self.ha.call({"type": "assist_pipeline/pipeline/list"})
        self.pipelines = {p["id"]: p for p in res["pipelines"]}

    async def poll_once(self, first=False):
        for pid, p in self.pipelines.items():
            lst = await self.ha.call({"type": "assist_pipeline/pipeline_debug/list", "pipeline_id": pid})
            for run in lst.get("pipeline_runs", []):
                rid = run["pipeline_run_id"]
                if rid in self.seen:
                    continue
                if first:
                    self.seen.add(rid)
                    continue
                det = await self.ha.call({"type": "assist_pipeline/pipeline_debug/get",
                                          "pipeline_id": pid, "pipeline_run_id": rid})
                ev = det.get("events", [])
                types = [e["type"] for e in ev]
                if "intent-end" not in types and "error" not in types and "run-end" not in types:
                    if rid not in self.preps:
                        heard = self.heard_text(ev)
                        if heard:
                            self.preps[rid] = asyncio.create_task(self.prepare(heard, p))
                    continue   # still running, look again later
                self.seen.add(rid)
                summary = self.summarise(p, run, ev)
                if summary is None:
                    task = self.preps.pop(rid, None)
                    if task:
                        task.cancel()
                    continue
                summary["_rid"] = rid
                if rid not in self.preps and summary.get("heard"):
                    # the run finished between two polls: start preparing now
                    self.preps[rid] = asyncio.create_task(self.prepare(summary["heard"], p))
                self.history.append(summary)
                asyncio.create_task(self.decide(summary))

    def test_mode(self):
        """A file named "test" next to the instructions makes typed requests count as spoken (silent tests)."""
        return os.path.exists(os.path.join(os.path.dirname(self.opts["instructions_file"]), "test"))

    @staticmethod
    def heard_text(events):
        for e in events:
            if e["type"] == "stt-end":
                return ((e.get("data") or {}).get("stt_output") or {}).get("text", "")
        for e in events:
            if e["type"] == "intent-start":
                return (e.get("data") or {}).get("intent_input", "") or ""
        return ""

    async def prepare(self, heard, pipeline):
        """While Gervàsia is answering, prepare what the guardian would say if she fails."""
        t0 = time.monotonic()
        system = self.instructions() + PREP_RULES
        previous = [{k: v for k, v in r.items() if not k.startswith("_")} for r in list(self.history)][-10:]
        try:
            ctx = await self.prefetch()
            messages = [{"role": "user", "content": json.dumps({
                "ora": dt.datetime.now().astimezone().strftime("%A %d %B %Y, %H:%M:%S %Z"),
                "richiesta_in_corso": heard, "assistente": pipeline.get("name"),
                "richieste_precedenti": previous, "contesto_casa": ctx}, ensure_ascii=False, default=str)}]
            read_tools = [t for t in self.tool_specs() if t["name"] in READ_TOOLS]
            draft = ""
            for _ in range(5):
                resp = await self.claude.messages.create(
                    model=self.opts.get("model", "claude-sonnet-5-5"), max_tokens=600,
                    system=[{"type": "text", "text": system, "cache_control": {"type": "ephemeral"}}],
                    tools=read_tools, messages=messages)
                messages.append({"role": "assistant", "content": resp.content})
                uses = [b for b in resp.content if b.type == "tool_use"]
                draft = " ".join(b.text for b in resp.content if b.type == "text").strip()
                if not uses:
                    break
                results = []
                for u in uses:
                    out = await self.run_tool(u.name, u.input or {}, {}, []) if u.name in READ_TOOLS else "non disponibile"
                    results.append({"type": "tool_result", "tool_use_id": u.id, "content": out})
                messages.append({"role": "user", "content": results})
            if not draft or draft.upper().startswith("NESSUNA"):
                draft = ""
            return {"draft": draft, "secs": round(time.monotonic() - t0, 1)}
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log("prepare error", e)
            return {"draft": "", "secs": round(time.monotonic() - t0, 1), "error": str(e)}

    def summarise(self, pipeline, run, events):
        get = lambda t: next((e for e in events if e["type"] == t), None)
        rs, stt, ie = get("run-start"), get("stt-end"), get("intent-end")
        errors = [e["data"] for e in events if e["type"] == "error"]
        if errors and all((e or {}).get("code") == "duplicate_wake_up_detected" for e in errors) and not stt:
            return None
        heard = ((stt or {}).get("data") or {}).get("stt_output", {}).get("text", "")
        typed = False
        if not heard:
            ist = get("intent-start")
            heard = ((ist or {}).get("data") or {}).get("intent_input", "") or ""
            typed = bool(heard)
        reply = ""
        cont = None
        if ie:
            out = ie["data"].get("intent_output", {})
            reply = out.get("response", {}).get("speech", {}).get("plain", {}).get("speech", "")
            cont = out.get("continue_conversation")
        tools = []
        for e in events:
            d = (e.get("data") or {}).get("chat_log_delta") or {}
            for tc in d.get("tool_calls") or []:
                tools.append({"tool": tc.get("tool_name"), "args": tc.get("tool_args")})
            if d.get("role") == "tool_result":
                tools.append({"result_of": d.get("tool_name"), "result": short(d.get("tool_result"), 600)})
        # our own announcement picked up by a microphone: ignore
        now = time.time()
        for t, said in self.spoken:
            if now - t < 30 and heard and (heard.lower().strip(" .") in said.lower() or said.lower() in heard.lower()):
                return None
        return {
            "time": run.get("timestamp"),
            "assistant": pipeline.get("name"),
            "pipeline_id": pipeline.get("id"),
            "satellite_id": ((rs or {}).get("data") or {}).get("satellite_id")
                            or (self.settings().get("default_satellite") if typed and self.test_mode() else None),
            "conversation_id": ((rs or {}).get("data") or {}).get("conversation_id"),
            "heard": heard,
            "typed_not_spoken": typed and not self.test_mode(),
            "reply": reply,
            "continue_conversation": cont,
            "tool_calls": tools,
            "errors": errors,
            "_intent_end_monotonic": time.monotonic(),
        }

    async def watch(self):
        await self.load_pipelines()
        await self.poll_once(first=True)
        await self.ha.subscribe("state_changed", self.on_state)
        log("watching %d pipelines, shadow mode %s" % (len(self.pipelines), self.shadow))
        tick = 0
        while True:
            hot = time.monotonic() < self.hot_until
            await asyncio.sleep(HOT_POLL_S if hot else IDLE_POLL_S)
            try:
                await self.poll_once()
                tick += 1
                if tick % 120 == 0:
                    await self.load_pipelines()
            except Exception as e:
                log("poll error", e)
                raise

    # ---------- tools ----------
    def tool_specs(self):
        return [
            {"name": "get_states", "description": "Read current states. Give entity_ids, or a search text matched against entity_id and friendly name (max 40 results).",
             "input_schema": {"type": "object", "properties": {
                 "entity_ids": {"type": "array", "items": {"type": "string"}},
                 "search": {"type": "string"}}}},
            {"name": "get_history", "description": "State history of some entities over the last N minutes.",
             "input_schema": {"type": "object", "properties": {
                 "entity_ids": {"type": "array", "items": {"type": "string"}},
                 "minutes": {"type": "integer"}}, "required": ["entity_ids"]}},
            {"name": "get_logbook", "description": "Logbook further back than the 10 minutes you already have (minutes must be more than 10), optionally for one entity. For one or a few entities prefer reconstruct.",
             "input_schema": {"type": "object", "properties": {
                 "minutes": {"type": "integer"}, "entity_id": {"type": "string"}}}},
            {"name": "get_traces", "description": "Recent run traces of a script or automation (domain script or automation, item_id without the domain).",
             "input_schema": {"type": "object", "properties": {
                 "domain": {"type": "string", "enum": ["script", "automation"]},
                 "item_id": {"type": "string"}, "count": {"type": "integer"}}, "required": ["domain", "item_id"]}},
            {"name": "reconstruct", "description": "Everything that happened to some entities in the last N minutes (default 180, max 720), in one call: every state change with its time, whether a person, an automation or a script caused it, and the user name when a person did. Use it first for any 'why did X happen' question.",
             "input_schema": {"type": "object", "properties": {
                 "entity_ids": {"type": "array", "items": {"type": "string"}},
                 "minutes": {"type": "integer"}}, "required": ["entity_ids"]}},
            {"name": "call_service", "description": "Call a Home Assistant action (service). Some domains are blocked.",
             "input_schema": {"type": "object", "properties": {
                 "domain": {"type": "string"}, "service": {"type": "string"},
                 "data": {"type": "object"}, "target": {"type": "object"}}, "required": ["domain", "service"]}},
            {"name": "speak", "description": "Say one or two short sentences, as Gervàsia, from where the request came from. Use only once per request.",
             "input_schema": {"type": "object", "properties": {"text": {"type": "string"}}, "required": ["text"]}},
            {"name": "notify_gabe", "description": "Push notification to Gabe's phone, one short line.",
             "input_schema": {"type": "object", "properties": {"text": {"type": "string"}}, "required": ["text"]}},
            {"name": "add_todo", "description": "Add an item to the household fix list.",
             "input_schema": {"type": "object", "properties": {
                 "title": {"type": "string"}, "description": {"type": "string"}}, "required": ["title"]}},
        ]

    async def run_tool(self, name, args, req, acts):
        acting = name in ("call_service", "speak", "notify_gabe", "add_todo")
        if name in ("notify_gabe", "add_todo"):
            now = time.time()
            recent = [(t, k, x) for t, k, x in self.reported if k == name and now - t < REPORT_QUIET_S]
            if recent:
                t, k, x = recent[-1]
                return ("Già segnalato %d minuti fa (\"%s\"): non ripetere la stessa segnalazione. "
                        "Se è un problema davvero diverso, scrivilo solo nella riga finale." % ((now - t) // 60, x[:80]))
            self.reported.append((now, name, args.get("text") or args.get("title") or ""))
        if acting and self.shadow:
            acts.append({"would": name, "args": args})
            return "MODALITÀ OMBRA: non eseguito. Continua come se fosse andato a buon fine."
        try:
            if name == "get_states":
                if args.get("entity_ids"):
                    out = [await self.ha.state(e) for e in args["entity_ids"]]
                else:
                    st, allst = await self.ha.rest("GET", "/states")
                    q = (args.get("search") or "").lower()
                    out = [s for s in allst if q in s["entity_id"].lower()
                           or q in str(s["attributes"].get("friendly_name", "")).lower()][:40]
                    out = [{"entity_id": s["entity_id"], "state": s["state"],
                            "name": s["attributes"].get("friendly_name")} for s in out]
                return short(out)
            if name == "get_history":
                start = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=args.get("minutes", 60))).isoformat()
                res = await self.ha.call({"type": "history/history_during_period", "start_time": start,
                                          "entity_ids": args["entity_ids"], "minimal_response": True,
                                          "no_attributes": True})
                return short(res, 3000)
            if name == "get_logbook" and int(args.get("minutes", 10)) <= 10 and not args.get("entity_id"):
                return "Hai già il logbook degli ultimi 10 minuti in contesto_casa: usalo."
            if name == "get_logbook":
                start = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=args.get("minutes", 10))).isoformat()
                q = {"type": "logbook/get_events", "start_time": start}
                if args.get("entity_id"):
                    q["entity_ids"] = [args["entity_id"]]
                res = await self.ha.call(q)
                return short(compact_logbook(res, 80), 3500)
            if name == "get_system_log":
                res = await self.ha.call({"type": "system_log/list"})
                return short([[r.get("level"), r.get("name"), str(r.get("message"))[:160], r.get("count"),
                               hhmmss(r.get("timestamp"))] for r in res[:25]], 3000)
            if name == "get_traces":
                lst = await self.ha.call({"type": "trace/list", "domain": args["domain"], "item_id": args["item_id"]})
                lst = sorted(lst, key=lambda t: t.get("timestamp", {}).get("start", ""))[-args.get("count", 3):]
                out = []
                for t in lst:
                    full = await self.ha.call({"type": "trace/get", "domain": args["domain"],
                                               "item_id": args["item_id"], "run_id": t["run_id"]})
                    out.append({"start": t.get("timestamp"), "state": t.get("state"), "error": t.get("error"),
                                "script_execution": t.get("script_execution"),
                                "trace": short(full.get("trace"), 2500)})
                return short(out, 8000)
            if name == "reconstruct":
                return await self.reconstruct(args["entity_ids"], min(int(args.get("minutes", 180)), 720))
            if name == "get_recent_requests":
                return short([{k: v for k, v in r.items() if not k.startswith("_")} for r in self.history], 8000)
            if name == "call_service":
                if args["domain"] in BLOCKED_DOMAINS:
                    return "Bloccato: il dominio %s non è permesso al guardiano." % args["domain"]
                heard = (req.get("heard") or "").lower()
                asked_volume = any(w in heard for w in ("volume", "alza", "abbassa", "muto", "silenzi", "louder", "quieter", "mute"))
                if args["domain"] == "media_player" and args["service"] in ("volume_set", "volume_mute", "volume_up") and not asked_volume:
                    return "Bloccato: il volume e il muto li decidono le persone. Non cambiarli se non te lo chiedono."
                res = await self.ha.service(args["domain"], args["service"], args.get("data"), args.get("target"))
                acts.append({"did": name, "args": args})
                return short(res or "ok", 1500)
            if name == "speak":
                await self.speak(args["text"], req)
                acts.append({"did": name, "args": args})
                return "detto"
            if name == "notify_gabe":
                svc = self.opts.get("notify_service", "")
                d, s = svc.split(".", 1)
                await self.ha.service(d, s, {"message": args["text"], "title": "Guardiano"})
                acts.append({"did": name, "args": args})
                return "inviato"
            if name == "add_todo":
                todo = self.settings().get("todo_list", "")
                await self.ha.service("todo", "add_item", {"item": args["title"],
                                                         "description": args.get("description", "")},
                                      {"entity_id": todo})
                acts.append({"did": name, "args": args})
                return "aggiunto"
            return "strumento sconosciuto"
        except Exception as e:
            return "errore: %s" % e

    async def user_names(self):
        if not self.users:
            try:
                res = await self.ha.call({"type": "config/auth/list"})
                self.users = {u["id"]: u.get("name") for u in res}
            except Exception:
                self.users = {"_": None}
        return self.users

    async def reconstruct(self, entity_ids, minutes):
        names = await self.user_names()
        start = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=minutes)).isoformat()
        out = {}
        for e in entity_ids[:6]:
            try:
                lb = await self.ha.call({"type": "logbook/get_events", "start_time": start, "entity_ids": [e]})
            except Exception as ex:
                out[e] = "errore %s" % ex
                continue
            rows = []
            for r in lb:
                when = r.get("when")
                if isinstance(when, (int, float)):
                    when = dt.datetime.fromtimestamp(when).astimezone().strftime("%H:%M:%S")
                else:
                    when = str(when)[11:19]
                uid = r.get("context_user_id")
                if uid:
                    cause = "persona: %s" % (names.get(uid) or uid[:8])
                elif r.get("context_entity_id"):
                    cause = "da %s" % r.get("context_entity_id")
                elif r.get("context_event_type"):
                    cause = r.get("context_event_type")
                else:
                    cause = "dispositivo o integrazione"
                rows.append({"ora": when, "stato": r.get("state") or r.get("message"), "causa": cause,
                             "servizio": r.get("context_service") or r.get("context_domain")})
            out[e] = rows[-80:]
        try:
            hist = await self.ha.call({"type": "history/history_during_period", "start_time": start,
                                       "entity_ids": entity_ids[:6], "minimal_response": False,
                                       "significant_changes_only": False})
            vols = {}
            for e, lst in (hist or {}).items():
                v = []
                for x in lst:
                    a = x.get("a") or x.get("attributes") or {}
                    if "volume_level" in a:
                        lu = x.get("lu") or x.get("last_updated")
                        if isinstance(lu, (int, float)):
                            lu = dt.datetime.fromtimestamp(lu).astimezone().strftime("%H:%M:%S")
                        v.append((lu, a["volume_level"], a.get("is_volume_muted")))
                if v:
                    vols[e] = v[-40:]
            if vols:
                out["_volumi"] = vols
        except Exception:
            pass
        return short(out, 5000)

    async def tts_url(self, text, req):
        p = self.pipelines.get(req.get("pipeline_id")) or next(
            (x for x in self.pipelines.values() if "gerv" in (x.get("name") or "").lower()), {})
        body = {"engine_id": p.get("tts_engine"), "message": text, "language": p.get("tts_language"),
                "options": {"voice": p.get("tts_voice"), "preferred_format": "mp3"}}
        st, js = await self.ha.rest("POST", "/tts_get_url", body)
        return js.get("url") if isinstance(js, dict) else None

    async def speak(self, text, req):
      async with self.speak_lock:
          cfg = self.settings()
          sats = cfg.get("satellites", {})
          sat = req.get("satellite_id") or cfg.get("default_satellite")
          sc = sats.get(sat) or sats.get(cfg.get("default_satellite"), {})
          # do not talk over Gervàsia's own reply
          wait = len(req.get("reply") or "") * 0.07 + 1.5 - (time.monotonic() - req["_intent_end_monotonic"])
          if wait > 0:
              await asyncio.sleep(min(wait, 20))
          mutes = cfg.get("mute_switches", [])
          before = {m: ((await self.ha.state(m)) or {}).get("state") for m in mutes}
          for m in mutes:
              await self.ha.service("switch", "turn_on", target={"entity_id": m})
          self.spoken.append((time.time(), text))
          try:
              on_sonos = sc.get("sonos_switch") and ((await self.ha.state(sc["sonos_switch"])) or {}).get("state") == "on"
              if on_sonos and cfg.get("sonos_script"):
                  url = await self.tts_url(text, req)
                  d, s = cfg["sonos_script"].split(".", 1)
                  await self.ha.service(d, s, {"url": url})
              else:
                  try:
                      await self.ha.service("assist_satellite", "announce", {"message": text, "preannounce": False},
                                            {"entity_id": sat})
                  except Exception as e:
                      # some satellites (the BOX-3) do not support announce: play on their media player
                      mp = sc.get("media_player")
                      if not mp:
                          raise
                      log("announce not supported on %s (%s), using %s" % (sat, e, mp))
                      url = await self.tts_url(text, req)
                      await self.ha.service("media_player", "play_media",
                                            {"media_content_id": url, "media_content_type": "music", "announce": True},
                                            {"entity_id": mp})
          finally:
              await asyncio.sleep(len(text) * 0.07 + 2)
              for m in mutes:
                  if before.get(m) == "off":
                      await self.ha.service("switch", "turn_off", target={"entity_id": m})

      # ---------- deciding ----------
    async def prefetch(self):
        """House context handed to the model up front, so it needs fewer tool turns."""
        cfg = self.settings()
        ids = list(cfg.get("context_entities", [])) + list(cfg.get("mute_switches", []))
        for sc in cfg.get("satellites", {}).values():
            if sc.get("sonos_switch"):
                ids.append(sc["sonos_switch"])
        states = {}
        for e in ids:
            st = await self.ha.state(e)
            if st:
                a = st.get("attributes", {})
                states[e] = {"state": st["state"], "changed": st.get("last_changed", "")[11:19],
                             **{k: a[k] for k in ("volume_level", "is_volume_muted", "media_title", "media_artist") if k in a}}
        start = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=10)).isoformat()
        out = {"stati": states}
        try:
            lb = await self.ha.call({"type": "logbook/get_events", "start_time": start})
            out["logbook_10_minuti [ora, entità, stato, causa]"] = compact_logbook(lb, 60)
        except Exception as e:
            out["logbook_10_minuti"] = "errore %s" % e
        try:
            sl = await self.ha.call({"type": "system_log/list"})
            cutoff = time.time() - 3600
            out["system_log_ultima_ora [livello, fonte, messaggio, volte, ora]"] = [
                [r.get("level"), r.get("name"), str(r.get("message"))[:140], r.get("count"), hhmmss(r.get("timestamp"))]
                for r in sl if (r.get("timestamp") or 0) > cutoff][:12]
        except Exception as e:
            out["system_log_ultima_ora"] = "errore %s" % e
        todo = cfg.get("todo_list")
        if todo:
            try:
                res = await self.ha.call({"type": "call_service", "domain": "todo", "service": "get_items",
                                          "target": {"entity_id": todo}, "return_response": True})
                items = (res.get("response") or {}).get(todo, {}).get("items", [])
                out["lista_da_sistemare"] = [i.get("summary") for i in items if i.get("status") != "completed"][:15]
            except Exception as e:
                out["lista_da_sistemare"] = "errore %s" % e
        return out

    async def triage(self, system, payload):
        model = self.opts.get("triage_model", "claude-haiku-5-5")
        if not model:
            return True, False, True, "nessun triage"
        prompt = system + ("\n\nTRIAGE. Do not act now. Decide only whether this request needs the guardian: "
                           "a failure, a useless or 'cannot' answer, a question about the house system, a request "
                           "addressed to you, or a confirmation of a question you asked. When unsure, say yes. "
                           "Set indagine true when answering needs digging into history or logs (why something happened, what went wrong). "
                           "Set azione true when fixing it needs doing something in the house, not just giving an answer. "
                           "Answer with only a JSON object: {\"intervieni\": true or false, \"indagine\": true or false, \"azione\": true or false, \"motivo\": \"few words\"}.")
        resp = await self.claude.messages.create(
            model=model, max_tokens=120,
            system=[{"type": "text", "text": prompt, "cache_control": {"type": "ephemeral"}}],
            messages=[{"role": "user", "content": payload}])
        txt = " ".join(b.text for b in resp.content if b.type == "text")
        try:
            js = json.loads(txt[txt.index("{"):txt.rindex("}") + 1])
            return (bool(js.get("intervieni", True)), bool(js.get("indagine", False)),
                    bool(js.get("azione", True)), str(js.get("motivo", "")))
        except Exception:
            return True, False, True, "triage illeggibile: " + txt[:100]

    async def decide(self, req):
        async with self.lock:
            t0 = time.monotonic()
            steps = []
            acts = []
            final = ""
            visible = {k: v for k, v in req.items() if not k.startswith("_")}
            real_errors = [e for e in req.get("errors") or []
                           if (e or {}).get("code") not in ("stt-no-text-recognized", "duplicate_wake_up_detected")]
            if not (req.get("heard") or "").strip() and not real_errors:
                log("skip: no text and no error")
                return
            system = self.instructions()
            if self.shadow:
                system += ("\n\nMODALITÀ OMBRA ATTIVA: le azioni non vengono eseguite, vengono solo registrate. "
                           "Comportati esattamente come faresti davvero.")
            now = dt.datetime.now().astimezone().strftime("%A %d %B %Y, %H:%M:%S %Z")
            previous = [{k: v for k, v in r.items() if not k.startswith("_")} for r in list(self.history)[:-1]][-10:]
            base = {"ora": now, "richiesta_nuova": visible, "richieste_precedenti": previous,
                    "gia_segnalato_ultimi_30_minuti": [{"minuti_fa": int((time.time() - t) // 60), "tipo": k, "testo": x[:120]}
                                                       for t, k, x in self.reported if time.time() - t < REPORT_QUIET_S],
                    "detto_dal_guardiano": [{"secondi_fa": int(time.time() - t), "testo": x}
                                            for t, x in self.spoken if time.time() - t < 600]}
            triage_s = None
            try:
                ctx_task = asyncio.create_task(self.prefetch())
                go, digging, needs_action, why = await self.triage(system, json.dumps(base, ensure_ascii=False))
                triage_s = round(time.monotonic() - t0, 1)
                steps.append(["triage", triage_s])
                prep = self.preps.pop(req.get("_rid"), None)
                if go and prep and not needs_action:
                    if not prep.done():
                        digging = True   # the prepared answer is not ready yet: say a holding line first
                q = (req.get("heard") or "").lower()
                if any(w in q for w in ("perché", "perche", "come mai", "cosa è successo", "cos'è successo",
                                         "che è successo", "log", "registro", "why", "what happened")):
                    digging = True
                if go and digging:
                    hold = random.choice(HOLD_LINES)
                    if self.shadow:
                        acts.append({"would": "speak", "args": {"text": hold}, "note": "frase d'attesa"})
                    else:
                        asyncio.create_task(self.speak(hold, req))
                        acts.append({"did": "speak", "args": {"text": hold}, "note": "frase d'attesa"})
                    base["frase_di_attesa_gia_detta"] = hold
                ready = None
                if go and prep and not needs_action:
                    tw = time.monotonic()
                    try:
                        ready = await asyncio.wait_for(prep, 25)
                    except Exception:
                        ready = None
                    steps.append(["attesa bozza", round(time.monotonic() - tw, 1)])
                elif prep:
                    prep.cancel()
                if ready and ready.get("draft"):
                    ctx_task.cancel()
                    go = False
                    draft = ready["draft"]
                    steps.append(["bozza pronta dopo", ready.get("secs")])
                    if self.shadow:
                        acts.append({"would": "speak", "args": {"text": draft}, "note": "risposta preparata"})
                    else:
                        await self.speak(draft, req)
                        acts.append({"did": "speak", "args": {"text": draft}, "note": "risposta preparata"})
                    final = "risposta preparata in anticipo (%s)" % why
                elif not go:
                    ctx_task.cancel()
                    final = "triage: nessun intervento (%s)" % why
                else:
                    tc = time.monotonic()
                    base["contesto_casa"] = await ctx_task
                    steps.append(["attesa contesto", round(time.monotonic() - tc, 1)])
            except Exception as e:
                go = True
                log("triage/prefetch error", e)
            messages = [{"role": "user", "content": json.dumps(base, ensure_ascii=False, default=str)}]
            try:
                for _ in range(MAX_TOOL_TURNS if go else 0):
                    for m in messages:
                        if isinstance(m["content"], list):
                            for b in m["content"]:
                                if isinstance(b, dict):
                                    b.pop("cache_control", None)
                    last = messages[-1]
                    if isinstance(last["content"], str):
                        last["content"] = [{"type": "text", "text": last["content"]}]
                    if isinstance(last["content"][-1], dict):
                        last["content"][-1]["cache_control"] = {"type": "ephemeral"}
                    tm = time.monotonic()
                    resp = await self.claude.messages.create(
                        model=self.opts.get("model", "claude-sonnet-5-5"), max_tokens=1500,
                        system=[{"type": "text", "text": system, "cache_control": {"type": "ephemeral"}}],
                        tools=self.tool_specs(), messages=messages)
                    steps.append(["modello", round(time.monotonic() - tm, 1), resp.usage.input_tokens,
                                  getattr(resp.usage, "cache_read_input_tokens", 0), resp.usage.output_tokens])
                    messages.append({"role": "assistant", "content": resp.content})
                    uses = [b for b in resp.content if b.type == "tool_use"]
                    final = " ".join(b.text for b in resp.content if b.type == "text").strip()
                    if not uses:
                        break
                    results = []
                    for u in uses:
                        tt = time.monotonic()
                        out = await self.run_tool(u.name, u.input or {}, req, acts)
                        steps.append(["tool " + u.name, round(time.monotonic() - tt, 1), len(out)])
                        results.append({"type": "tool_result", "tool_use_id": u.id, "content": out})
                    messages.append({"role": "user", "content": results})
            except Exception as e:
                final = "errore del guardiano: %s" % e
                traceback.print_exc()
            secs = round(time.monotonic() - t0, 1)
            entry = {"at": dt.datetime.now().astimezone().isoformat(timespec="seconds"), "secs": secs,
                     "shadow": self.shadow, "triage_s": triage_s, "steps": steps, "request": visible, "actions": acts, "verdict": final}
            log("steps", json.dumps(steps))
            log("decision %.1fs (triage %ss) heard=%r actions=%s verdict=%s" % (secs, triage_s, req.get("heard"), acts, final[:200]))
            try:
                os.makedirs(os.path.dirname(self.opts["log_file"]), exist_ok=True)
                with open(self.opts["log_file"], "a", encoding="utf-8") as f:
                    f.write(json.dumps(entry, ensure_ascii=False, default=str) + "\n")
            except Exception as e:
                log("log write failed", e)
            label = ("OMBRA " if self.shadow else "") + ("interviene" if acts else "nessun intervento")
            await self.ha.rest("POST", "/states/sensor.guardiano_ultima_decisione", {
                "state": label, "attributes": {
                    "friendly_name": "Guardiano: ultima decisione", "sentito": req.get("heard"),
                    "risposta_gervasia": req.get("reply"), "azioni": short(acts, 1500),
                    "giudizio": final[:1500], "secondi": secs}})


async def main():
    opts = load_options()
    if not opts.get("anthropic_api_key"):
        log("anthropic_api_key is empty: set it in the add-on configuration")
        while True:
            await asyncio.sleep(3600)
    while True:
        try:
            async with aiohttp.ClientSession() as s:
                ha = HA(s)
                await ha.connect()
                await Guardian(ha, opts).watch()
        except Exception as e:
            log("restarting after error:", e)
            traceback.print_exc()
            await asyncio.sleep(10)


if __name__ == "__main__":
    asyncio.run(main())
