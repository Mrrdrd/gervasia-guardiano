"""Chi parla: a Wyoming speech-to-text proxy for Gervàsia.

Home Assistant sends the audio of each voice request here (Wyoming protocol).
The audio is streamed on to Home Assistant's own speech-to-text (Home Assistant
Cloud) exactly as before, and at the same time a CAM++ speaker model compares
the voice with the family's voiceprints. Before the transcript goes back, the
result is written to input_text.chi_parla as "Name|alta|<unix time>" (or
"sconosciuto|bassa|<time>"), so Gervàsia's prompt and the guardian can read who
is speaking for this very request.

Safety: speaker recognition can never block speech to text. It runs with a
time limit, and any error there only leaves chi_parla unchanged.
Voiceprints live on the HP in /share/chi_parla/voiceprints.json, never in git.
"""
import asyncio
import json
import logging
import os
import time
from functools import partial

import aiohttp
import numpy as np
import sherpa_onnx
from wyoming.asr import Transcribe, Transcript
from wyoming.audio import AudioChunk, AudioStart, AudioStop
from wyoming.event import Event
from wyoming.info import AsrModel, AsrProgram, Attribution, Describe, Info
from wyoming.server import AsyncEventHandler, AsyncServer

OPT = json.load(open("/data/options.json")) if os.path.exists("/data/options.json") else {}
STT_ENTITY = OPT.get("stt_entity", "stt.home_assistant_cloud")
HELPER = OPT.get("helper", "input_text.chi_parla")
VOICEPRINTS = OPT.get("voiceprints", "/share/chi_parla/voiceprints.json")
LOG_FILE = OPT.get("log_file", "/share/chi_parla/log.jsonl")
MIN_SCORE = float(OPT.get("min_score", 0.45))
MIN_MARGIN = float(OPT.get("min_margin", 0.08))
MODEL = os.environ.get("CHI_MODEL", "/app/campplus.onnx")
API = os.environ.get("CHI_API", "http://supervisor/core/api")
TOKEN = os.environ.get("SUPERVISOR_TOKEN", "")
HDR = {"Authorization": f"Bearer {TOKEN}"}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("chi_parla")

extractor = sherpa_onnx.SpeakerEmbeddingExtractor(
    sherpa_onnx.SpeakerEmbeddingExtractorConfig(model=MODEL, num_threads=2)
)
PRINTS = {}


ENROLL_ZIPS = [OPT.get("enroll_zip", "/share/chi_parla/registrazioni.zip"),
               "/homeassistant/www/ehi_gervasia_68433373cdb9.zip"]


def enroll():
    """Build voiceprints from a zip of labeled clips named <person>_<anything>.wav."""
    import io
    import wave
    import zipfile
    src = next((z for z in ENROLL_ZIPS if os.path.exists(z)), None)
    if not src:
        return
    acc = {}
    with zipfile.ZipFile(src) as z:
        for n in z.namelist():
            if not n.lower().endswith(".wav"):
                continue
            who = os.path.basename(n).split("_")[0].lower()
            w = wave.open(io.BytesIO(z.read(n)))
            pcm = np.frombuffer(w.readframes(w.getnframes()), dtype=np.int16).astype(np.float32) / 32768.0
            if w.getnchannels() > 1:
                pcm = pcm.reshape(-1, w.getnchannels()).mean(1)
            s = extractor.create_stream()
            s.accept_waveform(w.getframerate(), pcm)
            s.input_finished()
            e = np.array(extractor.compute(s), dtype=np.float32)
            acc.setdefault(who, []).append(e / np.linalg.norm(e))
    prints = {k: (np.mean(v, 0) / np.linalg.norm(np.mean(v, 0))).round(5).tolist() for k, v in acc.items()}
    os.makedirs(os.path.dirname(VOICEPRINTS), exist_ok=True)
    json.dump(prints, open(VOICEPRINTS, "w"))
    log.info("enrolled from %s: %s", src, {k: len(v) for k, v in acc.items()})


def load_prints():
    global PRINTS
    if not os.path.exists(VOICEPRINTS):
        try:
            enroll()
        except Exception as e:
            log.warning("enrollment failed: %s", e)
    try:
        raw = json.load(open(VOICEPRINTS))
        PRINTS = {k: np.array(v, dtype=np.float32) / np.linalg.norm(v) for k, v in raw.items()}
        log.info("voiceprints: %s", ", ".join(PRINTS))
    except Exception as e:  # no voiceprints yet: transcripts still work
        PRINTS = {}
        log.warning("no voiceprints (%s)", e)


def identify(pcm: bytes, rate: int):
    """Return (name, score, margin) for 16-bit mono PCM."""
    if not PRINTS:
        return "sconosciuto", 0.0, 0.0
    w = np.frombuffer(pcm, dtype=np.int16).astype(np.float32) / 32768.0
    if len(w) < rate * 0.8:  # under 0.8 s of audio: too short to judge
        return "sconosciuto", 0.0, 0.0
    s = extractor.create_stream()
    s.accept_waveform(rate, w)
    s.input_finished()
    e = np.array(extractor.compute(s), dtype=np.float32)
    e /= np.linalg.norm(e)
    sc = sorted(((float(e @ v), k) for k, v in PRINTS.items()), reverse=True)
    best, second = sc[0], (sc[1] if len(sc) > 1 else (0.0, ""))
    margin = best[0] - second[0]
    name = best[1] if best[0] >= MIN_SCORE and margin >= MIN_MARGIN else "sconosciuto"
    return name, best[0], margin


def lang(code: str) -> str:
    code = (code or "it-IT").replace("_", "-")
    return {"it": "it-IT", "en": "en-US"}.get(code, code)


class Handler(AsyncEventHandler):
    def __init__(self, *args, session=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.session = session
        self.language = "it-IT"
        self.reset()

    def reset(self):
        self.q = None
        self.task = None
        self.pcm = bytearray()
        self.rate = 16000
        self.t0 = 0.0

    async def _stream(self):
        while True:
            c = await self.q.get()
            if c is None:
                return
            yield c

    async def _stt(self, rate, width, channels):
        h = dict(HDR)
        h["X-Speech-Content"] = (
            f"format=wav; codec=pcm; sample_rate={rate}; bit_rate={width * 8}; "
            f"channel={channels}; language={self.language}"
        )
        async with self.session.post(f"{API}/stt/{STT_ENTITY}", headers=h, data=self._stream(),
                                     timeout=aiohttp.ClientTimeout(total=30)) as r:
            j = await r.json(content_type=None)
            if j.get("result") != "success":
                log.warning("stt result %s", j)
            return j.get("text") or ""

    async def handle_event(self, event: Event) -> bool:
        if Describe.is_type(event.type):
            await self.write_event(INFO.event())
            return True
        if Transcribe.is_type(event.type):
            self.language = lang(Transcribe.from_event(event).language)
            return True
        if AudioStart.is_type(event.type):
            a = AudioStart.from_event(event)
            self.reset()
            self.rate, self.t0 = a.rate, time.time()
            self.q = asyncio.Queue()
            self.task = asyncio.create_task(self._stt(a.rate, a.width, a.channels))
            return True
        if AudioChunk.is_type(event.type):
            c = AudioChunk.from_event(event)
            if self.q is None:  # chunk without start: open the stream now
                self.q = asyncio.Queue()
                self.rate, self.t0 = c.rate, time.time()
                self.task = asyncio.create_task(self._stt(c.rate, c.width, c.channels))
            self.q.put_nowait(c.audio)
            self.pcm.extend(c.audio)
            return True
        if AudioStop.is_type(event.type):
            if self.q is None:
                await self.write_event(Transcript(text="").event())
                return False
            self.q.put_nowait(None)
            loop = asyncio.get_running_loop()
            who = loop.run_in_executor(None, partial(identify, bytes(self.pcm), self.rate))
            text = ""
            try:
                text = await self.task
            except Exception as e:
                log.error("stt failed: %s", e)
            name, score, margin = "sconosciuto", 0.0, 0.0
            try:
                name, score, margin = await asyncio.wait_for(who, timeout=1.5)
                conf = "alta" if name != "sconosciuto" else "bassa"
                await self.session.post(f"{API}/services/input_text/set_value", headers=HDR,
                                        json={"entity_id": HELPER,
                                              "value": f"{name.capitalize()}|{conf}|{int(time.time())}"},
                                        timeout=aiohttp.ClientTimeout(total=2))
            except Exception as e:
                log.warning("speaker id skipped: %s", e)
            secs = len(self.pcm) / 2 / max(self.rate, 1)
            log.info("%.1fs audio, %.2fs total: %s (%.2f, +%.2f) | %s", secs, time.time() - self.t0,
                     name, score, margin, text[:60])
            try:
                with open(LOG_FILE, "a") as f:
                    f.write(json.dumps({"t": int(time.time()), "chi": name, "score": round(score, 3),
                                        "margine": round(margin, 3), "secondi": round(secs, 1),
                                        "testo": text}, ensure_ascii=False) + "\n")
            except Exception:
                pass
            await self.write_event(Transcript(text=text).event())
            self.reset()
            return False
        return True


INFO = Info(asr=[AsrProgram(
    name="chi_parla",
    attribution=Attribution(name="Home Assistant Cloud + 3D-Speaker CAM++", url="https://github.com/modelscope/3D-Speaker"),
    installed=True,
    description="Home Assistant Cloud speech to text that also recognises who is speaking",
    version="1.0.0",
    models=[AsrModel(name="cloud+campplus", attribution=Attribution(name="3D-Speaker", url="https://github.com/modelscope/3D-Speaker"),
                     installed=True, description="cloud transcript, local speaker id", version="1.0.0",
                     languages=["it-IT", "it", "en-US", "en"])],
)])


async def main():
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    load_prints()
    async with aiohttp.ClientSession() as session:
        server = AsyncServer.from_uri("tcp://0.0.0.0:10400")
        log.info("listening on 10400, stt %s", STT_ENTITY)
        await server.run(partial(Handler, session=session))


if __name__ == "__main__":
    asyncio.run(main())
