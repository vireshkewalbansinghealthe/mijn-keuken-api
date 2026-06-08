"""
Bruynzeel Mijn Keuken — Backend Proxy
Bridges the DKG MCP catalog server and the Flutter app.

Flutter sends: POST /generate  → returns concept name + catalog config + Gemini kitchen image
Flutter sends: GET  /health    → liveness check

MCP findings (confirmed via test_deep.py / test_kitchen_build.py):
  - NO images in catalog responses — only structured data (dims, features, option_keys)
  - lookup_option works: "knop" → K012/K026/K028, "marmer" → MW05, "eiken" → EB/AE2/etc.
  - search_articles: returns dimensions_mm with h/b/t (nominal, from, to, step, variable)
  - validate_kitchen: returns kitchen_valid bool + per-article violations list
  - Strategy: use MCP metadata to build a rich, dimension-accurate Gemini image prompt
"""

import asyncio
import json
import logging
import os
import ssl
import socket
import threading
import uuid
from contextlib import asynccontextmanager
from queue import Queue, Empty
from typing import Optional

import base64

import httpx
from fastapi import FastAPI, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from reference_images import pick_reference_image

# ── Config ────────────────────────────────────────────────────────────────────

MCP_HOST    = os.getenv("MCP_HOST", "dkg-dev-dockerswarm-app-ingress.azurewebsites.net")
MCP_BASE    = f"https://{MCP_HOST}/idm-mcp"
MCP_API_KEY = os.getenv("MCP_API_KEY", "D0AF34E63F344B569DA861AF8D326E3F")
GEMINI_KEY  = os.getenv("GEMINI_API_KEY", "AIzaSyCiy4UN2JL9D_UN8mIE-8MwpPYvjH40RKc")
APP_API_KEY = os.getenv("APP_API_KEY", "mijn-keuken-secret")

GEMINI_TEXT_URL  = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent"
GEMINI_IMAGE_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-3-pro-image:generateContent"

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("mijn-keuken-api")

# ── MCP Session ───────────────────────────────────────────────────────────────

class McpSession:
    def __init__(self):
        self._sock: Optional[ssl.SSLSocket] = None
        self._ep: Optional[str] = None
        self._q: Queue = Queue()
        self._mid = 0
        self._lock = threading.Lock()
        self._reader_thread: Optional[threading.Thread] = None

    def _decode_chunked(self, data: bytes) -> bytes:
        out = b""
        while data:
            crlf = data.find(b"\r\n")
            if crlf == -1:
                break
            try:
                size = int(data[:crlf], 16)
            except ValueError:
                break
            if size == 0:
                break
            out += data[crlf + 2: crlf + 2 + size]
            data = data[crlf + 2 + size + 2:]
        return out

    def _parse_events(self, raw: str) -> list[dict]:
        events = []
        for block in raw.split("\n\n"):
            ev = {}
            for line in block.strip().splitlines():
                if line.startswith("event:"):
                    ev["event"] = line[6:].strip()
                elif line.startswith("data:"):
                    ev["data"] = line[5:].strip()
            if ev:
                events.append(ev)
        return events

    def connect(self) -> bool:
        try:
            ctx = ssl.create_default_context()
            self._sock = ctx.wrap_socket(
                socket.create_connection((MCP_HOST, 443), timeout=15),
                server_hostname=MCP_HOST,
            )
            self._sock.sendall((
                f"GET /idm-mcp/sse HTTP/1.1\r\nHost: {MCP_HOST}\r\n"
                f"X-Api-Key: {MCP_API_KEY}\r\n"
                f"Accept: text/event-stream\r\nCache-Control: no-cache\r\n"
                f"Connection: keep-alive\r\n\r\n"
            ).encode())

            buf = b""
            while b"\r\n\r\n" not in buf:
                buf += self._sock.recv(1)
            _, body = buf.split(b"\r\n\r\n", 1)

            self._sock.settimeout(8)
            try:
                while True:
                    body += self._sock.recv(4096)
                    evs = self._parse_events(self._decode_chunked(body).decode("utf-8", "ignore"))
                    ep = next((e for e in evs if e.get("event") == "endpoint"), None)
                    if ep:
                        break
            except socket.timeout:
                pass

            if not ep:
                log.error("No endpoint event from SSE")
                return False

            raw_path = ep["data"].strip()
            self._ep = raw_path if raw_path.startswith("/idm-mcp") else "/idm-mcp" + raw_path
            log.info(f"MCP session: {self._ep}")

            self._reader_thread = threading.Thread(target=self._read_loop, daemon=True)
            self._reader_thread.start()

            self._rpc("initialize", {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "mijn-keuken-backend", "version": "1.0"},
            })
            self._post_raw({"jsonrpc": "2.0", "method": "notifications/initialized"})
            log.info("MCP initialized ✅")
            return True

        except Exception as e:
            log.error(f"MCP connect failed: {e}")
            return False

    def _read_loop(self):
        self._sock.settimeout(60)
        partial = b""
        try:
            while True:
                chunk = self._sock.recv(4096)
                if not chunk:
                    break
                partial += chunk
                for ev in self._parse_events(self._decode_chunked(partial).decode("utf-8", "ignore")):
                    if ev.get("event") == "message":
                        self._q.put(ev)
        except Exception as e:
            log.warning(f"SSE reader stopped: {e}")

    def _post_raw(self, payload: dict):
        body = json.dumps(payload).encode()
        ctx = ssl.create_default_context()
        s = ctx.wrap_socket(
            socket.create_connection((MCP_HOST, 443), timeout=10),
            server_hostname=MCP_HOST,
        )
        req = (
            f"POST {self._ep} HTTP/1.1\r\nHost: {MCP_HOST}\r\n"
            f"X-Api-Key: {MCP_API_KEY}\r\nContent-Type: application/json\r\n"
            f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n"
        ).encode() + body
        s.sendall(req)
        buf = b""
        s.settimeout(5)
        try:
            while True:
                c = s.recv(4096)
                if not c:
                    break
                buf += c
        except socket.timeout:
            pass
        s.close()

    def _rpc(self, method: str, params: dict = None, timeout: int = 30) -> dict:
        with self._lock:
            self._mid += 1
            mid = self._mid

        msg = {"jsonrpc": "2.0", "method": method, "id": mid}
        if params:
            msg["params"] = params
        self._post_raw(msg)

        import time
        end = time.time() + timeout
        while time.time() < end:
            try:
                ev = self._q.get(timeout=0.5)
                try:
                    d = json.loads(ev["data"])
                    if d.get("id") == mid:
                        return d.get("result") or d.get("error") or {}
                except json.JSONDecodeError:
                    # Try partial recovery for large truncated responses
                    raw = ev["data"]
                    last = raw.rfind("}")
                    if last > 0:
                        try:
                            d = json.loads(raw[:last + 1])
                            if d.get("id") == mid:
                                return d.get("result") or {}
                        except Exception:
                            pass
            except Empty:
                pass
        return {}

    def call_tool(self, name: str, args: dict, timeout: int = 30) -> list[dict]:
        raw = self._rpc("tools/call", {"name": name, "arguments": args}, timeout=timeout)
        content = raw.get("content", [])
        results = []
        for c in content:
            if c.get("type") != "text":
                continue
            text = c["text"]
            try:
                parsed = json.loads(text)
                if isinstance(parsed, list):
                    results.extend(parsed)
                else:
                    results.append(parsed)
            except json.JSONDecodeError:
                # Recover partial JSON array from truncated chunked response
                last_close = text.rfind("}")
                if last_close > 0:
                    try:
                        partial = text[:last_close + 1]
                        if not partial.strip().startswith("["):
                            partial = "[" + partial + "]"
                        results.extend(json.loads(partial))
                    except Exception:
                        log.warning(f"Could not parse response for {name}")
        return results

    def ensure_connected(self):
        if self._ep is None or (self._reader_thread and not self._reader_thread.is_alive()):
            log.info("Reconnecting MCP...")
            self.connect()


mcp = McpSession()


@asynccontextmanager
async def lifespan(app: FastAPI):
    mcp.connect()
    yield


app = FastAPI(title="Mijn Keuken API", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ── Models ────────────────────────────────────────────────────────────────────

class GenerateRequest(BaseModel):
    prompt: str
    onboarding: list[dict] = []


class ArticleResult(BaseModel):
    type_no: str
    name: str
    dimensions_mm: dict = {}
    chosen_options: dict = {}


class GenerateResponse(BaseModel):
    project_id: str
    concept_name: str
    description: str
    style_tags: list[str] = []
    articles: list[ArticleResult] = []
    image_base64: Optional[str] = None
    catalog_valid: bool = False
    gemini_prompt: str = ""


# ── Helpers ───────────────────────────────────────────────────────────────────

async def gemini_text(prompt: str) -> dict:
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(
            f"{GEMINI_TEXT_URL}?key={GEMINI_KEY}",
            json={
                "contents": [{"role": "user", "parts": [{"text": prompt}]}],
                "generationConfig": {"responseMimeType": "application/json"},
            },
        )
        r.raise_for_status()
        text = r.json()["candidates"][0]["content"]["parts"][0]["text"]
        return json.loads(text)


async def fetch_reference_image(url: str) -> Optional[str]:
    """Download a Bruynzeel reference kitchen photo and return base64 JPEG."""
    async with httpx.AsyncClient(timeout=15, headers={"User-Agent": "Mozilla/5.0"}) as client:
        r = await client.get(url)
        if r.status_code == 200:
            return base64.b64encode(r.content).decode()
    return None


async def gemini_image(prompt: str, reference_b64: Optional[str] = None) -> Optional[str]:
    """
    Generate a kitchen image using Gemini.
    If reference_b64 is provided, it is passed as a style reference so the
    output actually looks like a Bruynzeel kitchen, not a generic AI kitchen.
    """
    parts = []
    if reference_b64:
        parts.append({
            "inlineData": {
                "mimeType": "image/jpeg",
                "data": reference_b64,
            }
        })
        parts.append({"text": (
            "This is a real Bruynzeel Keukens kitchen photograph. "
            "Use it as a style and layout reference. "
            "Generate a new photorealistic kitchen image in the same Bruynzeel style "
            "but with the following specific changes:\n\n" + prompt
        )})
    else:
        parts.append({"text": prompt})

    async with httpx.AsyncClient(timeout=90) as client:
        r = await client.post(
            f"{GEMINI_IMAGE_URL}?key={GEMINI_KEY}",
            json={
                "contents": [{"parts": parts}],
                "generationConfig": {"responseModalities": ["TEXT", "IMAGE"]},
            },
        )
        if r.status_code != 200:
            log.warning(f"Gemini image failed: {r.status_code} {r.text[:200]}")
            return None
        resp_parts = r.json()["candidates"][0]["content"]["parts"]
        for p in resp_parts:
            if "inlineData" in p:
                return p["inlineData"]["data"]
    return None


def run_blocking(fn, *args):
    loop = asyncio.get_event_loop()
    return loop.run_in_executor(None, fn, *args)


def _mcp_call(name, args):
    return mcp.call_tool(name, args)


def _build_image_prompt(concept: dict, catalog_items: list[dict]) -> str:
    """
    Build a detailed Gemini image prompt from catalog metadata.
    MCP returns no images — we use dimensions + model names + option names
    to drive photorealistic generation.
    """
    style       = concept.get("style", "modern warm")
    color_desc  = concept.get("color_description", "warm natural wood tones")
    worktop     = concept.get("worktop_description", "white marble")
    handle      = concept.get("handle_description", "knob handles")
    has_island  = concept.get("has_island", False)
    extra_desc  = concept.get("extra_description", "")

    dim_parts = []
    model_parts = []
    for item in catalog_items[:3]:
        dims = item.get("dimensions_mm", {})
        if dims:
            b = dims.get("b", {})
            h = dims.get("h", {})
            t = dims.get("t", {})
            w = b.get("nominal", b.get("from", "")) if isinstance(b, dict) else b
            ht = h.get("nominal", h.get("from", "")) if isinstance(h, dict) else h
            d = t.get("nominal", t.get("from", "")) if isinstance(t, dict) else t
            if w: dim_parts.append(f"{w}mm wide cabinet")
        model_name = item.get("model_name", "")
        if model_name:
            model_parts.append(model_name)

    dim_text   = ", ".join(dim_parts) if dim_parts else "standard 600mm cabinets"
    model_text = f"Cabinet model: {', '.join(set(model_parts))}." if model_parts else ""

    return f"""Photorealistic interior design photograph of a Bruynzeel kitchen.
Professional architecture photography, magazine quality, natural daylight through large windows, no people, no text.
Landscape orientation, wide establishing shot showing full kitchen.

Kitchen design brief:
- Style: {style}
- Cabinet finish: {color_desc}
- Worktop: {worktop}, thick 30mm edge profile
- Handles: {handle}
- {model_text}
- Cabinet dimensions: {dim_text}
{"- Kitchen island with seating" if has_island else "- Straight run or L-shape kitchen layout"}
{f"- {extra_desc}" if extra_desc else ""}
- Floor: light herringbone oak parquet
- Wall tiles: off-white metro tiles behind hob area
- Lighting: warm pendant lights, under-cabinet LED strips
- Atmosphere: inviting, magazine-quality Dutch interior design

Bruynzeel Keukens signature style: clean lines, quality materials, timeless Dutch craftsmanship."""


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "mcp_connected": mcp._ep is not None}


@app.post("/generate", response_model=GenerateResponse)
async def generate(req: GenerateRequest, x_api_key: str = Header(default="")):
    if APP_API_KEY and x_api_key != APP_API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")

    mcp.ensure_connected()

    # ── 1. Gemini: extract structured intent from the prompt ──────────────────
    intent_prompt = f"""Je bent een Bruynzeel keuken configurator.
Analyseer de keuken beschrijving en geef ALLEEN een JSON object terug (geen markdown):
{{
  "concept_name": "Korte Nederlandse naam voor dit concept",
  "style": "one of: modern, klassiek, landelijk, industrieel, scandinavisch",
  "color_description": "beschrijving van kastkleur/fineer in het Engels",
  "worktop_description": "beschrijving van het aanrechtblad in het Engels",
  "handle_description": "beschrijving van de grepen in het Engels",
  "has_island": false,
  "search_terms": ["NL zoekterm1", "NL zoekterm2"],
  "lookup_terms": ["NL term voor greep/kleur/materiaal lookup"],
  "style_tags": ["tag1", "tag2", "tag3"],
  "extra_description": "extra details voor beeldgeneratie in het Engels"
}}

Beschrijving: "{req.prompt}"
Onboarding: {json.dumps(req.onboarding, ensure_ascii=False)}"""

    try:
        intent = await gemini_text(intent_prompt)
    except Exception as e:
        log.warning(f"Gemini intent failed: {e}")
        intent = {
            "concept_name": "Mijn Droomkeuken",
            "style": "modern",
            "color_description": "warm natural wood",
            "worktop_description": "white marble",
            "handle_description": "knob handles",
            "has_island": False,
            "search_terms": ["keuken"],
            "lookup_terms": ["knop"],
            "style_tags": [],
            "extra_description": req.prompt,
        }

    log.info(f"Intent: {intent.get('concept_name')} / {intent.get('style')}")

    # ── 2. MCP: lookup option keys for material/style terms ───────────────────
    option_map: dict[str, list] = {}
    for term in intent.get("lookup_terms", [])[:4]:
        try:
            opts = await run_blocking(_mcp_call, "lookup_option", {"query": term, "language": "NL"})
            if opts:
                option_map[term] = opts
        except Exception as e:
            log.warning(f"lookup_option({term}): {e}")

    # ── 3. MCP: search for relevant articles ─────────────────────────────────
    raw_articles: list[dict] = []
    try:
        raw_articles = await run_blocking(_mcp_call, "search_articles", {"width_mm": 600, "limit": 6})
    except Exception as e:
        log.warning(f"search_articles: {e}")

    # ── 4. Build chosen options per article from lookup results ───────────────
    catalog_items: list[dict] = []
    kitchen_config: list[dict] = []

    for art in raw_articles[:4]:
        type_no = art.get("type_no", "")
        if not type_no:
            continue

        chosen_options: dict[str, str] = {}

        # Map lookup results to feature_no → option_key
        for term, opts in option_map.items():
            for opt in opts[:1]:
                fn = str(opt.get("feature_no", ""))
                ok = opt.get("option_key", "")
                if fn and ok and fn not in chosen_options:
                    chosen_options[fn] = ok

        catalog_items.append({
            "type_no": type_no,
            "name": art.get("name", ""),
            "dimensions_mm": art.get("dimensions_mm", {}),
            "chosen_options": chosen_options,
            "model_name": next(
                (o["name"] for opts in option_map.values() for o in opts if o.get("feature_no") == 1),
                ""
            ),
        })
        kitchen_config.append({"type_no": type_no, "options": chosen_options})

    # ── 5. MCP: validate the kitchen config ───────────────────────────────────
    catalog_valid = False
    if kitchen_config:
        try:
            val_result = await run_blocking(
                _mcp_call, "validate_kitchen", {"kitchen_json": json.dumps(kitchen_config)}
            )
            catalog_valid = val_result[0].get("kitchen_valid", False) if val_result else False
            violations = [
                f"{r['type_no']}: {'; '.join(r['violations'])}"
                for r in (val_result[0].get("results", []) if val_result else [])
                if r.get("violations")
            ]
            if violations:
                log.warning(f"Violations: {violations}")
        except Exception as e:
            log.warning(f"validate_kitchen: {e}")

    # ── 6. Pick reference image based on chosen MCP options ──────────────────
    # Derive model_key and color_key from the lookup results
    model_key = next(
        (o["option_key"] for opts in option_map.values() for o in opts if o.get("feature_no") == 1),
        ""
    )
    color_key = next(
        (o["option_key"] for opts in option_map.values() for o in opts if o.get("feature_no") in (100, 101)),
        ""
    )
    ref_url = pick_reference_image(
        model_key=model_key,
        color_key=color_key,
        style_tags=intent.get("style_tags", []) + [intent.get("style", "")],
    )
    log.info(f"Reference image: {ref_url.split('/')[-1]}")
    reference_b64 = await fetch_reference_image(ref_url)
    if not reference_b64:
        log.warning("Could not fetch reference image, generating without reference")

    # ── 7. Build Gemini image prompt from catalog metadata ────────────────────
    img_prompt = _build_image_prompt(intent, catalog_items)
    log.info(f"Generating image (prompt {len(img_prompt)} chars, ref={'yes' if reference_b64 else 'no'})...")

    # ── 8. Generate kitchen image with reference ───────────────────────────────
    image_b64 = await gemini_image(img_prompt, reference_b64=reference_b64)

    return GenerateResponse(
        project_id=str(uuid.uuid4()),
        concept_name=intent.get("concept_name", "Mijn Droomkeuken"),
        description=req.prompt,
        style_tags=intent.get("style_tags", []),
        articles=[
            ArticleResult(
                type_no=item["type_no"],
                name=item["name"],
                dimensions_mm=item["dimensions_mm"],
                chosen_options=item["chosen_options"],
            )
            for item in catalog_items
        ],
        image_base64=image_b64,
        catalog_valid=catalog_valid,
        gemini_prompt=img_prompt,
    )
