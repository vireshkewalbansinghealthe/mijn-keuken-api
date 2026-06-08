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

import httpx
from fastapi import FastAPI, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ── Config ────────────────────────────────────────────────────────────────────

MCP_HOST    = os.getenv("MCP_HOST", "dkg-dev-dockerswarm-app-ingress.azurewebsites.net")
MCP_BASE    = f"https://{MCP_HOST}/idm-mcp"
MCP_API_KEY = os.getenv("MCP_API_KEY", "D0AF34E63F344B569DA861AF8D326E3F")
GEMINI_KEY  = os.getenv("GEMINI_API_KEY", "")
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
    price_eur: Optional[int] = None


class GenerateResponse(BaseModel):
    project_id: str
    concept_name: str
    description: str
    style_tags: list[str] = []
    articles: list[ArticleResult] = []
    image_base64: Optional[str] = None
    catalog_valid: bool = False
    gemini_prompt: str = ""


class AdjustRequest(BaseModel):
    project_id: str
    original_prompt: str
    adjustment: str
    current_image_base64: Optional[str] = None
    onboarding: list[dict] = []


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


async def gemini_image(prompt: str, reference_b64: Optional[str] = None) -> Optional[str]:
    parts = []
    if reference_b64:
        parts.append({
            "inlineData": {"mimeType": "image/jpeg", "data": reference_b64}
        })
        parts.append({"text": f"Use this kitchen as reference and apply the following changes. Keep the same camera angle, lighting, and layout.\n\n{prompt}"})
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
        parts_resp = r.json()["candidates"][0]["content"]["parts"]
        for p in parts_resp:
            if "inlineData" in p:
                return p["inlineData"]["data"]
    return None


def run_blocking(fn, *args):
    loop = asyncio.get_event_loop()
    return loop.run_in_executor(None, fn, *args)


def _estimate_price(article: dict) -> Optional[int]:
    """Rough indicative price per article based on dimensions."""
    dims = article.get("dimensions_mm", {})
    b = dims.get("b", {})
    width = b.get("nominal", b.get("from", 0)) if isinstance(b, dict) else (b or 0)
    try:
        width = int(width)
    except (TypeError, ValueError):
        width = 600
    if width <= 0:
        width = 600
    # Base: €700 per linear 300mm, scaled
    base = max(400, int((width / 300) * 700))
    return base


def _mcp_call(name, args):
    return mcp.call_tool(name, args)


# ── Sacred sequence implementation ───────────────────────────────────────────

def _sacred_sequence(lookup_terms: list[str], target_width: int = 600) -> dict:
    """
    Implements the full documented MCP call sequence:

    1. lookup_option(term)          → option_key per feature
    2. search_articles(width_mm)    → candidate type_nos
    3. get_article_detail(type_no)  → all configurable features
    4. get_valid_options(type_no, feature_no, selection_json)  → forward-check each step
    5. validate_kitchen([...])      → confirm config is legal

    Returns a dict with:
      - articles: list of found article dicts (with type_no, name, dims)
      - selection: {type_no: {feature_no: option_key}} — valid final config
      - model_key: chosen model option_key (feature 1)
      - color_key: chosen color option_key (feature 100/101)
      - kitchen_valid: bool
      - violations: list of strings
    """
    result = {
        "articles": [],
        "selection": {},
        "model_key": "",
        "color_key": "",
        "kitchen_valid": False,
        "violations": [],
    }

    # Step 1 — lookup_option for each requested term
    option_by_feature: dict[int, list[dict]] = {}
    for term in lookup_terms[:5]:
        opts = mcp.call_tool("lookup_option", {"query": term, "language": "NL"})
        for opt in opts:
            fn = opt.get("feature_no")
            if fn is not None:
                option_by_feature.setdefault(fn, [])
                # Keep first unique option_key per feature
                keys = [o["option_key"] for o in option_by_feature[fn]]
                if opt["option_key"] not in keys:
                    option_by_feature[fn].append(opt)

    log.info(f"lookup_option found features: {list(option_by_feature.keys())}")

    # Step 2 — search_articles: try with serie_no explicit, then fallback
    raw_articles: list[dict] = []
    for serie in ["1", "2", None]:
        args: dict = {"width_mm": target_width, "limit": 8}
        if serie:
            args["serie_no"] = serie
        found = mcp.call_tool("search_articles", args)
        for a in found:
            if a.get("type_no") and not a.get("error"):
                raw_articles.append(a)
        if raw_articles:
            break

    if not raw_articles:
        # Last resort: no width filter
        raw_articles = mcp.call_tool("search_articles", {"limit": 8})

    log.info(f"search_articles found {len(raw_articles)} articles")
    result["articles"] = raw_articles

    # Step 3 + 4 — for each article: get_article_detail then get_valid_options
    for art in raw_articles[:3]:
        type_no  = art.get("type_no", "")
        serie_no = art.get("serie_no", "0")
        if not type_no:
            continue

        detail_args: dict = {"type_no": type_no}
        if serie_no and serie_no != "0":
            detail_args["serie_no"] = serie_no

        detail = mcp.call_tool("get_article_detail", detail_args)
        if not detail or not isinstance(detail[0], dict) or not detail[0].get("features"):
            continue

        d = detail[0]
        features = {f["feature_no"]: f for f in d.get("features", [])}
        log.info(f"  {type_no} features: {list(features.keys())}")

        chosen: dict[str, str] = {}   # {str(feature_no): option_key}

        # For each feature that has a desired option from lookup:
        for fn_int, desired_opts in sorted(option_by_feature.items()):
            if fn_int not in features:
                continue
            fn_str = str(fn_int)

            # Step 4: get_valid_options with current selection as selection_json
            valid_args: dict = {
                "type_no": type_no,
                "feature_no": fn_int,
                "selection_json": json.dumps({int(k): v for k, v in chosen.items()}) if chosen else "{}",
            }
            if serie_no and serie_no != "0":
                valid_args["serie_no"] = serie_no

            valid_result = mcp.call_tool("get_valid_options", valid_args)
            valid_keys = set()
            if valid_result and isinstance(valid_result[0], dict):
                valid_keys = {o["option_key"] for o in valid_result[0].get("valid_options", [])}

            # Pick the first desired option that is currently valid
            picked = None
            for opt in desired_opts:
                key = opt["option_key"]
                if not valid_keys or key in valid_keys:
                    picked = key
                    break

            # Fallback: first valid option for this feature
            if not picked and valid_result and valid_result[0].get("valid_options"):
                picked = valid_result[0]["valid_options"][0]["option_key"]

            if picked:
                chosen[fn_str] = picked
                log.info(f"    feature {fn_int} → {picked}")

        if not chosen:
            # Fill model (feature 1) at minimum
            if 1 in features and features[1].get("options"):
                chosen["1"] = features[1]["options"][0]["option_key"]

        if chosen:
            result["selection"][type_no] = chosen
            # Extract model/color keys for reference image selection
            if not result["model_key"] and "1" in chosen:
                result["model_key"] = chosen["1"]
            if not result["color_key"] and ("100" in chosen or "101" in chosen):
                result["color_key"] = chosen.get("100") or chosen.get("101", "")
            break   # one article with valid config is enough

    # Step 5 — validate_kitchen
    if result["selection"]:
        kitchen_cfg = [
            {"type_no": tn, "options": {int(k): v for k, v in opts.items()}}
            for tn, opts in result["selection"].items()
        ]
        try:
            val = mcp.call_tool("validate_kitchen", {"kitchen_json": json.dumps(kitchen_cfg)})
            if val and isinstance(val[0], dict):
                result["kitchen_valid"] = val[0].get("kitchen_valid", False)
                result["violations"] = [
                    f"{r['type_no']}: {'; '.join(r['violations'])}"
                    for r in val[0].get("results", [])
                    if r.get("violations")
                ]
        except Exception as e:
            log.warning(f"validate_kitchen: {e}")

    return result


def _build_image_prompt(concept: dict, catalog_items: list[dict]) -> str:
    """
    Build a Gemini image prompt from catalog metadata + design intent.
    The MCP catalog contains handelsartikelen (trade articles: appliances, taps,
    sinks). Cabinet reference images will be added once available from DKG.
    """
    style      = concept.get("style", "modern warm")
    color_desc = concept.get("color_description", "warm natural wood tones")
    worktop    = concept.get("worktop_description", "white marble")
    handle     = concept.get("handle_description", "knob handles")
    has_island = concept.get("has_island", False)
    extra_desc = concept.get("extra_description", "")

    # Extract appliance info from trade articles (what the MCP actually has)
    appliance_parts = []
    dim_parts = []
    for item in catalog_items[:4]:
        name = item.get("name", "")
        dims = item.get("dimensions_mm", {})
        if name:
            # Classify article type for the image prompt
            nl = name.lower()
            if any(w in nl for w in ["oven", "magnetron", "combi"]):
                appliance_parts.append("built-in oven")
            elif any(w in nl for w in ["kraan", "mengkraan", "tap"]):
                appliance_parts.append("designer tap")
            elif any(w in nl for w in ["vaat", "dishwasher"]):
                appliance_parts.append("integrated dishwasher")
            elif any(w in nl for w in ["koel", "fridge", "vriezer"]):
                appliance_parts.append("integrated fridge")
            elif any(w in nl for w in ["stopcontact", "inbouw"]):
                appliance_parts.append("integrated power sockets")
        if dims:
            b = dims.get("b", {})
            w = b.get("nominal", b.get("from", "")) if isinstance(b, dict) else b
            if w:
                dim_parts.append(f"{int(w)}mm")

    appliance_text = ", ".join(dict.fromkeys(appliance_parts)) if appliance_parts else ""
    dim_text = f"Cabinet widths: {', '.join(dim_parts)}." if dim_parts else ""

    return f"""Photorealistic interior design photograph of a Bruynzeel kitchen.
Professional architecture photography, magazine quality, natural daylight through large windows, no people, no text.
Landscape orientation, wide establishing shot showing the full kitchen.

Kitchen specifications:
- Style: {style}
- Cabinet finish: {color_desc}
- Worktop: {worktop}, thick 30mm edge profile
- Handles: {handle}
{f"- Appliances visible: {appliance_text}" if appliance_text else ""}
{f"- {dim_text}" if dim_text else ""}
{"- Kitchen island with seating stools" if has_island else "- L-shape or straight run layout"}
{f"- {extra_desc}" if extra_desc else ""}
- Floor: light herringbone oak parquet
- Wall: off-white metro tiles behind hob
- Lighting: warm pendant lights over island, under-cabinet LED strips
- Atmosphere: inviting, Dutch magazine-quality interior

Bruynzeel Keukens style: clean lines, quality materials, timeless Dutch craftsmanship."""


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
  "lookup_terms": ["NL termen voor lookup_option, bijv: eiken, knop, marmer, wit, mat zwart"],
  "target_width_mm": 600,
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
            "lookup_terms": ["knop", "eiken"],
            "target_width_mm": 600,
            "style_tags": [],
            "extra_description": req.prompt,
        }

    log.info(f"Intent: {intent.get('concept_name')} / {intent.get('style')}")

    # ── 2-5. Sacred sequence: lookup → search → detail → valid_options → validate
    seq = await run_blocking(
        _sacred_sequence,
        intent.get("lookup_terms", ["knop"]),
        intent.get("target_width_mm", 600),
    )

    catalog_valid = seq["kitchen_valid"]
    if seq["violations"]:
        log.warning(f"Violations: {seq['violations']}")

    # Build catalog_items for image prompt
    catalog_items: list[dict] = []
    for art in seq["articles"][:4]:
        type_no = art.get("type_no", "")
        chosen  = seq["selection"].get(type_no, {})
        catalog_items.append({
            "type_no":      type_no,
            "name":         art.get("name", ""),
            "dimensions_mm": art.get("dimensions_mm", {}),
            "chosen_options": chosen,
            "model_name":   "",
        })

    # ── 6. Build Gemini image prompt ──────────────────────────────────────────
    # Note: MCP catalog contains only trade articles (handelsartikelen: appliances,
    # taps, sinks). Cabinet images are not yet available in the catalog.
    # Reference image support will be added once the DKG developer ships that endpoint.
    img_prompt = _build_image_prompt(intent, catalog_items)
    log.info(f"Generating image (prompt {len(img_prompt)} chars)...")

    # ── 7. Generate kitchen image ─────────────────────────────────────────────
    image_b64 = await gemini_image(img_prompt)

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
                price_eur=_estimate_price(item),
            )
            for item in catalog_items
        ],
        image_base64=image_b64,
        catalog_valid=seq["kitchen_valid"],
        gemini_prompt=img_prompt,
    )


@app.post("/adjust", response_model=GenerateResponse)
async def adjust(req: AdjustRequest, x_api_key: str = Header(default="")):
    if APP_API_KEY and x_api_key != APP_API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")

    mcp.ensure_connected()

    combined_prompt = f"{req.original_prompt}. Aanpassing: {req.adjustment}"

    intent_prompt = f"""Je bent een Bruynzeel keuken configurator.
Analyseer de keuken beschrijving inclusief de aanpassing en geef ALLEEN een JSON object terug (geen markdown):
{{
  "concept_name": "Korte Nederlandse naam voor dit concept",
  "style": "one of: modern, klassiek, landelijk, industrieel, scandinavisch",
  "color_description": "beschrijving van kastkleur/fineer in het Engels",
  "worktop_description": "beschrijving van het aanrechtblad in het Engels",
  "handle_description": "beschrijving van de grepen in het Engels",
  "has_island": false,
  "lookup_terms": ["NL termen voor lookup_option"],
  "target_width_mm": 600,
  "style_tags": ["tag1", "tag2", "tag3"],
  "extra_description": "extra details voor beeldgeneratie in het Engels"
}}

Originele beschrijving: "{req.original_prompt}"
Aanpassing: "{req.adjustment}"
Onboarding: {json.dumps(req.onboarding, ensure_ascii=False)}"""

    try:
        intent = await gemini_text(intent_prompt)
    except Exception as e:
        log.warning(f"Gemini intent failed: {e}")
        intent = {
            "concept_name": "Aangepaste Keuken",
            "style": "modern",
            "color_description": "warm natural wood",
            "worktop_description": "white marble",
            "handle_description": "knob handles",
            "has_island": False,
            "lookup_terms": ["knop", "eiken"],
            "target_width_mm": 600,
            "style_tags": [],
            "extra_description": combined_prompt,
        }

    seq = await run_blocking(
        _sacred_sequence,
        intent.get("lookup_terms", ["knop"]),
        intent.get("target_width_mm", 600),
    )

    catalog_items: list[dict] = []
    for art in seq["articles"][:4]:
        type_no = art.get("type_no", "")
        chosen  = seq["selection"].get(type_no, {})
        catalog_items.append({
            "type_no":      type_no,
            "name":         art.get("name", ""),
            "dimensions_mm": art.get("dimensions_mm", {}),
            "chosen_options": chosen,
        })

    # Build adjustment-focused image prompt
    adj_intent = dict(intent)
    adj_intent["extra_description"] = f"{intent.get('extra_description', '')}. Adjustment: {req.adjustment}"
    img_prompt = _build_image_prompt(adj_intent, catalog_items)

    # Use current image as reference for inpainting-style editing
    image_b64 = await gemini_image(img_prompt, reference_b64=req.current_image_base64)

    return GenerateResponse(
        project_id=req.project_id,
        concept_name=intent.get("concept_name", "Aangepaste Keuken"),
        description=combined_prompt,
        style_tags=intent.get("style_tags", []),
        articles=[
            ArticleResult(
                type_no=item["type_no"],
                name=item["name"],
                dimensions_mm=item["dimensions_mm"],
                chosen_options=item["chosen_options"],
                price_eur=_estimate_price(item),
            )
            for item in catalog_items
        ],
        image_base64=image_b64,
        catalog_valid=seq["kitchen_valid"],
        gemini_prompt=img_prompt,
    )
