"""
Bruynzeel Mijn Keuken — Backend Proxy
Bridges the DKG MCP server and the Flutter app.

Flutter calls: POST /generate  → returns kitchen config + Gemini image (base64)
Flutter calls: GET  /health    → liveness check
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
GEMINI_KEY  = os.getenv("GEMINI_API_KEY", "AIzaSyCiy4UN2JL9D_UN8mIE-8MwpPYvjH40RKc")
APP_API_KEY = os.getenv("APP_API_KEY", "mijn-keuken-secret")   # Flutter sends this

GEMINI_TEXT_URL  = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent"
GEMINI_IMAGE_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-3-pro-image:generateContent"

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("mijn-keuken-api")

# ── MCP Session (one shared session, re-created on failure) ───────────────────

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

            # Start reader thread
            self._reader_thread = threading.Thread(target=self._read_loop, daemon=True)
            self._reader_thread.start()

            # MCP handshake
            self._rpc("initialize", {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "mijn-keuken-backend", "version": "1.0"},
            })
            self._post_raw({
                "jsonrpc": "2.0",
                "method": "notifications/initialized",
            })
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

    def _rpc(self, method: str, params: dict = None, timeout: int = 15) -> dict:
        with self._lock:
            self._mid += 1
            mid = self._mid

        msg = {"jsonrpc": "2.0", "method": method, "id": mid}
        if params:
            msg["params"] = params
        self._post_raw(msg)

        deadline = asyncio.get_event_loop().time() + timeout if False else None
        import time
        end = time.time() + timeout
        while time.time() < end:
            try:
                ev = self._q.get(timeout=0.5)
                d = json.loads(ev["data"])
                if d.get("id") == mid:
                    return d.get("result") or d.get("error") or {}
            except Empty:
                pass
        return {}

    def call_tool(self, name: str, args: dict, timeout: int = 20) -> list[dict]:
        raw = self._rpc("tools/call", {"name": name, "arguments": args}, timeout=timeout)
        content = raw.get("content", [])
        results = []
        for c in content:
            if c.get("type") != "text":
                continue
            text = c["text"]
            # MCP responses can be large and arrive chunked; try parsing,
            # and if the JSON is truncated fall back to extracting partial array items.
            try:
                parsed = json.loads(text)
                if isinstance(parsed, list):
                    results.extend(parsed)
                else:
                    results.append(parsed)
            except json.JSONDecodeError:
                # Try to recover a partial JSON array
                try:
                    # Find last complete object in the array
                    last_close = text.rfind("}")
                    if last_close > 0:
                        partial = text[:last_close + 1]
                        if not partial.strip().startswith("["):
                            partial = "[" + partial + "]"
                        partial += "]"
                        # Remove double-closing bracket if present
                        partial = partial.replace("]]", "]")
                        results.extend(json.loads(partial))
                except Exception:
                    log.warning(f"Could not parse tool response for {name}")
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
    onboarding: list[dict] = []   # [{question, answer}]

class GenerateResponse(BaseModel):
    project_id: str
    concept_name: str
    description: str
    articles: list[dict]
    image_base64: Optional[str] = None
    catalog_valid: bool = False

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


async def gemini_image(prompt: str) -> Optional[str]:
    async with httpx.AsyncClient(timeout=90) as client:
        r = await client.post(
            f"{GEMINI_IMAGE_URL}?key={GEMINI_KEY}",
            json={
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"responseModalities": ["TEXT", "IMAGE"]},
            },
        )
        if r.status_code != 200:
            return None
        parts = r.json()["candidates"][0]["content"]["parts"]
        for p in parts:
            if "inlineData" in p:
                return p["inlineData"]["data"]
    return None


def run_in_thread(fn, *args):
    """Run a blocking call (MCP) in a thread pool so it doesn't block the event loop."""
    loop = asyncio.get_event_loop()
    return loop.run_in_executor(None, fn, *args)

# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "mcp_connected": mcp._ep is not None}


@app.post("/generate", response_model=GenerateResponse)
async def generate(req: GenerateRequest, x_api_key: str = Header(default="")):
    if APP_API_KEY and x_api_key != APP_API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")

    mcp.ensure_connected()

    # 1. Use Gemini to extract catalog search intent from the prompt
    intent_prompt = f"""
Je bent een Bruynzeel keuken configurator assistent.
Analyseer deze keuken beschrijving en geef een JSON object terug:
{{
  "style_terms": ["term1", "term2"],   // Dutch style/material keywords to look up
  "width_mm": 600,                      // typical cabinet width based on description
  "has_island": false,                  // kookeiland?
  "concept_name": "Naam voor dit concept",
  "image_prompt": "English photorealistic kitchen description for image generation"
}}

Keuken beschrijving: "{req.prompt}"
Onboarding antwoorden: {json.dumps(req.onboarding, ensure_ascii=False)}
"""
    try:
        intent = await gemini_text(intent_prompt)
    except Exception as e:
        log.warning(f"Gemini intent failed: {e}")
        intent = {"style_terms": ["modern"], "width_mm": 600, "has_island": False,
                  "concept_name": "Mijn Droomkeuken", "image_prompt": req.prompt}

    # 2. MCP: look up option keys for style terms
    option_keys = []
    for term in intent.get("style_terms", [])[:3]:
        try:
            opts = await run_in_thread(mcp.call_tool, "lookup_option", {"query": term, "language": "NL"})
            option_keys.extend(opts)
        except Exception as e:
            log.warning(f"lookup_option({term}) failed: {e}")

    # 3. MCP: search articles
    articles = []
    try:
        articles = await run_in_thread(
            mcp.call_tool, "search_articles",
            {"width_mm": intent.get("width_mm", 600), "limit": 8},
        )
    except Exception as e:
        log.warning(f"search_articles failed: {e}")

    # 4. MCP: validate kitchen if we have articles
    catalog_valid = False
    if articles:
        try:
            kitchen_cfg = [{"type_no": a["type_no"], "serie_no": a.get("serie_no", "0"), "options": {}} for a in articles[:4]]
            val_result = await run_in_thread(
                mcp.call_tool, "validate_kitchen",
                {"kitchen_json": json.dumps(kitchen_cfg)},
            )
            catalog_valid = val_result[0].get("kitchen_valid", False) if val_result else False
        except Exception as e:
            log.warning(f"validate_kitchen failed: {e}")

    # 5. Gemini image generation
    image_b64 = await gemini_image(
        f"Photorealistic Bruynzeel kitchen interior. {intent.get('image_prompt', req.prompt)}. "
        f"{'Kitchen island present. ' if intent.get('has_island') else ''}"
        f"Professional interior photography, natural light, no people."
    )

    return GenerateResponse(
        project_id=str(uuid.uuid4()),
        concept_name=intent.get("concept_name", "Mijn Droomkeuken"),
        description=req.prompt,
        articles=articles[:6],
        image_base64=image_b64,
        catalog_valid=catalog_valid,
    )
