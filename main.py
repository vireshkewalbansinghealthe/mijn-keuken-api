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

# ── Real Bruynzeel catalog (from bruynzeelkeukens.nl) ─────────────────────────
# Source: https://www.bruynzeelkeukens.nl/keukens/deuren
#         https://www.bruynzeelkeukens.nl/keukens/grepen
#         https://www.bruynzeelkeukens.nl/keukens/keukenbladen
#         https://www.bruynzeelkeukens.nl/keukens/keukenapparatuur

BRUYNZEEL_DEUREN = [
    {"slug":"atlas",        "naam":"Atlas",         "stijl":"modern",             "kleuren":"wit, antraciet, grijs, beige, taupe, zand, creme, olijfgroen, blauw (20 opties)", "materiaal":"MDF",    "afwerking":"mat"},
    {"slug":"atlas_trend",  "naam":"Atlas Trend",   "stijl":"modern/minimalistisch","kleuren":"wit, antraciet, grijs, donkergroen, blauw (10 opties)", "materiaal":"MDF",    "afwerking":"mat"},
    {"slug":"holten",       "naam":"Holten",        "stijl":"scandinavisch/natuur","kleuren":"naturel eiken, gerookt eiken, walnoot, donker eiken (6 opties)", "materiaal":"hout fineer", "afwerking":"eiken"},
    {"slug":"jura",         "naam":"Jura",          "stijl":"scandinavisch/warm",  "kleuren":"wit, zwart, wild eiken, licht eiken (7 opties)", "materiaal":"hout/MDF","afwerking":"mat/hout"},
    {"slug":"andes",        "naam":"Andes",         "stijl":"rustiek/natuur",      "kleuren":"onbehandeld eiken (1 optie)",  "materiaal":"hout",    "afwerking":"ruwe eiken"},
    {"slug":"pallas",       "naam":"Pallas",        "stijl":"modern/supermatt",    "kleuren":"wit, zwart, grijs, beige, blauw, groen, kashmir, taupe (8 opties)", "materiaal":"MDF", "afwerking":"supermatt"},
    {"slug":"matterhorn",   "naam":"Matterhorn",    "stijl":"modern",              "kleuren":"wit, antraciet, grijs, beige, blauw (8 opties)", "materiaal":"MDF",    "afwerking":"mat"},
    {"slug":"linea",        "naam":"Linea",         "stijl":"modern/strak",        "kleuren":"mat zwart, mat wit, antraciet (3 opties)", "materiaal":"MDF",    "afwerking":"mat"},
    {"slug":"geo",          "naam":"Geo",           "stijl":"industrieel/geometrisch","kleuren":"antraciet, zwart, wit (3 opties)", "materiaal":"MDF",    "afwerking":"mat"},
    {"slug":"laren",        "naam":"Laren",         "stijl":"klassiek/landelijk",  "kleuren":"wit, creme, groen, blauw, grijs, beige (32 opties)", "materiaal":"MDF", "afwerking":"klassiek"},
    {"slug":"ravenstein",   "naam":"Ravenstein",    "stijl":"klassiek/deftig",     "kleuren":"wit, gebroken wit, grijs, taupe (44 opties)", "materiaal":"MDF",  "afwerking":"klassiek"},
    {"slug":"senso",        "naam":"Senso",         "stijl":"modern/puur",         "kleuren":"wit, antraciet (2 opties)",    "materiaal":"MDF",    "afwerking":"mat"},
    {"slug":"olympia",      "naam":"Olympia",       "stijl":"landelijk/retro",     "kleuren":"wit, creme, groen, blauw (32 opties)", "materiaal":"MDF",  "afwerking":"mat"},
    {"slug":"karakter",     "naam":"Karakter",      "stijl":"klassiek/ambacht",    "kleuren":"wit, creme, grijs, groen, blauw (44 opties)", "materiaal":"MDF", "afwerking":"mat"},
    {"slug":"piet_zwart",   "naam":"Piet Zwart",    "stijl":"design/iconisch",     "kleuren":"wit, zwart, grijs, antraciet, blauw (5 opties)", "materiaal":"MDF", "afwerking":"mat"},
]

BRUYNZEEL_GREPEN = [
    {"slug":"greeploos_greeplijst","naam":"Greeploos met greeplijst","type":"greeploos","stijl":"modern/strak","detail":"Integrale greeplijst over hele frontbreedte"},
    {"slug":"tip_on",      "naam":"Tip-on systeem",      "type":"greeploos",   "stijl":"ultra-modern/minimalistisch","detail":"Push-to-open, geen greep zichtbaar"},
    {"slug":"infreesgreep","naam":"Infreesgreep aluminium","type":"infreesgreep","stijl":"modern/strak","detail":"Over volledige breedte front, beschermt bovenrand"},
    {"slug":"wave_rvs",    "naam":"Wave RVS geborsteld", "type":"handgreep",   "stijl":"modern",      "detail":"Langwerpige greep, geborsteld roestvrij staal"},
    {"slug":"georgia_ant", "naam":"Georgia mat antraciet","type":"handgreep",  "stijl":"modern",      "detail":"Strakke greep, mat antraciet afwerking"},
    {"slug":"knop_zwart",  "naam":"Knop mat zwart",      "type":"knop",        "stijl":"modern/landelijk","detail":"Ronde knop, mat zwart, subtiele uitstraling"},
    {"slug":"knop_goud",   "naam":"Knop goud",           "type":"knop",        "stijl":"klassiek/landelijk","detail":"Ronde knop, gouden afwerking, warm karakter"},
    {"slug":"knop_nikkel", "naam":"Knop mat nikkel",     "type":"knop",        "stijl":"klassiek",    "detail":"Ronde knop, mat vernikkeld, tijdloos"},
    {"slug":"greep_leer",  "naam":"Leren greep naturel", "type":"handgreep",   "stijl":"landelijk/warm","detail":"Handgreep van echt leer, warm en organisch"},
]

BRUYNZEEL_WERKBLADEN = [
    {"slug":"composiet_calacatta","naam":"Composiet Calacatta wit",    "materiaal":"Composiet","kleur":"wit met grijze aders",       "stijl":"modern/klassiek","prijs_indicatie":"hoog"},
    {"slug":"composiet_statuario","naam":"Composiet Statuario",        "materiaal":"Composiet","kleur":"lichtgrijs marmer look",     "stijl":"modern/luxe",    "prijs_indicatie":"hoog"},
    {"slug":"composiet_nero",     "naam":"Composiet Nero",             "materiaal":"Composiet","kleur":"zwart",                     "stijl":"modern/industrieel","prijs_indicatie":"hoog"},
    {"slug":"keramiek_beton",     "naam":"Keramiek Beton donker",      "materiaal":"Keramiek", "kleur":"donker betongrijs",         "stijl":"industrieel",    "prijs_indicatie":"hoog"},
    {"slug":"keramiek_wit",       "naam":"Keramiek Wit mat",           "materiaal":"Keramiek", "kleur":"zuiver wit mat",            "stijl":"modern",         "prijs_indicatie":"hoog"},
    {"slug":"dekton_kelya",       "naam":"Dekton Kelya",               "materiaal":"Dekton",   "kleur":"donker marmer/leisteen look","stijl":"luxe/modern",   "prijs_indicatie":"hoog"},
    {"slug":"quartsiet",          "naam":"Quartsiet natuursteen",      "materiaal":"Quartsiet","kleur":"naturel grijs-beige",        "stijl":"natuur/luxe",    "prijs_indicatie":"hoog"},
    {"slug":"kunststof_wit",      "naam":"Kunststof Wit mat",          "materiaal":"Kunststof","kleur":"wit mat",                   "stijl":"neutraal",       "prijs_indicatie":"laag"},
    {"slug":"hpl_eiken",          "naam":"HPL Eiken naturel",          "materiaal":"HPL",      "kleur":"naturel eiken houtlook",    "stijl":"scandinavisch/natuur","prijs_indicatie":"midden"},
    {"slug":"greengridz",         "naam":"Greengridz gerecycled",      "materiaal":"Greengridz","kleur":"diverse",                  "stijl":"duurzaam",       "prijs_indicatie":"midden"},
    {"slug":"centoTop",           "naam":"CentoTop",                   "materiaal":"Composiet","kleur":"diverse steentinten",       "stijl":"modern",         "prijs_indicatie":"hoog"},
]

BRUYNZEEL_APPARATUUR_MERKEN = ["AEG","ATAG","BORA","BOSCH","ETNA","GAGGENAU","PELGRIM","SIEMENS","WAVE","Neff"]

BRUYNZEEL_KASTEN_LAYOUTS = {
    "rechte opstelling": "Kasten langs één wand, ideaal voor smalle keukens",
    "L-vorm":            "Kasten langs twee aangrenzende wanden, goed gebruik van hoeken",
    "U-vorm":            "Kasten langs drie wanden, maximale opbergruimte",
    "met eiland":        "Centrale kookunit met losse kasten en extra werkblad",
    "met schiereiland":  "Aangebouwd eiland dat de keuken scheidt van de woonruimte",
    "parallel":          "Twee tegenover elkaar staande keukenblokken",
}

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


BRUYNZEEL_KRANEN = [
    {"slug":"mengkraan_rvs",   "naam":"Mengkraan RVS geborsteld",  "stijl":"modern",    "detail":"Enkelhendel, RVS geborsteld, hoog design"},
    {"slug":"mengkraan_zwart", "naam":"Mengkraan mat zwart",        "stijl":"modern/ind","detail":"Enkelhendel, mat zwart, industrieel karakter"},
    {"slug":"koken_water",     "naam":"Quooker kokend water kraan", "stijl":"luxe",      "detail":"100°C direct, geïntegreerde boiler, 4-in-1"},
    {"slug":"mengkraan_goud",  "naam":"Mengkraan geborsteld goud",  "stijl":"klassiek",  "detail":"Enkelhendel, gouden afwerking, warm accent"},
    {"slug":"mengkraan_chroom","naam":"Mengkraan chroom",           "stijl":"klassiek/n","detail":"Tweehandels, chroom, tijdloze keuze"},
    {"slug":"uittrek_rvs",     "naam":"Uittrekbare mengkraan RVS",  "stijl":"modern",    "detail":"Uittrekbare sproeikop, 360° draaibaar"},
]


class KeukenConfigModel(BaseModel):
    """Full kitchen configuration based on real Bruynzeel catalog data."""
    # Doors/fronts
    deur_slug: str = ""
    deur_naam: str = ""
    deur_kleur: str = ""
    deur_stijl: str = ""
    deur_afwerking: str = ""
    # Handle
    greep_slug: str = ""
    greep_naam: str = ""
    greep_type: str = ""
    greep_detail: str = ""
    # Worktop
    werkblad_slug: str = ""
    werkblad_naam: str = ""
    werkblad_materiaal: str = ""
    werkblad_kleur: str = ""
    # Tap
    kraan_slug: str = ""
    kraan_naam: str = ""
    kraan_detail: str = ""
    # Layout + cabinets
    kasten_layout: str = ""
    kasten_layout_desc: str = ""
    heeft_eiland: bool = False
    # Appliances
    apparatuur_merk: str = ""
    apparatuur_items: list[str] = []
    # MCP-validated trade articles
    mcp_artikelen: list[ArticleResult] = []
    mcp_valid: bool = False


class GenerateResponse(BaseModel):
    project_id: str
    concept_name: str
    description: str
    style_tags: list[str] = []
    keuken_config: Optional[KeukenConfigModel] = None
    image_base64: Optional[str] = None
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
    base = max(400, int((width / 300) * 700))
    return base


def _select_from_catalog(intent: dict, original_prompt: str = "") -> KeukenConfigModel:
    """
    Select real Bruynzeel products based on AI intent.
    Everything comes from the real catalog — nothing is invented.
    """
    style = intent.get("style", "modern").lower()
    has_island = intent.get("has_island", False)
    kleur_desc = intent.get("color_description", "").lower()
    werkblad_desc = intent.get("worktop_description", "").lower()
    greep_desc = intent.get("handle_description", "").lower()
    layout_pref = intent.get("layout", "").lower()
    app_merk = intent.get("apparatuur_merk", "").strip()

    # ── Select door model based on style ────────────────────────────────────
    style_prio: dict[str, list[str]] = {
        "modern":          ["pallas", "atlas", "linea", "senso", "matterhorn"],
        "klassiek":        ["ravenstein", "laren", "karakter", "coevorden"],
        "landelijk":       ["laren", "olympia", "karakter", "holten"],
        "industrieel":     ["geo", "andes", "linea", "atlas"],
        "scandinavisch":   ["holten", "jura", "atlas", "andes"],
        "design":          ["piet_zwart", "pallas", "linea", "senso"],
        "warm":            ["holten", "jura", "laren", "olympia"],
        "minimalistisch":  ["senso", "linea", "atlas_trend", "pallas"],
    }
    prio = style_prio.get(style, style_prio["modern"])
    deur = next((d for slug in prio for d in BRUYNZEEL_DEUREN if d["slug"] == slug), BRUYNZEEL_DEUREN[0])

    # Refine kleur based on color description
    chosen_kleur = deur["kleuren"].split(",")[0].strip()
    if any(w in kleur_desc for w in ["zwart", "black", "dark", "donker", "antraciet", "coal"]):
        chosen_kleur = next(
            (k.strip() for k in deur["kleuren"].split(",") if any(w in k for w in ["zwart","antraciet","dark"])),
            chosen_kleur)
    elif any(w in kleur_desc for w in ["wit", "white", "licht", "light", "cream", "creme"]):
        chosen_kleur = next(
            (k.strip() for k in deur["kleuren"].split(",") if any(w in k for w in ["wit","creme","licht"])),
            chosen_kleur)
    elif any(w in kleur_desc for w in ["eiken", "oak", "hout", "wood", "naturel"]):
        chosen_kleur = next(
            (k.strip() for k in deur["kleuren"].split(",") if any(w in k for w in ["eiken","naturel","hout","oak"])),
            chosen_kleur)
    elif any(w in kleur_desc for w in ["groen", "green", "sage", "olijf"]):
        chosen_kleur = next(
            (k.strip() for k in deur["kleuren"].split(",") if "groen" in k),
            chosen_kleur)
    elif any(w in kleur_desc for w in ["blauw", "blue", "navy"]):
        chosen_kleur = next(
            (k.strip() for k in deur["kleuren"].split(",") if "blauw" in k),
            chosen_kleur)

    # ── Select handle ────────────────────────────────────────────────────────
    greep_prio: dict[str, list[str]] = {
        "modern":         ["greeploos_greeplijst", "wave_rvs", "georgia_ant"],
        "minimalistisch": ["tip_on", "greeploos_greeplijst", "infreesgreep"],
        "industrieel":    ["georgia_ant", "knop_zwart", "wave_rvs"],
        "scandinavisch":  ["knop_zwart", "knop_nikkel", "greeploos_greeplijst"],
        "klassiek":       ["knop_goud", "knop_nikkel", "greep_leer"],
        "landelijk":      ["knop_goud", "greep_leer", "knop_nikkel"],
        "warm":           ["greep_leer", "knop_goud", "knop_nikkel"],
        "design":         ["tip_on", "greeploos_greeplijst", "infreesgreep"],
    }
    g_prio = greep_prio.get(style, greep_prio["modern"])
    if "knop" in greep_desc or "knob" in greep_desc:
        g_prio = ["knop_zwart", "knop_goud", "knop_nikkel"] + g_prio
    elif "greeploos" in greep_desc or "handleless" in greep_desc:
        g_prio = ["greeploos_greeplijst", "tip_on"] + g_prio
    elif "leer" in greep_desc or "leather" in greep_desc:
        g_prio = ["greep_leer"] + g_prio
    greep = next((g for slug in g_prio for g in BRUYNZEEL_GREPEN if g["slug"] == slug), BRUYNZEEL_GREPEN[0])

    # ── Select worktop ───────────────────────────────────────────────────────
    werkblad_prio: dict[str, list[str]] = {
        "modern":         ["composiet_calacatta", "composiet_statuario", "keramiek_wit", "kunststof_wit"],
        "klassiek":       ["composiet_calacatta", "composiet_statuario", "quartsiet"],
        "industrieel":    ["keramiek_beton", "composiet_nero", "dekton_kelya"],
        "scandinavisch":  ["hpl_eiken", "kunststof_wit", "composiet_calacatta"],
        "landelijk":      ["hpl_eiken", "composiet_calacatta", "quartsiet"],
        "warm":           ["hpl_eiken", "composiet_calacatta", "quartsiet"],
        "duurzaam":       ["greengridz", "hpl_eiken", "kunststof_wit"],
        "luxe":           ["quartsiet", "dekton_kelya", "composiet_statuario"],
    }
    w_prio = werkblad_prio.get(style, werkblad_prio["modern"])
    if any(w in werkblad_desc for w in ["marmer","marble","calacatta","statuario","wit","white"]):
        w_prio = ["composiet_calacatta","composiet_statuario"] + w_prio
    elif any(w in werkblad_desc for w in ["zwart","black","nero","donker"]):
        w_prio = ["composiet_nero","dekton_kelya","keramiek_beton"] + w_prio
    elif any(w in werkblad_desc for w in ["beton","concrete","cement","industrieel"]):
        w_prio = ["keramiek_beton","composiet_nero"] + w_prio
    elif any(w in werkblad_desc for w in ["eiken","hout","wood","oak","naturel"]):
        w_prio = ["hpl_eiken","greengridz"] + w_prio
    elif any(w in werkblad_desc for w in ["keramiek","ceramic"]):
        w_prio = ["keramiek_wit","keramiek_beton"] + w_prio
    werkblad = next((w for slug in w_prio for w in BRUYNZEEL_WERKBLADEN if w["slug"] == slug), BRUYNZEEL_WERKBLADEN[0])

    # ── Select layout ────────────────────────────────────────────────────────
    if has_island or "eiland" in layout_pref:
        layout_key = "met eiland"
    elif "u" in layout_pref or "u-vorm" in layout_pref:
        layout_key = "U-vorm"
    elif "l" in layout_pref or "l-vorm" in layout_pref:
        layout_key = "L-vorm"
    elif "parallel" in layout_pref:
        layout_key = "parallel"
    elif "schiereiland" in layout_pref:
        layout_key = "met schiereiland"
    else:
        layout_key = "met eiland" if has_island else "L-vorm"
    layout_desc = BRUYNZEEL_KASTEN_LAYOUTS[layout_key]

    # ── Select appliance brand ───────────────────────────────────────────────
    if not app_merk or app_merk not in BRUYNZEEL_APPARATUUR_MERKEN:
        brand_prio = {
            "modern": "BORA", "industrieel": "BOSCH", "klassiek": "ATAG",
            "scandinavisch": "SIEMENS", "landelijk": "ETNA", "design": "GAGGENAU",
        }
        app_merk = brand_prio.get(style, "BOSCH")

    # ── Build appliances — always complete set, refined by keywords ─────────
    # Combine all text sources for keyword detection
    prompt_lower = " ".join([
        original_prompt,
        intent.get("extra_description", ""),
        intent.get("style", ""),
        intent.get("color_description", ""),
    ]).lower()

    app_items = []

    # 1. Kookplaat — always present, type depends on keywords
    if any(w in prompt_lower for w in ["inductie", "induction"]):
        app_items.append("Inductiekookplaat")
    elif any(w in prompt_lower for w in ["gas", "wok"]):
        app_items.append("Gaskookplaat")
    elif any(w in prompt_lower for w in ["bora", "domino"]):
        app_items.append("Kookplaat met afzuiging (BORA)")
    else:
        app_items.append("Inductiekookplaat")  # default modern choice

    # 2. Oven — always present
    if any(w in prompt_lower for w in ["combi", "stoom", "steam"]):
        app_items.append("Combi-stoomoven")
    else:
        app_items.append("Inbouwoven")

    # 3. Magnetron — always present as separate unit
    if any(w in prompt_lower for w in ["magnetron", "microwave", "combi-magnet"]):
        app_items.append("Combi-magnetron")
    else:
        app_items.append("Magnetron")

    # 4. Vaatwasser — always present
    app_items.append("Vaatwasser")

    # 5. Koelkast — always present
    if any(w in prompt_lower for w in ["amerikaans", "american"]):
        app_items.append("Amerikaanse koelkast")
    elif any(w in prompt_lower for w in ["koel-vriezer", "koel vriezer", "combinatie"]):
        app_items.append("Koel-vriescombinatie")
    else:
        app_items.append("Inbouwkoelkast")

    # 6. Afzuigkap
    if has_island or "eiland" in layout_pref:
        app_items.append("Plafond afzuigkap")
    elif any(w in prompt_lower for w in ["bora", "kookplaat met afzuig"]):
        app_items.append("Geïntegreerde afzuiging (kookplaat)")
    else:
        app_items.append("Afzuigkap")

    # 7. Optionals
    if any(w in prompt_lower for w in ["koffie", "coffee", "barista", "espresso"]):
        app_items.append("Inbouw koffiemachine")
    if any(w in prompt_lower for w in ["wijnklimat", "wine", "wijn"]):
        app_items.append("Wijnklimaatkast")
    if any(w in prompt_lower for w in ["warmhoud", "warming"]):
        app_items.append("Warmhoudlade")

    # ── Select tap ────────────────────────────────────────────────────────────
    kraan_prio: dict[str, list[str]] = {
        "modern":         ["uittrek_rvs", "mengkraan_rvs", "mengkraan_zwart"],
        "minimalistisch": ["mengkraan_rvs", "mengkraan_zwart"],
        "industrieel":    ["mengkraan_zwart", "uittrek_rvs"],
        "scandinavisch":  ["mengkraan_rvs", "uittrek_rvs"],
        "klassiek":       ["mengkraan_goud", "mengkraan_chroom"],
        "landelijk":      ["mengkraan_goud", "mengkraan_chroom", "uittrek_rvs"],
        "warm":           ["mengkraan_goud", "uittrek_rvs"],
        "luxe":           ["koken_water", "mengkraan_goud"],
        "design":         ["mengkraan_zwart", "koken_water"],
    }
    k_prio = kraan_prio.get(style, kraan_prio["modern"])
    if any(w in prompt_lower for w in ["quooker", "kokend water", "boiling"]):
        k_prio = ["koken_water"] + k_prio
    elif any(w in prompt_lower for w in ["zwart", "black", "mat"]):
        k_prio = ["mengkraan_zwart"] + k_prio
    elif any(w in prompt_lower for w in ["goud", "gold", "brass", "messing"]):
        k_prio = ["mengkraan_goud"] + k_prio
    kraan = next((k for slug in k_prio for k in BRUYNZEEL_KRANEN if k["slug"] == slug), BRUYNZEEL_KRANEN[0])

    return KeukenConfigModel(
        deur_slug=deur["slug"],
        deur_naam=deur["naam"],
        deur_kleur=chosen_kleur,
        deur_stijl=deur["stijl"],
        deur_afwerking=deur["afwerking"],
        greep_slug=greep["slug"],
        greep_naam=greep["naam"],
        greep_type=greep["type"],
        greep_detail=greep["detail"],
        werkblad_slug=werkblad["slug"],
        werkblad_naam=werkblad["naam"],
        werkblad_materiaal=werkblad["materiaal"],
        werkblad_kleur=werkblad["kleur"],
        kraan_slug=kraan["slug"],
        kraan_naam=kraan["naam"],
        kraan_detail=kraan["detail"],
        kasten_layout=layout_key,
        kasten_layout_desc=layout_desc,
        heeft_eiland=has_island,
        apparatuur_merk=app_merk,
        apparatuur_items=list(dict.fromkeys(app_items)),
    )


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


def _build_image_prompt(intent: dict, config: KeukenConfigModel) -> str:
    """
    Build a Gemini image prompt from the real Bruynzeel catalog selection.
    Every detail comes from the actual catalog — nothing is invented.
    """
    has_island = config.heeft_eiland
    layout = config.kasten_layout
    extra   = intent.get("extra_description", "")

    return f"""Photorealistic interior design photograph of a Bruynzeel kitchen.
Professional architecture photography, magazine quality, natural daylight through large windows, no people, no text.
Landscape orientation, wide establishing shot showing the full kitchen.

Kitchen specification (Bruynzeel catalog):
- Door model: {config.deur_naam} — {config.deur_afwerking} in {config.deur_kleur}
- Handles: {config.greep_naam} ({config.greep_type})
- Worktop: {config.werkblad_naam} — {config.werkblad_materiaal}, {config.werkblad_kleur}, thick 30mm edge profile
- Layout: {layout} — {config.kasten_layout_desc}
- Appliances: {config.apparatuur_merk} brand — {", ".join(config.apparatuur_items[:3])}
{f"- {extra}" if extra else ""}
- Floor: light herringbone oak parquet
- Wall: off-white metro tiles behind hob
- Lighting: warm pendant lights{" over island," if has_island else ","} under-cabinet LED strips
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

    # ── 1. Gemini: extract structured intent ──────────────────────────────────
    intent_prompt = f"""Je bent een Bruynzeel keuken configurator. Analyseer en geef ALLEEN JSON (geen markdown):
{{
  "concept_name": "Korte Nederlandse naam",
  "style": "one of: modern, klassiek, landelijk, industrieel, scandinavisch, warm, minimalistisch, design",
  "color_description": "kastkleur/fineer beschrijving in het Engels",
  "worktop_description": "aanrechtblad beschrijving in het Engels",
  "handle_description": "grepen beschrijving in het Engels (knob/handleless/bar handle/leather)",
  "has_island": false,
  "layout": "one of: rechte opstelling, L-vorm, U-vorm, met eiland, met schiereiland, parallel",
  "apparatuur_merk": "one of: AEG, ATAG, BORA, BOSCH, ETNA, GAGGENAU, PELGRIM, SIEMENS, WAVE, Neff or empty",
  "lookup_terms": ["altijd minimaal: kraan, vaatwasser + andere NL termen vanuit de prompt, bijv: knop, marmer, inductie"],
  "target_width_mm": 600,
  "style_tags": ["tag1", "tag2", "tag3"],
  "extra_description": "extra beeldgeneratie details in het Engels"
}}

BELANGRIJK: lookup_terms moet ALTIJD minimaal ["kraan", "vaatwasser"] bevatten, plus extra termen vanuit de klantwens.

Beschrijving: "{req.prompt}"
Onboarding: {json.dumps(req.onboarding, ensure_ascii=False)}"""

    try:
        intent = await gemini_text(intent_prompt)
    except Exception as e:
        log.warning(f"Gemini intent failed: {e}")
        intent = {
            "concept_name": "Moderne Droomkeuken",
            "style": "modern",
            "color_description": "warm natural wood",
            "worktop_description": "white marble",
            "handle_description": "handleless",
            "has_island": False,
            "layout": "L-vorm",
            "apparatuur_merk": "BOSCH",
            "lookup_terms": ["knop", "eiken"],
            "target_width_mm": 600,
            "style_tags": ["modern", "strak"],
            "extra_description": req.prompt,
        }

    log.info(f"Intent: {intent.get('concept_name')} / {intent.get('style')}")

    # ── 2. Select from real Bruynzeel catalog ─────────────────────────────────
    config = _select_from_catalog(intent, original_prompt=req.prompt)
    log.info(f"Config: {config.deur_naam} / {config.werkblad_naam} / {config.greep_naam} / {config.kraan_naam}")

    # ── 3-6. Sacred MCP sequence for handelsartikelen validation ─────────────
    raw_terms = intent.get("lookup_terms", [])
    lookup_terms = list(dict.fromkeys(["kraan", "vaatwasser"] + [t for t in raw_terms if t not in ("kraan", "vaatwasser")]))
    seq = await run_blocking(
        _sacred_sequence,
        lookup_terms,
        intent.get("target_width_mm", 600),
    )
    if seq["violations"]:
        log.warning(f"MCP violations: {seq['violations']}")

    # Attach MCP-validated trade articles to config
    mcp_artikelen = [
        ArticleResult(
            type_no=art.get("type_no", ""),
            name=art.get("name", ""),
            dimensions_mm=art.get("dimensions_mm", {}),
            chosen_options=seq["selection"].get(art.get("type_no", ""), {}),
            price_eur=_estimate_price(art),
        )
        for art in seq["articles"][:3]
    ]
    config.mcp_artikelen = mcp_artikelen
    config.mcp_valid = seq["kitchen_valid"]

    # ── 7. Build image prompt from real catalog selection ─────────────────────
    img_prompt = _build_image_prompt(intent, config)
    log.info(f"Generating image ({len(img_prompt)} chars)...")

    # ── 8. Generate kitchen image ─────────────────────────────────────────────
    image_b64 = await gemini_image(img_prompt)

    return GenerateResponse(
        project_id=str(uuid.uuid4()),
        concept_name=intent.get("concept_name", "Mijn Droomkeuken"),
        description=req.prompt,
        style_tags=intent.get("style_tags", []),
        keuken_config=config,
        image_base64=image_b64,
        gemini_prompt=img_prompt,
    )


@app.post("/adjust", response_model=GenerateResponse)
async def adjust(req: AdjustRequest, x_api_key: str = Header(default="")):
    if APP_API_KEY and x_api_key != APP_API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")

    mcp.ensure_connected()

    combined_prompt = f"{req.original_prompt}. Aanpassing: {req.adjustment}"

    intent_prompt = f"""Je bent een Bruynzeel keuken configurator. Analyseer en geef ALLEEN JSON (geen markdown):
{{
  "concept_name": "Korte Nederlandse naam",
  "style": "one of: modern, klassiek, landelijk, industrieel, scandinavisch, warm, minimalistisch, design",
  "color_description": "kastkleur beschrijving in het Engels",
  "worktop_description": "aanrechtblad beschrijving in het Engels",
  "handle_description": "grepen beschrijving in het Engels",
  "has_island": false,
  "layout": "one of: rechte opstelling, L-vorm, U-vorm, met eiland, met schiereiland, parallel",
  "apparatuur_merk": "one of: AEG, ATAG, BORA, BOSCH, ETNA, GAGGENAU, PELGRIM, SIEMENS, WAVE, Neff or empty",
  "lookup_terms": ["NL termen voor MCP"],
  "target_width_mm": 600,
  "style_tags": ["tag1", "tag2", "tag3"],
  "extra_description": "details in het Engels inclusief aanpassing"
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
            "handle_description": "handleless",
            "has_island": False,
            "layout": "L-vorm",
            "apparatuur_merk": "BOSCH",
            "lookup_terms": ["knop"],
            "target_width_mm": 600,
            "style_tags": [],
            "extra_description": combined_prompt,
        }

    intent["extra_description"] = f"{intent.get('extra_description','')}. Aanpassing: {req.adjustment}"

    config = _select_from_catalog(intent, original_prompt=combined_prompt)

    raw_terms_adj = intent.get("lookup_terms", [])
    lookup_terms_adj = list(dict.fromkeys(["kraan", "vaatwasser"] + [t for t in raw_terms_adj if t not in ("kraan", "vaatwasser")]))
    seq = await run_blocking(
        _sacred_sequence,
        lookup_terms_adj,
        intent.get("target_width_mm", 600),
    )
    config.mcp_artikelen = [
        ArticleResult(
            type_no=art.get("type_no", ""),
            name=art.get("name", ""),
            dimensions_mm=art.get("dimensions_mm", {}),
            chosen_options=seq["selection"].get(art.get("type_no", ""), {}),
            price_eur=_estimate_price(art),
        )
        for art in seq["articles"][:3]
    ]
    config.mcp_valid = seq["kitchen_valid"]

    img_prompt = _build_image_prompt(intent, config)
    image_b64 = await gemini_image(img_prompt, reference_b64=req.current_image_base64)

    return GenerateResponse(
        project_id=req.project_id,
        concept_name=intent.get("concept_name", "Aangepaste Keuken"),
        description=combined_prompt,
        style_tags=intent.get("style_tags", []),
        keuken_config=config,
        image_base64=image_b64,
        gemini_prompt=img_prompt,
    )
