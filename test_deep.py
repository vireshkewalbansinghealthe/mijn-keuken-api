"""
Deep probe: test every MCP tool relevant to kitchen generation.
Specifically looking for:
  - image data in get_article_detail / search_articles
  - dimensions in responses
  - what validate_kitchen returns
  
Run: python3 test_deep.py
"""

import json, ssl, socket, time, base64, os

HOST    = "dkg-dev-dockerswarm-app-ingress.azurewebsites.net"
API_KEY = "D0AF34E63F344B569DA861AF8D326E3F"
EP      = None   # filled after SSE handshake
_mid    = 0

def decode_chunked(data: bytes) -> bytes:
    out = b""
    while data:
        crlf = data.find(b"\r\n")
        if crlf == -1: break
        try: size = int(data[:crlf], 16)
        except ValueError: break
        if size == 0: break
        out += data[crlf+2: crlf+2+size]
        data = data[crlf+2+size+2:]
    return out

def parse_events(raw: str) -> list:
    events = []
    for block in raw.split("\n\n"):
        ev = {}
        for line in block.strip().splitlines():
            if line.startswith("event:"): ev["event"] = line[6:].strip()
            elif line.startswith("data:"): ev["data"] = line[5:].strip()
        if ev: events.append(ev)
    return events

def new_sock():
    ctx = ssl.create_default_context()
    return ctx.wrap_socket(socket.create_connection((HOST, 443), timeout=10), server_hostname=HOST)

def post(path, payload):
    body = json.dumps(payload).encode()
    s = new_sock()
    req = (
        f"POST {path} HTTP/1.1\r\nHost: {HOST}\r\n"
        f"X-Api-Key: {API_KEY}\r\nContent-Type: application/json\r\n"
        f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n"
    ).encode() + body
    s.sendall(req)
    buf = b""
    s.settimeout(5)
    try:
        while True:
            c = s.recv(4096)
            if not c: break
            buf += c
    except socket.timeout: pass
    s.close()

# ── Connect SSE ───────────────────────────────────────────────────────────────
print("Connecting SSE...")
s = new_sock()
s.sendall((
    f"GET /idm-mcp/sse HTTP/1.1\r\nHost: {HOST}\r\n"
    f"X-Api-Key: {API_KEY}\r\nAccept: text/event-stream\r\n"
    f"Cache-Control: no-cache\r\nConnection: keep-alive\r\n\r\n"
).encode())

buf = b""
while b"\r\n\r\n" not in buf:
    buf += s.recv(1)
_, body = buf.split(b"\r\n\r\n", 1)

s.settimeout(8)
try:
    while True:
        body += s.recv(4096)
        evs = parse_events(decode_chunked(body).decode("utf-8","ignore"))
        ep_ev = next((e for e in evs if e.get("event") == "endpoint"), None)
        if ep_ev: break
except socket.timeout: pass

raw_path = ep_ev["data"].strip()
EP = raw_path if raw_path.startswith("/idm-mcp") else "/idm-mcp" + raw_path
print(f"Session: {EP}")

# SSE reader thread
import threading, queue
Q = queue.Queue()
def reader():
    s.settimeout(60)
    partial = b""
    try:
        while True:
            chunk = s.recv(4096)
            if not chunk: break
            partial += chunk
            for ev in parse_events(decode_chunked(partial).decode("utf-8","ignore")):
                if ev.get("event") == "message": Q.put(ev)
    except: pass
threading.Thread(target=reader, daemon=True).start()

def rpc(method, params=None, timeout=25):
    global _mid
    _mid += 1
    mid = _mid
    msg = {"jsonrpc": "2.0", "method": method, "id": mid}
    if params: msg["params"] = params
    post(EP, msg)
    end = time.time() + timeout
    while time.time() < end:
        try:
            ev = Q.get(timeout=0.5)
            d = json.loads(ev["data"])
            if d.get("id") == mid:
                return d.get("result") or d.get("error") or {}
        except queue.Empty: pass
    return {}

# Handshake
rpc("initialize", {"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"probe","version":"1"}})
post(EP, {"jsonrpc":"2.0","method":"notifications/initialized"})
print("MCP ready ✅\n")

# ─────────────────────────────────────────────────────────────────────────────
def tool(name, args, timeout=25):
    r = rpc("tools/call", {"name": name, "arguments": args}, timeout=timeout)
    content = r.get("content", [])
    results = []
    for c in content:
        if c.get("type") == "text":
            try: results.append(json.loads(c["text"]))
            except: results.append({"raw": c["text"][:500]})
        elif c.get("type") == "image":
            results.append({"IMAGE_DATA": f"<base64 {len(c.get('data',''))} chars, mime:{c.get('mimeType')}>", "full_image": c.get('data','')})
    return results

def scan_for_images(obj, path=""):
    """Recursively look for any base64 image data or image URLs in a response."""
    found = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            p = f"{path}.{k}"
            if isinstance(v, str):
                if len(v) > 200 and all(c in 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=' for c in v[:50]):
                    found.append((p, f"<possible base64: {len(v)} chars>"))
                elif v.startswith("http") and any(x in v for x in [".jpg",".png",".webp",".jpeg","image"]):
                    found.append((p, v))
                elif k.lower() in ("image","img","photo","thumbnail","picture","preview","url","src"):
                    found.append((p, str(v)[:200]))
            found.extend(scan_for_images(v, p))
    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            found.extend(scan_for_images(item, f"{path}[{i}]"))
    return found

# ─────────────────────────────────────────────────────────────────────────────
print("=" * 60)
print("1. lookup_option('knop') — knob handles")
print("=" * 60)
knop = tool("lookup_option", {"query": "knop", "language": "NL"})
print(json.dumps(knop[:5], indent=2, ensure_ascii=False))

print("\n" + "=" * 60)
print("2. lookup_option('wit marmer') — white marble worktop")
print("=" * 60)
marmer = tool("lookup_option", {"query": "wit marmer", "language": "NL"})
print(json.dumps(marmer[:5], indent=2, ensure_ascii=False))

print("\n" + "=" * 60)
print("3. lookup_option('warm hout') — warm wood finish")
print("=" * 60)
hout = tool("lookup_option", {"query": "warm hout", "language": "NL"})
print(json.dumps(hout[:5], indent=2, ensure_ascii=False))

print("\n" + "=" * 60)
print("4. search_articles(width_mm=600, limit=3) — look for cabinet dims")
print("=" * 60)
arts = tool("search_articles", {"width_mm": 600, "limit": 3}, timeout=30)
print(json.dumps(arts, indent=2, ensure_ascii=False))
imgs = scan_for_images(arts)
print(f"\n  🔍 Image scan: {imgs if imgs else 'No images found'}")

# Pick first article for detail
first_type_no = None
if arts and isinstance(arts[0], list) and arts[0]:
    first_type_no = arts[0][0].get("type_no")
elif arts and isinstance(arts[0], dict):
    first_type_no = arts[0].get("type_no")

print("\n" + "=" * 60)
print(f"5. get_article_detail('{first_type_no or 'OL60'}') — FULL detail with image check")
print("=" * 60)
detail = tool("get_article_detail", {"type_no": first_type_no or "OL60"}, timeout=30)
imgs2 = scan_for_images(detail)
print(f"\n  🔍 Image scan: {imgs2 if imgs2 else 'No images found'}")
# Print structure without potentially huge base64
def strip_b64(obj):
    if isinstance(obj, dict):
        return {k: (f"<base64:{len(v)}>" if isinstance(v,str) and len(v)>200 else strip_b64(v)) for k,v in obj.items()}
    if isinstance(obj, list):
        return [strip_b64(i) for i in obj]
    return obj
print(json.dumps(strip_b64(detail), indent=2, ensure_ascii=False)[:3000])

print("\n" + "=" * 60)
print("6. validate_kitchen — test with a realistic config")
print("=" * 60)
# Use what we found from lookup_option
knop_key = knop[0][0]["option_key"] if knop and isinstance(knop[0], list) and knop[0] else "KN"
test_kitchen = json.dumps([
    {"type_no": first_type_no or "OL60", "options": {}},
])
val = tool("validate_kitchen", {"kitchen_json": test_kitchen}, timeout=30)
print(json.dumps(val, indent=2, ensure_ascii=False)[:2000])

print("\n" + "=" * 60)
print("7. Checking RAW content types returned by tools/list")
print("=" * 60)
tools_list = rpc("tools/list", timeout=10)
for t in tools_list.get("tools", []):
    print(f"  - {t['name']}: {t.get('description','')[:80]}")

print("\nDone.")
