"""
Image hunt: scan every field of every response for image data, URLs, or image keys.
Also tries get_article_detail on multiple real articles to find thumbnails.
"""

import json, ssl, socket, time, threading, queue, re

HOST    = "dkg-dev-dockerswarm-app-ingress.azurewebsites.net"
API_KEY = "D0AF34E63F344B569DA861AF8D326E3F"
EP      = None
_mid    = 0
Q       = queue.Queue()

def decode_chunked(data):
    out = b""
    while data:
        crlf = data.find(b"\r\n")
        if crlf == -1: break
        try: size = int(data[:crlf], 16)
        except: break
        if size == 0: break
        out += data[crlf+2: crlf+2+size]
        data = data[crlf+2+size+2:]
    return out

def parse_events(raw):
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
    return ctx.wrap_socket(socket.create_connection((HOST, 443), timeout=12), server_hostname=HOST)

def post_rpc(path, payload):
    body = json.dumps(payload).encode()
    s = new_sock()
    s.sendall((
        f"POST {path} HTTP/1.1\r\nHost: {HOST}\r\n"
        f"X-Api-Key: {API_KEY}\r\nContent-Type: application/json\r\n"
        f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n"
    ).encode() + body)
    buf = b""
    s.settimeout(5)
    try:
        while True:
            c = s.recv(8192)
            if not c: break
            buf += c
    except socket.timeout: pass
    s.close()

# ── Connect ───────────────────────────────────────────────────────────────────
s_sse = new_sock()
s_sse.sendall((
    f"GET /idm-mcp/sse HTTP/1.1\r\nHost: {HOST}\r\n"
    f"X-Api-Key: {API_KEY}\r\nAccept: text/event-stream\r\n"
    f"Cache-Control: no-cache\r\nConnection: keep-alive\r\n\r\n"
).encode())

buf = b""
while b"\r\n\r\n" not in buf: buf += s_sse.recv(1)
_, body = buf.split(b"\r\n\r\n", 1)
s_sse.settimeout(8)
try:
    while True:
        body += s_sse.recv(4096)
        evs = parse_events(decode_chunked(body).decode("utf-8","ignore"))
        ep_ev = next((e for e in evs if e.get("event") == "endpoint"), None)
        if ep_ev: break
except socket.timeout: pass

raw_path = ep_ev["data"].strip()
EP = raw_path if raw_path.startswith("/idm-mcp") else "/idm-mcp" + raw_path
print(f"Session: {EP}")

def reader():
    s_sse.settimeout(60)
    partial = b""
    try:
        while True:
            chunk = s_sse.recv(8192)
            if not chunk: break
            partial += chunk
            for ev in parse_events(decode_chunked(partial).decode("utf-8","ignore")):
                if ev.get("event") == "message": Q.put(ev)
    except: pass
threading.Thread(target=reader, daemon=True).start()

def rpc(method, params=None, timeout=35):
    global _mid
    _mid += 1; mid = _mid
    msg = {"jsonrpc": "2.0", "method": method, "id": mid}
    if params: msg["params"] = params
    post_rpc(EP, msg)
    end = time.time() + timeout
    while time.time() < end:
        try:
            ev = Q.get(timeout=0.5)
            try:
                d = json.loads(ev["data"])
                if d.get("id") == mid: return d
            except: pass
        except queue.Empty: pass
    return {}

def tool_raw(name, args, timeout=35):
    """Return the FULL raw result dict — don't parse content."""
    r = rpc("tools/call", {"name": name, "arguments": args}, timeout=timeout)
    return r.get("result", {})

rpc("initialize", {"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"img-hunt","version":"1"}})
post_rpc(EP, {"jsonrpc":"2.0","method":"notifications/initialized"})
print("MCP ready ✅\n")

def all_keys(obj, prefix=""):
    """Recursively collect all key paths in a JSON object."""
    keys = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            keys.append(f"{prefix}.{k}" if prefix else k)
            keys.extend(all_keys(v, f"{prefix}.{k}" if prefix else k))
    elif isinstance(obj, list) and obj:
        keys.extend(all_keys(obj[0], f"{prefix}[0]"))
    return keys

def find_image_hints(obj, path=""):
    """Find anything that looks like an image: URLs, base64, keys with 'img/image/photo/thumb'."""
    found = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            p = f"{path}.{k}" if path else k
            kl = k.lower()
            if isinstance(v, str):
                if any(w in kl for w in ["img","image","photo","thumb","picture","url","src","blob","media","afbeelding"]):
                    found.append((p, str(v)[:300]))
                elif v.startswith("http") and any(x in v for x in [".jpg",".png",".webp",".jpeg","image","photo","thumb","media","cdn"]):
                    found.append((p, v[:300]))
                elif len(v) > 500 and re.match(r'^[A-Za-z0-9+/=]+$', v[:100]):
                    found.append((p, f"<possible base64: {len(v)} chars>"))
            found.extend(find_image_hints(v, p))
    elif isinstance(obj, list):
        for i, item in enumerate(obj[:3]):
            found.extend(find_image_hints(item, f"{path}[{i}]"))
    return found

# ─────────────────────────────────────────────────────────────────────────────
print("="*60)
print("A. search_articles — RAW full response structure scan")
print("="*60)
r = tool_raw("search_articles", {"width_mm": 600, "limit": 10})
content = r.get("content", [])
print(f"Content items: {len(content)}, types: {[c.get('type') for c in content]}")
for i, c in enumerate(content):
    print(f"\nContent[{i}] type={c.get('type')} keys={list(c.keys())}")
    if c.get("type") == "text":
        try:
            parsed = json.loads(c["text"])
            if isinstance(parsed, list) and parsed:
                print(f"  Array of {len(parsed)} items. First item keys: {list(parsed[0].keys())}")
                print(f"  First item: {json.dumps(parsed[0], ensure_ascii=False)[:600]}")
                imgs = find_image_hints(parsed)
                print(f"  Image hints: {imgs if imgs else 'NONE'}")
        except: print(f"  raw text: {c['text'][:400]}")
    elif c.get("type") == "image":
        print(f"  *** IMAGE CONTENT! mimeType={c.get('mimeType')}, data length={len(c.get('data',''))}")
    else:
        # Check for any other types
        print(f"  Full: {json.dumps(c, ensure_ascii=False)[:400]}")

# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("B. Get real type_nos from search, then detail scan on each")
print("="*60)

# Get a list of type_nos to try
try:
    arts_text = content[0]["text"] if content and content[0].get("type") == "text" else "[]"
    arts = json.loads(arts_text) if isinstance(json.loads(arts_text), list) else []
except: arts = []

# Also try some known Bruynzeel article codes
candidate_types = [a.get("type_no","") for a in arts[:4] if a.get("type_no")] 
candidate_types += ["OL60", "OB60", "HL60", "HB60", "9AWAB_1_1F"]
candidate_types = list(dict.fromkeys(candidate_types))[:6]

print(f"Will probe: {candidate_types}")
for tn in candidate_types:
    r = tool_raw("get_article_detail", {"type_no": tn})
    content_list = r.get("content", [])
    if not content_list:
        print(f"  {tn}: empty response")
        continue
    for c in content_list:
        print(f"\n  {tn} → content type={c.get('type')}, keys={list(c.keys())}")
        if c.get("type") == "text":
            try:
                parsed = json.loads(c["text"])
                keys = all_keys(parsed)
                print(f"    All keys: {keys[:40]}")
                imgs = find_image_hints(parsed)
                print(f"    Image hints: {imgs if imgs else 'NONE'}")
            except Exception as e:
                print(f"    Parse error: {e}, raw: {c['text'][:300]}")
        elif c.get("type") == "image":
            print(f"    *** IMAGE! mimeType={c.get('mimeType')} data={len(c.get('data',''))} chars")

# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("C. lookup_option — check if options have image/thumbnail fields")
print("="*60)
for term in ["knop", "eiken", "marmer", "mat zwart", "wit"]:
    r = tool_raw("lookup_option", {"query": term, "language": "NL"})
    content_list = r.get("content", [])
    for c in content_list:
        if c.get("type") == "text":
            try:
                parsed = json.loads(c["text"])
                if isinstance(parsed, list) and parsed:
                    keys = list(parsed[0].keys()) if isinstance(parsed[0], dict) else []
                    imgs = find_image_hints(parsed)
                    print(f"  '{term}': option keys={keys}, image_hints={imgs if imgs else 'NONE'}")
            except: pass
        elif c.get("type") == "image":
            print(f"  '{term}': *** IMAGE content! {c.get('mimeType')}")

# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("D. tools/list — check if there are undocumented tools (e.g. get_image)")
print("="*60)
# Read response fully since it's large
_mid += 1; mid = _mid
post_rpc(EP, {"jsonrpc": "2.0", "method": "tools/list", "id": mid})
# Collect all SSE messages for this id for up to 15s
all_data = []
end = time.time() + 15
while time.time() < end:
    try:
        ev = Q.get(timeout=0.5)
        try:
            raw_text = ev["data"]
            # Try to find our message id in the raw text before full parse
            if f'"id":{mid}' in raw_text or f'"id": {mid}' in raw_text:
                all_data.append(raw_text)
                # Try parsing
                try:
                    d = json.loads(raw_text)
                    tools = d.get("result", {}).get("tools", [])
                    for t in tools:
                        print(f"  Tool: {t['name']} — {t.get('description','')[:80]}")
                    break
                except json.JSONDecodeError:
                    # Truncated — extract tool names via regex
                    names = re.findall(r'"name"\s*:\s*"([^"]+)"', raw_text)
                    print(f"  Tools (partial): {names}")
                    break
        except: pass
    except queue.Empty: pass

print("\nDone.")
