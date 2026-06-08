"""
Full kitchen generation test:
"genereer een warme keuken met wit marmeren bladen en knop grepen"

Steps:
1. lookup_option for style/material terms
2. search_articles with proper tk_type filter for cabinets
3. get_article_detail for a base + wall cabinet
4. validate_kitchen
5. Call Gemini to generate image using all catalog metadata

Run: python3 test_kitchen_build.py
"""

import json, ssl, socket, time, base64, threading, queue, urllib.request

HOST    = "dkg-dev-dockerswarm-app-ingress.azurewebsites.net"
API_KEY = "D0AF34E63F344B569DA861AF8D326E3F"
GEMINI  = "AIzaSyCiy4UN2JL9D_UN8mIE-8MwpPYvjH40RKc"
EP      = None
_mid    = 0
Q       = queue.Queue()

# ── MCP boilerplate ───────────────────────────────────────────────────────────
def decode_chunked(data):
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
            c = s.recv(8192)
            if not c: break
            buf += c
    except socket.timeout: pass
    s.close()

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

def rpc(method, params=None, timeout=30):
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
                if d.get("id") == mid:
                    return d.get("result") or d.get("error") or {}
            except json.JSONDecodeError:
                # Try to recover partial
                raw = ev["data"]
                last = raw.rfind("}")
                if last > 0:
                    try:
                        d = json.loads(raw[:last+1] + "}" * raw[:last+1].count("{") - raw[:last+1].count("}"))
                    except: pass
        except queue.Empty: pass
    return {}

def tool(name, args, timeout=30):
    r = rpc("tools/call", {"name": name, "arguments": args}, timeout=timeout)
    content = r.get("content", [])
    results = []
    for c in content:
        if c.get("type") == "text":
            text = c["text"]
            try:
                p = json.loads(text)
                if isinstance(p, list): results.extend(p)
                else: results.append(p)
            except json.JSONDecodeError:
                last = text.rfind("}")
                if last > 0:
                    try:
                        partial = text[:last+1]
                        if not partial.strip().startswith("["):
                            partial = "[" + partial + "]"
                        results.extend(json.loads(partial))
                    except: results.append({"raw_truncated": text[:300]})
    return results

rpc("initialize", {"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"kitchen-builder","version":"1"}})
post_rpc(EP, {"jsonrpc":"2.0","method":"notifications/initialized"})
print("MCP ready ✅\n")

# ─────────────────────────────────────────────────────────────────────────────
print("BUILDING: 'warme keuken met wit marmeren bladen en knop grepen'\n")

# 1. Look up all relevant terms
print("Step 1: Option lookups...")
knop_opts  = tool("lookup_option", {"query": "knop", "language": "NL"})
marmer_opts = tool("lookup_option", {"query": "marmer", "language": "NL"})
eiken_opts  = tool("lookup_option", {"query": "eiken", "language": "NL"})
warm_opts   = tool("lookup_option", {"query": "warm", "language": "NL"})
licht_opts  = tool("lookup_option", {"query": "licht", "language": "NL"})

# Deduplicate by option_key
def dedup(opts):
    seen = set(); out = []
    for o in opts:
        k = o.get("option_key","")
        if k not in seen: seen.add(k); out.append(o)
    return out

knop_opts   = dedup(knop_opts)
marmer_opts = dedup(marmer_opts)
eiken_opts  = dedup(eiken_opts)

print(f"  knop grepen: {[o['option_key']+' '+o['name'] for o in knop_opts[:5]]}")
print(f"  marmer:      {[o['option_key']+' '+o['name'] for o in marmer_opts[:5]]}")
print(f"  eiken:       {[o['option_key']+' '+o['name'] for o in eiken_opts[:5]]}")

# 2. Search specifically for base cabinets (onderkast = tk_type 1) and wall cabinets (bovenkast = 2)
print("\nStep 2: Search articles (onderkast 60cm + bovenkast 60cm)...")
base_arts  = tool("search_articles", {"width_mm": 600, "tk_type": "1", "limit": 5}, timeout=40)
wall_arts  = tool("search_articles", {"width_mm": 600, "tk_type": "2", "limit": 3}, timeout=40)
work_arts  = tool("search_articles", {"name_query": "werkblad", "limit": 5}, timeout=40)

print(f"  Base cabinets: {[a.get('type_no','?')+' '+a.get('name','?')[:30] for a in base_arts[:3]]}")
print(f"  Wall cabinets: {[a.get('type_no','?')+' '+a.get('name','?')[:30] for a in wall_arts[:3]]}")
print(f"  Worktops:      {[a.get('type_no','?')+' '+a.get('name','?')[:30] for a in work_arts[:3]]}")

# 3. Get detail for first base cabinet
base_type = base_arts[0]["type_no"] if base_arts else "OL60"
print(f"\nStep 3: get_article_detail('{base_type}')...")
detail = tool("get_article_detail", {"type_no": base_type}, timeout=40)

# Extract dimensions and features
dims = detail[0].get("dimensions_mm", {}) if detail else {}
features = detail[0].get("features", []) if detail else []
feature_map = {f["feature_no"]: f for f in features}

print(f"  Dimensions: {dims}")
print(f"  Features: {[str(f['feature_no'])+' '+f['name'] for f in features[:8]]}")

# 4. Pick options: model + handle + color
model_feature = feature_map.get(1, {})
handle_feature = feature_map.get(300, {})
color_feature  = feature_map.get(100, {})

# Pick a warm model (Laren = classic/warm, Aerdenhout = warm)
chosen_model = next((o["option_key"] for o in model_feature.get("options",[]) 
                     if any(w in o["name"].lower() for w in ["laren","aerdenhout","romantiek","newport"])), 
                    model_feature.get("options",[{}])[0].get("option_key","L9") if model_feature.get("options") else "L9")

# Pick knop handle
chosen_handle = knop_opts[0]["option_key"] if knop_opts else "K012"

# Pick warm color (oak/natural)
chosen_color = next((o["option_key"] for o in color_feature.get("options",[]) 
                     if any(w in o["name"].lower() for w in ["eiken","naturel","warm","hout","beige"])), 
                    color_feature.get("options",[{}])[0].get("option_key","") if color_feature.get("options") else "")

kitchen_config = [{"type_no": base_type, "options": {
    "1":   chosen_model,
    "300": chosen_handle,
}}]
if chosen_color: kitchen_config[0]["options"]["100"] = chosen_color

print(f"\nStep 4: validate_kitchen with config:")
print(f"  Model: {chosen_model}, Handle: {chosen_handle}, Color: {chosen_color}")
val = tool("validate_kitchen", {"kitchen_json": json.dumps(kitchen_config)}, timeout=30)
valid = val[0].get("kitchen_valid", False) if val else False
violations = val[0].get("results", [{}])[0].get("violations", []) if val else []
print(f"  Valid: {valid}, Violations: {violations}")

# 5. Build Gemini prompt from all catalog metadata
print("\nStep 5: Building Gemini image prompt from catalog data...")

model_name  = next((o["name"] for o in model_feature.get("options",[]) if o["option_key"] == chosen_model), chosen_model)
handle_name = next((o["name"] for o in handle_feature.get("options",[]) if o["option_key"] == chosen_handle), chosen_handle) if handle_feature else chosen_handle
color_name  = next((o["name"] for o in color_feature.get("options",[]) if o["option_key"] == chosen_color), chosen_color) if color_feature else ""

dim_text = ""
if dims:
    b = dims.get("b", {})
    h = dims.get("h", {})
    t = dims.get("t", {})
    dim_text = (f"Base cabinet width {b.get('nominal', b.get('from',600))}mm, "
                f"height {h.get('nominal', h.get('from',720))}mm, "
                f"depth {t.get('nominal', t.get('from',560))}mm.")

worktop_info = ""
if marmer_opts:
    worktop_info = "White marble worktop (Carrara-style, thick 30mm edge)."

gemini_prompt = f"""Photorealistic interior design photograph of a Bruynzeel kitchen.
Professional architecture photography, magazine quality, natural daylight through window, no people, no text.
Landscape orientation, wide shot showing full kitchen layout.

Kitchen specifications:
- Cabinet model: {model_name} ({base_type}) — classic warm Dutch design
- Door handle: {handle_name} — decorative knob hardware
- Cabinet color/finish: {color_name if color_name else "warm natural wood tones, oak-inspired"}
- {dim_text}
- {worktop_info if worktop_info else "White marble worktop, thick edge profile"}
- Kitchen island present
- Warm atmosphere: soft pendant lighting over island, wooden accents, plants
- Floor: light herringbone oak parquet
- Wall tiles: metro-style off-white behind hob

Catalog reference: Bruynzeel {model_name} series, {base_type} article, Dutch kitchen design.
Make it warm, inviting, magazine-quality, like a high-end interior photo."""

print(f"\n  Prompt ({len(gemini_prompt)} chars):\n{gemini_prompt[:400]}...\n")

# 6. Call Gemini image gen
print("Step 6: Gemini image generation (this takes ~20s)...")
t0 = time.time()

body = json.dumps({
    "contents": [{"parts": [{"text": gemini_prompt}]}],
    "generationConfig": {"responseModalities": ["TEXT","IMAGE"]},
}).encode()

req = urllib.request.Request(
    f"https://generativelanguage.googleapis.com/v1beta/models/gemini-3-pro-image:generateContent?key={GEMINI}",
    data=body,
    headers={"Content-Type": "application/json"},
)

image_b64 = None
try:
    with urllib.request.urlopen(req, timeout=90) as r:
        resp = json.loads(r.read())
    parts = resp["candidates"][0]["content"]["parts"]
    for p in parts:
        if "inlineData" in p:
            image_b64 = p["inlineData"]["data"]
            break
except Exception as e:
    print(f"  Gemini error: {e}")

elapsed = time.time() - t0

if image_b64:
    out_path = "/tmp/kitchen_result.jpg"
    with open(out_path, "wb") as f:
        f.write(base64.b64decode(image_b64))
    print(f"\n  ✅ Image generated in {elapsed:.1f}s → {out_path} ({len(image_b64)} b64 chars)")
    print(f"\n  Open it: open {out_path}")
else:
    print(f"  ❌ No image returned ({elapsed:.1f}s)")

# 7. Summary of full result
print("\n" + "="*60)
print("RESULT SUMMARY")
print("="*60)
result = {
    "project_id": "test-001",
    "concept_name": "Warme Klassieke Keuken",
    "prompt": "warme keuken met wit marmeren bladen en knop grepen",
    "catalog": {
        "base_cabinet": {"type_no": base_type, "model": model_name, "handle": handle_name, "color": color_name},
        "dimensions_mm": dims,
        "worktop": "wit marmer",
        "knop_options_available": [o["option_key"]+": "+o["name"] for o in knop_opts[:5]],
        "marmer_options_available": [o["option_key"]+": "+o["name"] for o in marmer_opts[:5]],
    },
    "validation": {"valid": valid, "violations": violations},
    "image_generated": image_b64 is not None,
}
print(json.dumps({k:v for k,v in result.items() if k!="catalog"}, indent=2, ensure_ascii=False))
print(json.dumps(result["catalog"], indent=2, ensure_ascii=False))
