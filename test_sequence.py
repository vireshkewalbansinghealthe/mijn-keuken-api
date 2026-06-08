"""
Follow the EXACT documented call sequence to build a valid kitchen config.
Find real type_nos, use get_valid_options to navigate features step by step.
"""
import json, ssl, socket, time, threading, queue

HOST    = "dkg-dev-dockerswarm-app-ingress.azurewebsites.net"
API_KEY = "D0AF34E63F344B569DA861AF8D326E3F"
EP = None; _mid = 0; Q = queue.Queue()

def decode_chunked(data):
    out = b""
    while data:
        crlf = data.find(b"\r\n")
        if crlf == -1: break
        try: size = int(data[:crlf], 16)
        except: break
        if size == 0: break
        out += data[crlf+2:crlf+2+size]; data = data[crlf+2+size+2:]
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
    s.sendall((f"POST {path} HTTP/1.1\r\nHost: {HOST}\r\nX-Api-Key: {API_KEY}\r\n"
               f"Content-Type: application/json\r\nContent-Length: {len(body)}\r\nConnection: close\r\n\r\n"
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

# Connect SSE
s_sse = new_sock()
s_sse.sendall((f"GET /idm-mcp/sse HTTP/1.1\r\nHost: {HOST}\r\nX-Api-Key: {API_KEY}\r\n"
               f"Accept: text/event-stream\r\nCache-Control: no-cache\r\nConnection: keep-alive\r\n\r\n").encode())
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

def rpc(method, params=None, timeout=35):
    global _mid; _mid += 1; mid = _mid
    msg = {"jsonrpc":"2.0","method":method,"id":mid}
    if params: msg["params"] = params
    post_rpc(EP, msg)
    end = time.time() + timeout
    while time.time() < end:
        try:
            ev = Q.get(timeout=0.5)
            try:
                d = json.loads(ev["data"])
                if d.get("id") == mid: return d.get("result") or d.get("error") or {}
            except: pass
        except queue.Empty: pass
    return {}

def tool(name, args, timeout=35):
    r = rpc("tools/call", {"name": name, "arguments": args}, timeout=timeout)
    results = []
    for c in r.get("content", []):
        if c.get("type") == "text":
            try:
                p = json.loads(c["text"])
                results.extend(p if isinstance(p, list) else [p])
            except:
                # Try partial recovery
                t = c["text"]; last = t.rfind("}")
                if last > 0:
                    try:
                        partial = t[:last+1]
                        if not partial.strip().startswith("["): partial = "[" + partial + "]"
                        results.extend(json.loads(partial))
                    except: pass
    return results

rpc("initialize", {"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"seq-test","version":"1"}})
post_rpc(EP, {"jsonrpc":"2.0","method":"notifications/initialized"})
print("MCP ✅\n")

# ─────────────────────────────────────────────────────────────────────────────
print("STEP 1: lookup_option — find keys for our kitchen style")
print("-"*50)

# Prompt: "warme keuken met wit marmeren bladen en knop grepen"
lookups = ["eiken", "knop", "marmer", "warm", "wit", "naturel", "lak", "mat"]
option_keys = {}
for term in lookups:
    opts = tool("lookup_option", {"query": term, "language": "NL"})
    if opts:
        option_keys[term] = opts
        for o in opts[:3]:
            print(f"  '{term}' → feature {o['feature_no']} ({o['feature_name']}): {o['option_key']} = {o['name']}")
    else:
        print(f"  '{term}' → (no results)")

# ─────────────────────────────────────────────────────────────────────────────
print("\nSTEP 2: search_articles — find real type_nos")
print("-"*50)

# Try multiple widths and no-filter to find actual cabinets
widths_to_try = [None, 400, 450, 500, 600, 800, 1000]
name_queries  = ["onderkast", "bovenkast", "kast", "OL", "OB", "deur"]
all_articles  = {}

for w in widths_to_try:
    args = {"limit": 5}
    if w: args["width_mm"] = w
    arts = tool("search_articles", args)
    for a in arts:
        tn = a.get("type_no","")
        if tn and tn not in all_articles:
            all_articles[tn] = a
            print(f"  width={w}: {tn} | tk_type={a.get('tk_type')} tk_class={a.get('tk_class')} | {a.get('name','')[:50]}")

print(f"\nUnique articles found: {len(all_articles)}")
print("All type_nos:", list(all_articles.keys())[:20])

# Also try name_query
print("\n  Name queries:")
for q in name_queries:
    arts = tool("search_articles", {"name_query": q, "limit": 3})
    for a in arts:
        tn = a.get("type_no","")
        if tn and tn not in all_articles:
            all_articles[tn] = a
            print(f"  name_query='{q}': {tn} | tk_type={a.get('tk_type')} | {a.get('name','')[:50]}")

# ─────────────────────────────────────────────────────────────────────────────
print("\nSTEP 3: get_article_detail — full features for each real type_no")
print("-"*50)

detailed = {}
for tn, art in list(all_articles.items())[:8]:
    detail = tool("get_article_detail", {"type_no": tn})
    if detail and isinstance(detail[0], dict) and "features" in detail[0]:
        d = detail[0]
        features = d.get("features", [])
        dims = d.get("dimensions_mm", {})
        detailed[tn] = d
        feat_summary = ", ".join(f"{f['feature_no']}:{f['name']}" for f in features[:5])
        print(f"\n  {tn} — {d.get('name','')} (serie={d.get('serie_no')})")
        print(f"    dims: {dims}")
        print(f"    features: {feat_summary}")
        if len(features) > 5: print(f"    ... +{len(features)-5} more features")
    else:
        err = detail[0].get("error","?") if detail else "empty"
        print(f"  {tn}: error — {err}")

if not detailed:
    print("  ⚠ No detailed articles found — catalog may require serie_no")

# ─────────────────────────────────────────────────────────────────────────────
if detailed:
    pick_tn = list(detailed.keys())[0]
    pick_detail = detailed[pick_tn]
    features = pick_detail.get("features", [])
    feat_map = {f["feature_no"]: f for f in features}

    print(f"\nSTEP 4: get_valid_options — navigate features for '{pick_tn}'")
    print("-"*50)

    # Feature 1 = Model
    f1 = feat_map.get(1, {})
    first_model_key = f1.get("options", [{}])[0].get("option_key", "") if f1.get("options") else ""
    print(f"  Feature 1 (Model) options: {[(o['option_key'],o['name']) for o in f1.get('options',[])[:5]]}")

    if first_model_key:
        selection = {"1": first_model_key}
        # What colors are valid after choosing this model?
        valid = tool("get_valid_options", {
            "type_no": pick_tn,
            "feature_no": 100,
            "selection_json": json.dumps(selection),
        })
        if valid:
            v = valid[0]
            print(f"\n  Feature 100 ({v.get('feature_name','color')}) after model={first_model_key}:")
            for opt in v.get("valid_options", [])[:10]:
                print(f"    {opt['option_key']} = {opt['name']}")
            print(f"  ({v.get('count',0)} total options)")

        # What handle options are valid?
        valid_handle = tool("get_valid_options", {
            "type_no": pick_tn,
            "feature_no": 300,
            "selection_json": json.dumps(selection),
        })
        if valid_handle:
            v = valid_handle[0]
            print(f"\n  Feature 300 ({v.get('feature_name','handle')}) after model={first_model_key}:")
            for opt in v.get("valid_options", [])[:10]:
                print(f"    {opt['option_key']} = {opt['name']}")

    # ─────────────────────────────────────────────────────────────────────────
    print(f"\nSTEP 5: validate_kitchen — full config for '{pick_tn}'")
    print("-"*50)

    # Build best config: pick warm/wood model + knop handle
    model_key = first_model_key
    color_key = ""
    handle_key = "K012"

    # Use eiken option key if found
    eiken_opts = option_keys.get("eiken", [])
    if eiken_opts:
        color_key = eiken_opts[0]["option_key"]

    chosen = {"1": model_key}
    if color_key: chosen["100"] = color_key
    chosen["300"] = handle_key

    kitchen = [{"type_no": pick_tn, "options": chosen}]
    print(f"  Config: {json.dumps(kitchen, ensure_ascii=False)}")

    val = tool("validate_kitchen", {"kitchen_json": json.dumps(kitchen)})
    if val:
        v = val[0]
        print(f"  Valid: {v.get('kitchen_valid')} | articles checked: {v.get('articles_checked')}")
        for r in v.get("results", []):
            print(f"  {r['type_no']}: valid={r['valid']} violations={r.get('violations',[])}")

# ─────────────────────────────────────────────────────────────────────────────
print("\n\nSUMMARY: All unique tk_types and tk_classes in catalog")
print("-"*50)
type_class_map = {}
for tn, a in all_articles.items():
    key = (a.get('tk_type','?'), a.get('tk_class','?'))
    if key not in type_class_map: type_class_map[key] = []
    type_class_map[key].append(tn)
for (tt, tc), tns in sorted(type_class_map.items()):
    print(f"  tk_type={tt} tk_class={tc}: {tns[:5]}")
