from datetime import datetime, timedelta, timezone
from ipaddress import ip_network
import os

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse

from .db import client

app = FastAPI(title="AKGATE Dashboard", version="0.1.0")

CUSTOMER_NETWORKS = [
    ip_network(x.strip())
    for x in os.getenv("CUSTOMER_NETWORKS", "").split(",")
    if x.strip()
]

def v4(expr: str) -> str:
    return f"replaceRegexpOne(toString({expr}), '^::ffff:', '')"

def customer_filter(expr: str) -> str:
    parts = []
    for net in CUSTOMER_NETWORKS:
        parts.append(
            f"isIPAddressInRange({v4(expr)}, '{net.with_prefixlen}')"
        )
    return "(" + " OR ".join(parts) + ")" if parts else "1"

@app.get("/health")
def health():
    try:
        value = client().query("SELECT 1").result_rows[0][0]
        return {"status": "ok", "clickhouse": value == 1}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc))

@app.get("/api/summary")
def summary(minutes: int = Query(5, ge=1, le=1440)):
    ch = client()
    sql = f"""
    SELECT
      sumIf(Bytes * SamplingRate, InIfBoundary = 'external') AS down_bytes,
      sumIf(Bytes * SamplingRate, OutIfBoundary = 'external') AS up_bytes,
      count() AS flows
    FROM flows
    WHERE TimeReceived >= now() - INTERVAL {minutes} MINUTE
    """
    down, up, flows = ch.query(sql).result_rows[0]
    seconds = minutes * 60
    return {
        "window_minutes": minutes,
        "download_bps": int(down * 8 / seconds),
        "upload_bps": int(up * 8 / seconds),
        "flows_per_second": round(flows / seconds, 2),
        "download_bytes": int(down),
        "upload_bytes": int(up),
    }

@app.get("/api/top")
def top_customers(minutes: int = Query(5, ge=1, le=1440), limit: int = Query(20, ge=1, le=100)):
    ch = client()
    dst_filter = customer_filter("DstAddr")
    src_filter = customer_filter("SrcAddr")
    sql = f"""
    WITH
    down AS (
      SELECT {v4("DstAddr")} AS ip, sum(Bytes * SamplingRate) AS bytes
      FROM flows
      WHERE TimeReceived >= now() - INTERVAL {minutes} MINUTE
        AND InIfBoundary = 'external' AND {dst_filter}
      GROUP BY ip
    ),
    up AS (
      SELECT {v4("SrcAddr")} AS ip, sum(Bytes * SamplingRate) AS bytes
      FROM flows
      WHERE TimeReceived >= now() - INTERVAL {minutes} MINUTE
        AND OutIfBoundary = 'external' AND {src_filter}
      GROUP BY ip
    )
    SELECT
      coalesce(down.ip, up.ip) AS ip,
      ifNull(down.bytes, 0) AS down_bytes,
      ifNull(up.bytes, 0) AS up_bytes
    FROM down
    FULL OUTER JOIN up ON down.ip = up.ip
    ORDER BY down_bytes + up_bytes DESC
    LIMIT {limit}
    """
    seconds = minutes * 60
    rows = ch.query(sql).result_rows
    return [{
        "ip": ip,
        "download_bps": int(down * 8 / seconds),
        "upload_bps": int(up * 8 / seconds),
        "download_bytes": int(down),
        "upload_bytes": int(up),
    } for ip, down, up in rows]

@app.get("/", response_class=HTMLResponse)
def index():
    return """<!doctype html>
<html lang="cs">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>AKGATE Dashboard</title>
<style>
:root{color-scheme:dark;font-family:Inter,system-ui,sans-serif;background:#0b1020;color:#e8eefc}
body{margin:0;padding:28px;max-width:1500px;margin:auto}
h1{margin:0 0 6px}.muted{color:#8ea0c0}.cards{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin:24px 0}
.card,.panel{background:#121a2d;border:1px solid #26324d;border-radius:16px;padding:18px}
.value{font-size:30px;font-weight:750;margin-top:8px}
table{width:100%;border-collapse:collapse}th,td{text-align:left;padding:11px;border-bottom:1px solid #26324d}th{color:#8ea0c0}
.down{color:#71d7ff}.up{color:#b8f28b}
@media(max-width:800px){.cards{grid-template-columns:1fr 1fr}}
</style>
</head>
<body>
<h1>AKGATE</h1><div class="muted">ISP traffic dashboard · live</div>
<div class="cards">
<div class="card"><div class="muted">↓ Download</div><div id="down" class="value down">—</div></div>
<div class="card"><div class="muted">↑ Upload</div><div id="up" class="value up">—</div></div>
<div class="card"><div class="muted">Flows/s</div><div id="flows" class="value">—</div></div>
<div class="card"><div class="muted">Data / 5 min</div><div id="data" class="value">—</div></div>
</div>
<div class="panel"><h2>Top zákazníci · posledních 5 minut</h2>
<table><thead><tr><th>IP</th><th>Download</th><th>Upload</th><th>Data</th></tr></thead><tbody id="top"></tbody></table></div>
<script>
const rate=n=>{const u=['b/s','Kb/s','Mb/s','Gb/s'];let i=0;while(n>=1000&&i<u.length-1){n/=1000;i++}return n.toFixed(n>=100?0:n>=10?1:2)+' '+u[i]}
const bytes=n=>{const u=['B','KB','MB','GB','TB'];let i=0;while(n>=1000&&i<u.length-1){n/=1000;i++}return n.toFixed(n>=100?0:n>=10?1:2)+' '+u[i]}
async function refresh(){
 const s=await fetch('/api/summary').then(r=>r.json());
 down.textContent=rate(s.download_bps);up.textContent=rate(s.upload_bps);flows.textContent=s.flows_per_second;data.textContent=bytes(s.download_bytes+s.upload_bytes);
 const rows=await fetch('/api/top').then(r=>r.json());
 top.innerHTML=rows.map(x=>`<tr><td>${x.ip}</td><td class="down">${rate(x.download_bps)}</td><td class="up">${rate(x.upload_bps)}</td><td>${bytes(x.download_bytes+x.upload_bytes)}</td></tr>`).join('');
}
refresh();setInterval(refresh,10000);
</script>
</body></html>"""
