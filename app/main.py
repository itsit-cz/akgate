from ipaddress import ip_network
import os

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse

from .db import client

app = FastAPI(title="AKGATE Dashboard", version="0.2.0")

CUSTOMER_NETWORKS = [
    ip_network(x.strip())
    for x in os.getenv("CUSTOMER_NETWORKS", "").split(",")
    if x.strip()
]

def v4(expr: str) -> str:
    return f"replaceRegexpOne(toString({expr}), '^::ffff:', '')"

def customer_filter(expr: str) -> str:
    parts = [f"isIPAddressInRange({v4(expr)}, '{net.with_prefixlen}')" for net in CUSTOMER_NETWORKS]
    return "(" + " OR ".join(parts) + ")" if parts else "1"

@app.get("/health")
def health():
    try:
        value = client().query("SELECT 1").result_rows[0][0]
        return {"status": "ok", "clickhouse": value == 1}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc))

@app.get("/api/live")
def live(seconds: int = Query(10, ge=5, le=60)):
    ch = client()
    sql = f"""
    SELECT
      sumIf(Bytes * SamplingRate, InIfBoundary = 'external') AS down_bytes,
      sumIf(Bytes * SamplingRate, OutIfBoundary = 'external') AS up_bytes,
      count() AS flows
    FROM flows
    WHERE TimeReceived >= now() - INTERVAL {seconds} SECOND
    """
    down, up, flows = ch.query(sql).result_rows[0]
    return {
        "window_seconds": seconds,
        "download_bps": int(down * 8 / seconds),
        "upload_bps": int(up * 8 / seconds),
        "flows_per_second": round(flows / seconds, 1),
    }

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
    SELECT coalesce(down.ip, up.ip) AS ip,
      ifNull(down.bytes, 0) AS down_bytes,
      ifNull(up.bytes, 0) AS up_bytes
    FROM down FULL OUTER JOIN up ON down.ip = up.ip
    ORDER BY down_bytes + up_bytes DESC LIMIT {limit}
    """
    seconds = minutes * 60
    return [{
        "ip": ip,
        "download_bps": int(down * 8 / seconds),
        "upload_bps": int(up * 8 / seconds),
        "download_bytes": int(down),
        "upload_bytes": int(up),
    } for ip, down, up in ch.query(sql).result_rows]

@app.get("/", response_class=HTMLResponse)
def index():
    return """<!doctype html>
<html lang="cs"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>AKGATE Dashboard</title>
<style>
:root{color-scheme:dark;font-family:Inter,system-ui,sans-serif;background:#0b1020;color:#e8eefc}
body{margin:0;padding:28px;max-width:1500px;margin:auto}header{display:flex;justify-content:space-between;align-items:center;gap:16px}
h1{margin:0 0 6px}.muted{color:#8ea0c0}.live{font-weight:700;color:#77e29b}.dot{display:inline-block;width:9px;height:9px;border-radius:50%;background:#77e29b;margin-right:7px}
.cards{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin:24px 0}.card,.panel{background:#121a2d;border:1px solid #26324d;border-radius:16px;padding:18px}
.value{font-size:30px;font-weight:750;margin-top:8px}.down{color:#71d7ff}.up{color:#b8f28b}
.chart{height:260px;position:relative;margin-top:14px}.chart svg{width:100%;height:100%;overflow:visible}.grid{stroke:#26324d;stroke-width:1}.downline{fill:none;stroke:#71d7ff;stroke-width:3}.upline{fill:none;stroke:#b8f28b;stroke-width:3}
.legend{display:flex;gap:20px;color:#8ea0c0;font-size:13px}.legend b{font-weight:700}
table{width:100%;border-collapse:collapse}th,td{text-align:left;padding:11px;border-bottom:1px solid #26324d}th{color:#8ea0c0}
@media(max-width:800px){body{padding:16px}.cards{grid-template-columns:1fr 1fr}.value{font-size:23px}}
</style></head><body>
<header><div><h1>AKGATE</h1><div class="muted">ISP traffic dashboard</div></div><div id="liveState" class="live"><span class="dot"></span>LIVE · 1 s</div></header>
<div class="cards">
<div class="card"><div class="muted">↓ Download · live</div><div id="down" class="value down">—</div></div>
<div class="card"><div class="muted">↑ Upload · live</div><div id="up" class="value up">—</div></div>
<div class="card"><div class="muted">Flows/s · live</div><div id="flows" class="value">—</div></div>
<div class="card"><div class="muted">Data · posledních 5 min</div><div id="data" class="value">—</div></div>
</div>
<div class="panel"><div style="display:flex;justify-content:space-between;gap:16px;flex-wrap:wrap"><div><h2 style="margin:0">Live provoz</h2><div class="muted">10sekundový klouzavý průměr · 60 posledních bodů</div></div><div class="legend"><span class="down"><b>●</b> Download</span><span class="up"><b>●</b> Upload</span></div></div><div id="chart" class="chart"></div></div>
<div class="panel" style="margin-top:14px"><h2>Top zákazníci · posledních 5 minut</h2><table><thead><tr><th>IP</th><th>Download</th><th>Upload</th><th>Data</th></tr></thead><tbody id="top"></tbody></table></div>
<script>
const byId=id=>document.getElementById(id);
const downEl=byId('down');
const upEl=byId('up');
const flowsEl=byId('flows');
const dataEl=byId('data');
const topEl=byId('top');
const chartEl=byId('chart');
const liveStateEl=byId('liveState');
const points=[]; let liveBusy=false,topBusy=false,summaryBusy=false;
const rate=n=>{const u=['b/s','Kb/s','Mb/s','Gb/s'];let i=0;while(n>=1000&&i<u.length-1){n/=1000;i++}return n.toFixed(n>=100?0:n>=10?1:2)+' '+u[i]}
const bytes=n=>{const u=['B','KB','MB','GB','TB'];let i=0;while(n>=1000&&i<u.length-1){n/=1000;i++}return n.toFixed(n>=100?0:n>=10?1:2)+' '+u[i]}
function draw(){
 if(!points.length)return;
 const w=1000,h=240,p=18,max=Math.max(1,...points.flatMap(x=>[x.d,x.u]));
 const path=k=>points.map((x,i)=>{const px=p+(i/Math.max(1,points.length-1))*(w-2*p),py=h-p-(x[k]/max)*(h-2*p);return (i?'L':'M')+px.toFixed(1)+' '+py.toFixed(1)}).join(' ');
 chartEl.innerHTML='<svg viewBox="0 0 '+w+' '+h+'" preserveAspectRatio="none"><line class="grid" x1="'+p+'" y1="'+(h/2)+'" x2="'+(w-p)+'" y2="'+(h/2)+'"/><line class="grid" x1="'+p+'" y1="'+(h-p)+'" x2="'+(w-p)+'" y2="'+(h-p)+'"/><path class="downline" d="'+path('d')+'"/><path class="upline" d="'+path('u')+'"/></svg>';
}
async function refreshLive(){
 if(liveBusy)return; liveBusy=true;
 try{const r=await fetch('/api/live?seconds=10',{cache:'no-store'});if(!r.ok)throw Error(r.status);const s=await r.json();downEl.textContent=rate(s.download_bps);upEl.textContent=rate(s.upload_bps);flowsEl.textContent=s.flows_per_second;points.push({d:s.download_bps,u:s.upload_bps});if(points.length>60)points.shift();draw();liveStateEl.innerHTML='<span class="dot"></span>LIVE · 1 s';}
 catch(e){liveStateEl.textContent='● OFFLINE';}
 finally{liveBusy=false;}
}
async function refreshSummary(){if(summaryBusy)return;summaryBusy=true;try{const s=await fetch('/api/summary?minutes=5',{cache:'no-store'}).then(r=>r.json());dataEl.textContent=bytes(s.download_bytes+s.upload_bytes)}finally{summaryBusy=false}}
async function refreshTop(){if(topBusy)return;topBusy=true;try{const rows=await fetch('/api/top?minutes=5&limit=20',{cache:'no-store'}).then(r=>r.json());topEl.innerHTML=rows.map(x=>'<tr><td>'+x.ip+'</td><td class="down">'+rate(x.download_bps)+'</td><td class="up">'+rate(x.upload_bps)+'</td><td>'+bytes(x.download_bytes+x.upload_bytes)+'</td></tr>').join('')}finally{topBusy=false}}
refreshLive();refreshSummary();refreshTop();setInterval(refreshLive,1000);setInterval(refreshTop,5000);setInterval(refreshSummary,5000);
</script></body></html>"""
