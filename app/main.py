from ipaddress import ip_address, ip_network
import os

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse

from .db import client

app = FastAPI(title="AKGATE Dashboard", version="0.3.0")

CUSTOMER_NETWORKS = [
    ip_network(x.strip()) for x in os.getenv("CUSTOMER_NETWORKS", "").split(",") if x.strip()
]

RANGES = {
    "15m": (15, 10), "1h": (60, 30), "6h": (360, 180),
    "24h": (1440, 600), "7d": (10080, 3600), "15d": (21600, 7200),
}

def v4(expr: str) -> str:
    return f"replaceRegexpOne(toString({expr}), '^::ffff:', '')"

def customer_filter(expr: str) -> str:
    parts = [f"isIPAddressInRange({v4(expr)}, '{net.with_prefixlen}')" for net in CUSTOMER_NETWORKS]
    return "(" + " OR ".join(parts) + ")" if parts else "1"

def valid_ip(value: str) -> str:
    try:
        return str(ip_address(value))
    except ValueError:
        raise HTTPException(status_code=400, detail="Neplatná IP adresa")

def range_values(value: str):
    if value not in RANGES:
        raise HTTPException(status_code=400, detail="Neplatný časový rozsah")
    return RANGES[value]

@app.get("/health")
def health():
    try:
        return {"status": "ok", "clickhouse": client().query("SELECT 1").result_rows[0][0] == 1}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc))

@app.get("/api/live")
def live(seconds: int = Query(10, ge=5, le=60)):
    row = client().query(f"""
    SELECT
      sumIf(Bytes * SamplingRate, InIfBoundary = 'external' AND OutIfBoundary = 'internal'),
      sumIf(Bytes * SamplingRate, InIfBoundary = 'internal' AND OutIfBoundary = 'external'),
      count()
    FROM flows WHERE TimeReceived >= now() - INTERVAL {seconds} SECOND
    """).result_rows[0]
    return {"window_seconds": seconds, "download_bps": int(row[0]*8/seconds),
            "upload_bps": int(row[1]*8/seconds), "flows_per_second": round(row[2]/seconds,1)}

@app.get("/api/summary")
def summary(minutes: int = Query(5, ge=1, le=21600)):
    down, up, flows, packets = client().query(f"""
    SELECT
      sumIf(Bytes * SamplingRate, InIfBoundary='external' AND OutIfBoundary='internal'),
      sumIf(Bytes * SamplingRate, InIfBoundary='internal' AND OutIfBoundary='external'),
      count(), sum(Packets * SamplingRate)
    FROM flows WHERE TimeReceived >= now() - INTERVAL {minutes} MINUTE
    """).result_rows[0]
    seconds=minutes*60
    return {"window_minutes":minutes,"download_bps":int(down*8/seconds),"upload_bps":int(up*8/seconds),
            "flows_per_second":round(flows/seconds,2),"download_bytes":int(down),"upload_bytes":int(up),
            "packets":int(packets)}

@app.get("/api/history")
def history(range: str = Query("1h")):
    minutes,bucket=range_values(range)
    rows=client().query(f"""
    SELECT toUnixTimestamp(toStartOfInterval(TimeReceived, INTERVAL {bucket} SECOND)) t,
      sumIf(Bytes*SamplingRate,InIfBoundary='external' AND OutIfBoundary='internal')*8/{bucket} down,
      sumIf(Bytes*SamplingRate,InIfBoundary='internal' AND OutIfBoundary='external')*8/{bucket} up
    FROM flows WHERE TimeReceived>=now()-INTERVAL {minutes} MINUTE
    GROUP BY t ORDER BY t
    """).result_rows
    return [{"t":int(t),"down":int(d),"up":int(u)} for t,d,u in rows]

@app.get("/api/top")
def top_customers(minutes: int=Query(5,ge=1,le=21600), limit:int=Query(20,ge=1,le=100)):
    df,sf=customer_filter("DstAddr"),customer_filter("SrcAddr")
    rows=client().query(f"""
    WITH down AS (
      SELECT {v4("DstAddr")} ip,sum(Bytes*SamplingRate) bytes,count() flows
      FROM flows WHERE TimeReceived>=now()-INTERVAL {minutes} MINUTE
      AND InIfBoundary='external' AND OutIfBoundary='internal' AND {df} GROUP BY ip),
    up AS (
      SELECT {v4("SrcAddr")} ip,sum(Bytes*SamplingRate) bytes,count() flows
      FROM flows WHERE TimeReceived>=now()-INTERVAL {minutes} MINUTE
      AND InIfBoundary='internal' AND OutIfBoundary='external' AND {sf} GROUP BY ip)
    SELECT coalesce(down.ip,up.ip),ifNull(down.bytes,0),ifNull(up.bytes,0),
      ifNull(down.flows,0)+ifNull(up.flows,0)
    FROM down FULL OUTER JOIN up ON down.ip=up.ip
    ORDER BY down_bytes+up_bytes DESC LIMIT {limit}
    """).result_rows
    sec=minutes*60
    return [{"ip":ip,"download_bps":int(d*8/sec),"upload_bps":int(u*8/sec),
             "download_bytes":int(d),"upload_bytes":int(u),"flows_per_second":round(f/sec,2)}
            for ip,d,u,f in rows]

@app.get("/api/customer/{ip}/history")
def customer_history(ip:str, range:str=Query("1h")):
    ip=valid_ip(ip); minutes,bucket=range_values(range)
    rows=client().query(f"""
    SELECT toUnixTimestamp(toStartOfInterval(TimeReceived,INTERVAL {bucket} SECOND)) t,
      sumIf(Bytes*SamplingRate,InIfBoundary='external' AND OutIfBoundary='internal' AND {v4("DstAddr")}='{ip}')*8/{bucket} down,
      sumIf(Bytes*SamplingRate,InIfBoundary='internal' AND OutIfBoundary='external' AND {v4("SrcAddr")}='{ip}')*8/{bucket} up
    FROM flows WHERE TimeReceived>=now()-INTERVAL {minutes} MINUTE
    AND ({v4("SrcAddr")}='{ip}' OR {v4("DstAddr")}='{ip}')
    GROUP BY t ORDER BY t
    """).result_rows
    return [{"t":int(t),"down":int(d),"up":int(u)} for t,d,u in rows]

@app.get("/api/customer/{ip}")
def customer_detail(ip:str, range:str=Query("1h")):
    ip=valid_ip(ip); minutes,bucket=range_values(range); sec=minutes*60
    ch=client()
    stats=ch.query(f"""
    SELECT
      sumIf(Bytes*SamplingRate,InIfBoundary='external' AND OutIfBoundary='internal' AND {v4("DstAddr")}='{ip}'),
      sumIf(Bytes*SamplingRate,InIfBoundary='internal' AND OutIfBoundary='external' AND {v4("SrcAddr")}='{ip}'),
      sumIf(Packets*SamplingRate,{v4("SrcAddr")}='{ip}' OR {v4("DstAddr")}='{ip}'),count()
    FROM flows WHERE TimeReceived>=now()-INTERVAL {minutes} MINUTE
      AND ({v4("SrcAddr")}='{ip}' OR {v4("DstAddr")}='{ip}')
    """).result_rows[0]
    p95=ch.query(f"""
    WITH x AS (
      SELECT toStartOfInterval(TimeReceived,INTERVAL {bucket} SECOND) b,
       sumIf(Bytes*SamplingRate,InIfBoundary='external' AND {v4("DstAddr")}='{ip}')*8/{bucket} d,
       sumIf(Bytes*SamplingRate,OutIfBoundary='external' AND {v4("SrcAddr")}='{ip}')*8/{bucket} u
      FROM flows WHERE TimeReceived>=now()-INTERVAL {minutes} MINUTE
       AND ({v4("SrcAddr")}='{ip}' OR {v4("DstAddr")}='{ip}') GROUP BY b)
    SELECT quantile(0.95)(d),quantile(0.95)(u) FROM x
    """).result_rows[0]
    asns=ch.query(f"""
    SELECT DstAS,sum(Bytes*SamplingRate) b FROM flows
    WHERE TimeReceived>=now()-INTERVAL {minutes} MINUTE AND {v4("SrcAddr")}='{ip}'
      AND OutIfBoundary='external' GROUP BY DstAS ORDER BY b DESC LIMIT 8
    """).result_rows
    protos=ch.query(f"""
    SELECT Proto,sum(Bytes*SamplingRate) b,count() f FROM flows
    WHERE TimeReceived>=now()-INTERVAL {minutes} MINUTE AND ({v4("SrcAddr")}='{ip}' OR {v4("DstAddr")}='{ip}')
    GROUP BY Proto ORDER BY b DESC LIMIT 8
    """).result_rows
    ports=ch.query(f"""
    SELECT DstPort,sum(Bytes*SamplingRate) b FROM flows
    WHERE TimeReceived>=now()-INTERVAL {minutes} MINUTE AND {v4("SrcAddr")}='{ip}'
    GROUP BY DstPort ORDER BY b DESC LIMIT 10
    """).result_rows
    return {"ip":ip,"download_bytes":int(stats[0]),"upload_bytes":int(stats[1]),"packets":int(stats[2]),
      "flows":int(stats[3]),"avg_download_bps":int(stats[0]*8/sec),"avg_upload_bps":int(stats[1]*8/sec),
      "p95_download_bps":int(p95[0] or 0),"p95_upload_bps":int(p95[1] or 0),
      "top_asn":[{"asn":int(a),"bytes":int(b)} for a,b in asns],
      "protocols":[{"proto":int(p),"bytes":int(b),"flows":int(f)} for p,b,f in protos],
      "ports":[{"port":int(p),"bytes":int(b)} for p,b in ports]}

@app.get("/",response_class=HTMLResponse)
def index():
    return """<!doctype html><html lang="cs"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>AKGATE Dashboard</title><style>
:root{color-scheme:dark;font-family:Inter,system-ui,sans-serif;background:#0b1020;color:#e8eefc}*{box-sizing:border-box}
body{margin:0;padding:24px;max-width:1600px;margin:auto}header,.row,.toolbar{display:flex;align-items:center;justify-content:space-between;gap:14px;flex-wrap:wrap}
h1,h2,h3{margin:0}.muted{color:#8ea0c0}.live{font-weight:750;color:#77e29b}.dot{display:inline-block;width:9px;height:9px;border-radius:50%;background:#77e29b;margin-right:7px}
.cards{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin:20px 0}.card,.panel{background:#121a2d;border:1px solid #26324d;border-radius:15px;padding:17px}.panel{margin-top:12px}
.value{font-size:28px;font-weight:760;margin-top:7px}.down{color:#71d7ff}.up{color:#b8f28b}
button,input{font:inherit;color:inherit;background:#0d1425;border:1px solid #33415f;border-radius:9px;padding:9px 12px}button{cursor:pointer}button.active{background:#263a62;border-color:#5275b7}.ranges{display:flex;gap:6px;flex-wrap:wrap}
input{min-width:220px}.chart{height:300px;margin-top:12px;position:relative}.chart svg{width:100%;height:100%;overflow:visible}.grid{stroke:#26324d;stroke-width:1}.downline{fill:none;stroke:#71d7ff;stroke-width:2.5}.upline{fill:none;stroke:#b8f28b;stroke-width:2.5}
.legend{display:flex;gap:16px;font-size:13px}.tablewrap{overflow:auto}table{width:100%;border-collapse:collapse;min-width:700px}th,td{text-align:left;padding:10px;border-bottom:1px solid #26324d}th{color:#8ea0c0}tbody tr{cursor:pointer}tbody tr:hover{background:#18233b}
.detail{display:none}.detail.open{display:block}.detailgrid{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin:14px 0}.mini{background:#0d1425;border-radius:10px;padding:12px}.mini b{display:block;font-size:19px;margin-top:4px}.cols{display:grid;grid-template-columns:repeat(3,1fr);gap:12px}.list{background:#0d1425;border-radius:10px;padding:12px}.listrow{display:flex;justify-content:space-between;gap:12px;padding:6px 0;border-bottom:1px solid #202c45}.error{color:#ff9d9d}
@media(max-width:850px){body{padding:14px}.cards,.detailgrid{grid-template-columns:1fr 1fr}.cols{grid-template-columns:1fr}.value{font-size:22px}}
</style></head><body>
<header><div><h1>AKGATE</h1><div class="muted">ISP traffic dashboard</div></div><div id="liveState" class="live"><span class="dot"></span>LIVE · 1 s</div></header>
<div class="cards"><div class="card"><div class="muted">↓ Download · live</div><div id="down" class="value down">—</div></div>
<div class="card"><div class="muted">↑ Upload · live</div><div id="up" class="value up">—</div></div>
<div class="card"><div class="muted">Flows/s · live</div><div id="flows" class="value">—</div></div>
<div class="card"><div class="muted">Data · 5 min</div><div id="data" class="value">—</div></div></div>
<div class="panel"><div class="toolbar"><div><h2>Provoz</h2><div class="muted">DOWN / UP podle zvoleného období</div></div>
<div class="ranges" id="ranges"><button data-r="15m">15 min</button><button class="active" data-r="1h">1 h</button><button data-r="6h">6 h</button><button data-r="24h">24 h</button><button data-r="7d">7 dní</button><button data-r="15d">15 dní</button></div></div>
<div class="legend"><span class="down">● Download</span><span class="up">● Upload</span></div><div id="historyChart" class="chart"></div></div>
<div class="panel"><div class="toolbar"><div><h2>Top zákazníci</h2><div class="muted" id="topLabel">zvolené období</div></div><input id="search" placeholder="Hledat IP adresu…"></div>
<div class="tablewrap"><table><thead><tr><th>IP</th><th>Download Ø</th><th>Upload Ø</th><th>Flows/s</th><th>Data</th></tr></thead><tbody id="top"></tbody></table></div></div>
<div id="detail" class="panel detail"><div class="row"><div><h2 id="detailTitle">Detail zákazníka</h2><div class="muted" id="detailRange"></div></div><button id="closeDetail">Zavřít</button></div>
<div class="detailgrid"><div class="mini">↓ Průměr<b id="dAvg" class="down">—</b></div><div class="mini">↑ Průměr<b id="uAvg" class="up">—</b></div><div class="mini">↓ 95. percentil<b id="d95">—</b></div><div class="mini">↑ 95. percentil<b id="u95">—</b></div>
<div class="mini">Data celkem<b id="dData">—</b></div><div class="mini">Flows<b id="dFlows">—</b></div><div class="mini">Pakety<b id="dPackets">—</b></div><div class="mini">IP<b id="dIp">—</b></div></div>
<div id="customerChart" class="chart"></div><div class="cols"><div class="list"><h3>Top ASN</h3><div id="asn"></div></div><div class="list"><h3>Protokoly</h3><div id="proto"></div></div><div class="list"><h3>Cílové porty</h3><div id="ports"></div></div></div></div>
<script>
const getEl=id=>document.getElementById(id),formatRate=n=>{const u=['b/s','Kb/s','Mb/s','Gb/s'];let i=0;n=Number(n)||0;while(n>=1000&&i<3){n/=1000;i++}return n.toFixed(n>=100?0:n>=10?1:2)+' '+u[i]},bytes=n=>{const u=['B','KB','MB','GB','TB'];let i=0;n=Number(n)||0;while(n>=1000&&i<4){n/=1000;i++}return n.toFixed(n>=100?0:n>=10?1:2)+' '+u[i]};
const formatBytes=bytes;
const protoName=p=>({1:'ICMP',6:'TCP',17:'UDP',47:'GRE',50:'ESP',58:'ICMPv6'}[p]||('IP '+p));let range='1h',selected=null,topRows=[];
function draw(el,pts){if(!pts.length){el.innerHTML='<div class="muted">Bez dat</div>';return}const w=1000,h=270,p=20,max=Math.max(1,...pts.flatMap(x=>[x.down,x.up]));const path=k=>pts.map((x,i)=>{const X=p+i/Math.max(1,pts.length-1)*(w-2*p),Y=h-p-x[k]/max*(h-2*p);return(i?'L':'M')+X.toFixed(1)+' '+Y.toFixed(1)}).join(' ');el.innerHTML='<svg viewBox="0 0 '+w+' '+h+'" preserveAspectRatio="none"><line class="grid" x1="'+p+'" y1="'+(h/2)+'" x2="'+(w-p)+'" y2="'+(h/2)+'"/><line class="grid" x1="'+p+'" y1="'+(h-p)+'" x2="'+(w-p)+'" y2="'+(h-p)+'"/><path class="downline" d="'+path('down')+'"/><path class="upline" d="'+path('up')+'"/></svg>'}
async function refreshLive(){try{const s=await fetch('/api/live?seconds=10',{cache:'no-store'}).then(r=>{if(!r.ok)throw Error();return r.json()});getEl('down').textContent=formatRate(s.download_bps);getEl('up').textContent=formatRate(s.upload_bps);getEl('flows').textContent=s.flows_per_second;getEl('liveState').innerHTML='<span class="dot"></span>LIVE · 1 s'}catch(e){getEl('liveState').textContent='● OFFLINE'}}
async function refreshSummary(){try{const s=await fetch('/api/summary?minutes=5',{cache:'no-store'}).then(r=>r.json());getEl('data').textContent=bytes(s.download_bytes+s.upload_bytes)}catch(e){}}
async function loadHistory(){try{const x=await fetch('/api/history?range='+range,{cache:'no-store'}).then(r=>r.json());draw(getEl('historyChart'),x)}catch(e){getEl('historyChart').innerHTML='<div class="error">Chyba načtení grafu</div>'}}
function renderTop(){const q=getEl('search').value.trim();const rows=q?topRows.filter(x=>x.ip.includes(q)):topRows;getEl('top').innerHTML=rows.map(x=>'<tr data-ip="'+x.ip+'"><td>'+x.ip+'</td><td class="down">'+formatRate(x.download_bps)+'</td><td class="up">'+formatRate(x.upload_bps)+'</td><td>'+x.flows_per_second+'</td><td>'+bytes(x.download_bytes+x.upload_bytes)+'</td></tr>').join('');getEl('top').querySelectorAll('tr').forEach(tr=>tr.addEventListener('click',()=>detail(tr.dataset.ip)))}
async function loadTop(){const mins={15m:15,'1h':60,'6h':360,'24h':1440,'7d':10080,'15d':21600}[range];try{topRows=await fetch('/api/top?minutes='+mins+'&limit=50',{cache:'no-store'}).then(r=>r.json());renderTop()}catch(e){getEl('top').innerHTML='<tr><td colspan="5" class="error">Chyba načtení</td></tr>'}}
const rows=(id,a,fn)=>E(id).innerHTML=a.length?a.map(x=>'<div class="listrow"><span>'+fn(x)[0]+'</span><b>'+fn(x)[1]+'</b></div>').join(''):'<div class="muted">Bez dat</div>';
async function detail(ip){selected=ip;getEl('detail').classList.add('open');getEl('detailTitle').textContent='Zákazník '+ip;getEl('detailRange').textContent='Období: '+range;getEl('detail').scrollIntoView({behavior:'smooth',block:'start'});try{const [d,h]=await Promise.all([fetch('/api/customer/'+encodeURIComponent(ip)+'?range='+range).then(r=>r.json()),fetch('/api/customer/'+encodeURIComponent(ip)+'/history?range='+range).then(r=>r.json())]);getEl('dAvg').textContent=formatRate(d.avg_download_bps);getEl('uAvg').textContent=formatRate(d.avg_upload_bps);getEl('d95').textContent=formatRate(d.p95_download_bps);getEl('u95').textContent=formatRate(d.p95_upload_bps);getEl('dData').textContent=bytes(d.download_bytes+d.upload_bytes);getEl('dFlows').textContent=Number(d.flows).toLocaleString('cs-CZ');getEl('dPackets').textContent=Number(d.packets).toLocaleString('cs-CZ');getEl('dIp').textContent=d.ip;draw(getEl('customerChart'),h);rows('asn',d.top_asn,x=>['AS'+x.asn,bytes(x.bytes)]);rows('proto',d.protocols,x=>[protoName(x.proto),bytes(x.bytes)]);rows('ports',d.ports,x=>[x.port,bytes(x.bytes)])}catch(e){getEl('detailRange').textContent='Chyba načtení detailu'}}
getEl('ranges').querySelectorAll('button').forEach(b=>b.addEventListener('click',()=>{range=b.dataset.r;getEl('ranges').querySelectorAll('button').forEach(x=>x.classList.toggle('active',x===b));loadHistory();loadTop();if(selected)detail(selected)}));getEl('search').addEventListener('input',renderTop);getEl('search').addEventListener('keydown',e=>{if(e.key==='Enter'&&getEl('search').value.trim())detail(getEl('search').value.trim())});getEl('closeDetail').addEventListener('click',()=>{selected=null;getEl('detail').classList.remove('open')});
refreshLive();refreshSummary();loadHistory();loadTop();setInterval(refreshLive,1000);setInterval(refreshSummary,5000);setInterval(()=>{loadTop();if(selected)detail(selected)},10000);setInterval(loadHistory,30000);
</script></body></html>"""
