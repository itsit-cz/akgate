from ipaddress import ip_address, ip_network
import os
import sqlite3
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Body
from fastapi.responses import FileResponse

from .db import client

app = FastAPI(title="AKGATE Dashboard", version="0.5.2")

CUSTOMER_NETWORKS = [
    ip_network(x.strip()) for x in os.getenv("CUSTOMER_NETWORKS", "").split(",") if x.strip()
]

RANGES = {
    "1m": (1, 2), "5m": (5, 5), "15m": (15, 10), "1h": (60, 30), "6h": (360, 180),
    "24h": (1440, 600), "48h": (2880, 1200), "7d": (10080, 3600),
    "15d": (21600, 7200), "30d": (43200, 14400), "1y": (525600, 86400),
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

STATIC_DIR = Path(__file__).parent / "static"
NOTES_DB = os.getenv("NOTES_DB", "/data/akgate.db")

def notes_db():
    os.makedirs(os.path.dirname(NOTES_DB), exist_ok=True)
    db = sqlite3.connect(NOTES_DB)
    db.execute("CREATE TABLE IF NOT EXISTS notes (ip TEXT PRIMARY KEY, note TEXT NOT NULL DEFAULT '', updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)")
    return db

@app.get("/api/notes")
def notes():
    with notes_db() as db:
        return {ip: note for ip, note in db.execute("SELECT ip,note FROM notes WHERE note <> ''")}

@app.put("/api/notes/{ip}")
def save_note(ip: str, payload: dict = Body(...)):
    ip = valid_ip(ip)
    note = str(payload.get("note", "")).strip()[:2000]
    with notes_db() as db:
        if note:
            db.execute("INSERT INTO notes(ip,note,updated_at) VALUES(?,?,CURRENT_TIMESTAMP) ON CONFLICT(ip) DO UPDATE SET note=excluded.note,updated_at=CURRENT_TIMESTAMP", (ip,note))
        else:
            db.execute("DELETE FROM notes WHERE ip=?", (ip,))
        db.commit()
    return {"ip": ip, "note": note}

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
    SELECT coalesce(down.ip,up.ip) AS ip,
      ifNull(down.bytes,0) AS down_bytes,
      ifNull(up.bytes,0) AS up_bytes,
      ifNull(down.flows,0)+ifNull(up.flows,0) AS flow_count
    FROM down FULL OUTER JOIN up ON down.ip=up.ip
    ORDER BY down_bytes+up_bytes DESC LIMIT {limit}
    """).result_rows
    sec=minutes*60
    return [{"ip":ip,"download_bps":int(d*8/sec),"upload_bps":int(u*8/sec),
             "download_bytes":int(d),"upload_bytes":int(u),"flows_per_second":round(f/sec,2)}
            for ip,d,u,f in rows]


@app.get("/api/statistics")
def statistics(range: str = Query("1h")):
    minutes, bucket = range_values(range)
    ch = client()
    df, sf = customer_filter("DstAddr"), customer_filter("SrcAddr")
    series = ch.query(f"""
    SELECT
      toUnixTimestamp(toStartOfInterval(TimeReceived, INTERVAL {bucket} SECOND)) t,
      sumIf(Bytes*SamplingRate, InIfBoundary='external' AND OutIfBoundary='internal')*8/{bucket} down,
      sumIf(Bytes*SamplingRate, InIfBoundary='internal' AND OutIfBoundary='external')*8/{bucket} up,
      count()/{bucket} fps
    FROM flows
    WHERE TimeReceived >= now()-INTERVAL {minutes} MINUTE
    GROUP BY t ORDER BY t
    """).result_rows
    down_values = [float(r[1]) for r in series]
    up_values = [float(r[2]) for r in series]
    flow_values = [float(r[3]) for r in series]
    totals = ch.query(f"""
    SELECT
      sumIf(Bytes*SamplingRate, InIfBoundary='external' AND OutIfBoundary='internal'),
      sumIf(Bytes*SamplingRate, InIfBoundary='internal' AND OutIfBoundary='external')
    FROM flows WHERE TimeReceived >= now()-INTERVAL {minutes} MINUTE
    """).result_rows[0]
    top = ch.query(f"""
    WITH down AS (
      SELECT {v4("DstAddr")} ip, sum(Bytes*SamplingRate) bytes, count() flows
      FROM flows WHERE TimeReceived>=now()-INTERVAL {minutes} MINUTE
      AND InIfBoundary='external' AND OutIfBoundary='internal' AND {df} GROUP BY ip),
    up AS (
      SELECT {v4("SrcAddr")} ip, sum(Bytes*SamplingRate) bytes, count() flows
      FROM flows WHERE TimeReceived>=now()-INTERVAL {minutes} MINUTE
      AND InIfBoundary='internal' AND OutIfBoundary='external' AND {sf} GROUP BY ip)
    SELECT coalesce(down.ip,up.ip) ip, ifNull(down.bytes,0) db, ifNull(up.bytes,0) ub,
           ifNull(down.flows,0)+ifNull(up.flows,0) fc
    FROM down FULL OUTER JOIN up ON down.ip=up.ip
    ORDER BY db+ub DESC LIMIT 100
    """).result_rows
    sec = minutes * 60
    def mmav(values):
        return {"min": int(min(values) if values else 0),
                "avg": int(sum(values)/len(values) if values else 0),
                "max": int(max(values) if values else 0)}
    return {
      "range": range, "download_bytes": int(totals[0]), "upload_bytes": int(totals[1]),
      "download_bps": mmav(down_values), "upload_bps": mmav(up_values),
      "flows_per_second": mmav(flow_values),
      "series": [{"t":int(t),"down":int(d),"up":int(u),"flows":round(float(fp),2)} for t,d,u,fp in series],
      "top": [{"ip":ip,"download_bytes":int(d),"upload_bytes":int(u),
               "download_bps":int(d*8/sec),"upload_bps":int(u*8/sec),
               "flows_per_second":round(fc/sec,2)} for ip,d,u,fc in top]
    }


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


@app.get("/static/{filename}")
def static_file(filename: str):
    if filename not in {"app.js", "style.css"}:
        raise HTTPException(status_code=404)
    return FileResponse(STATIC_DIR / filename, headers={"Cache-Control": "no-store"})

@app.get("/")
def index():
    return FileResponse(STATIC_DIR / "index.html", headers={"Cache-Control": "no-store"})
