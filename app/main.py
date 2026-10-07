from ipaddress import ip_address, ip_network
import os
import sqlite3
import time
import threading
import routeros_api
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Body
from fastapi.responses import FileResponse

from .db import client

app = FastAPI(title="AKGATE Dashboard", version="0.7.9")

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
STATS_CACHE = {}
STATS_CACHE_TTL = 30
ROUTER_LOCK = threading.Lock()
ROUTER_POOL = None
ROUTER_API = None

def router_api():
    global ROUTER_POOL, ROUTER_API
    if ROUTER_API is None:
        ROUTER_POOL = routeros_api.RouterOsApiPool(os.getenv("MIKROTIK_HOST", ""), username=os.getenv("MIKROTIK_USER", ""), password=os.getenv("MIKROTIK_PASSWORD", ""), port=int(os.getenv("MIKROTIK_PORT", "8728")), plaintext_login=True, use_ssl=False)
        ROUTER_API = ROUTER_POOL.get_api()
    return ROUTER_API

def queue_pair(value):
    try:
        a, b = str(value or "0/0").split("/", 1)
        return int(a), int(b)
    except (ValueError, TypeError):
        return 0, 0

def is_customer_target(value):
    try:
        addr = ip_address(str(value).split("/", 1)[0])
        return not CUSTOMER_NETWORKS or any(addr in net for net in CUSTOMER_NETWORKS)
    except ValueError:
        return False

def notes_db():
    os.makedirs(os.path.dirname(NOTES_DB), exist_ok=True)
    db = sqlite3.connect(NOTES_DB)
    db.execute("CREATE TABLE IF NOT EXISTS notes (ip TEXT PRIMARY KEY, note TEXT NOT NULL DEFAULT '', updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)")
    db.execute("CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
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

@app.get("/api/settings")
def get_settings():
    defaults = {"wan_download_mbps":"500","wan_upload_mbps":"500","warn_percent":"85","client_warn_percent":"90","active_bps":"1000"}
    with notes_db() as db:
        defaults.update({k:v for k,v in db.execute("SELECT key,value FROM settings")})
    defaults["wan_interface"] = os.getenv("MIKROTIK_WAN_INTERFACE", "ether1")
    defaults["customer_networks"] = ",".join(str(n) for n in CUSTOMER_NETWORKS)
    return defaults

@app.put("/api/settings")
def save_settings(payload: dict = Body(...)):
    allowed={"wan_download_mbps","wan_upload_mbps","warn_percent","client_warn_percent","active_bps"}
    with notes_db() as db:
        for k in allowed:
            if k in payload:
                try: v=str(max(0,float(payload[k])))
                except (ValueError,TypeError): raise HTTPException(status_code=400,detail=f"Neplatná hodnota {k}")
                db.execute("INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",(k,v))
        db.commit()
    return get_settings()

@app.get("/health")
def health():
    try:
        return {"status": "ok", "clickhouse": client().query("SELECT 1").result_rows[0][0] == 1}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc))

@app.get("/api/live")
def live(seconds: int = Query(30, ge=10, le=120)):
    # NetFlow is asynchronous/batched. Use the latest completed TimeReceived window
    # instead of now(), so the rate is not artificially low between exporter batches.
    row = client().query("""
    WITH latest AS (SELECT max(TimeReceived) AS t FROM flows)
    SELECT
      max(TimeReceived),
      sumIf(Bytes * SamplingRate, InIfBoundary = 'external' AND OutIfBoundary = 'internal'),
      sumIf(Bytes * SamplingRate, InIfBoundary = 'internal' AND OutIfBoundary = 'external'),
      count()
    FROM flows
    WHERE TimeReceived > (SELECT t FROM latest) - INTERVAL %(seconds)s SECOND
      AND TimeReceived <= (SELECT t FROM latest)
    """, parameters={"seconds": seconds}).result_rows[0]
    return {"window_seconds": seconds, "sample_time": row[0].isoformat() if row[0] else None,
            "download_bps": int(row[1]*8/seconds), "upload_bps": int(row[2]*8/seconds),
            "flows_per_second": round(row[3]/seconds,1)}

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
    now = time.monotonic()
    cached = STATS_CACHE.get(range)
    if cached and now - cached[0] < STATS_CACHE_TTL:
        return cached[1]

    ch = client()
    df, sf = customer_filter("DstAddr"), customer_filter("SrcAddr")
    series = ch.query(f"""
    SELECT
      toUnixTimestamp(toStartOfInterval(TimeReceived, INTERVAL {bucket} SECOND)) t,
      sumIf(Bytes*SamplingRate, InIfBoundary='external' AND OutIfBoundary='internal') down_bytes,
      sumIf(Bytes*SamplingRate, InIfBoundary='internal' AND OutIfBoundary='external') up_bytes,
      count() flows
    FROM flows
    PREWHERE TimeReceived >= now()-INTERVAL {minutes} MINUTE
    GROUP BY t ORDER BY t
    """).result_rows

    # Autorita pro skutečnou WAN rychlost je MikroTik ether1.
    # Akvorado/NetFlow zůstává jen pro flows, klienty, ASN, protokoly a porty.
    wan_name = os.getenv("MIKROTIK_WAN_INTERFACE", "ether1")
    wan_stats = ch.query("""
    SELECT
      min(rx_bps), avg(rx_bps), max(rx_bps),
      min(tx_bps), avg(tx_bps), max(tx_bps)
    FROM interface_stats
    WHERE ts >= now()-INTERVAL %(minutes)s MINUTE AND interface = %(iface)s
    """, parameters={"minutes": minutes, "iface": wan_name}).result_rows[0]

    # NetFlow = offered/requested traffic. Keep a fixed 5s window for comparable MIN/AVG/MAX.
    offered_bucket = 5
    offered_series = ch.query(f"""
    SELECT
      sumIf(Bytes*SamplingRate, InIfBoundary='external' AND OutIfBoundary='internal')*8/{offered_bucket} down_bps,
      sumIf(Bytes*SamplingRate, InIfBoundary='internal' AND OutIfBoundary='external')*8/{offered_bucket} up_bps
    FROM flows
    PREWHERE TimeReceived >= now()-INTERVAL {minutes} MINUTE
    GROUP BY toStartOfInterval(TimeReceived, INTERVAL {offered_bucket} SECOND)
    """).result_rows
    offered_down_values = [float(r[0]) for r in offered_series]
    offered_up_values = [float(r[1]) for r in offered_series]

    down_values = [float(r[1])*8/bucket for r in series]
    up_values = [float(r[2])*8/bucket for r in series]
    flow_values = [float(r[3])/bucket for r in series]
    total_down = sum(int(r[1]) for r in series)
    total_up = sum(int(r[2]) for r in series)

    top = ch.query(f"""
    SELECT
      if(InIfBoundary='external' AND OutIfBoundary='internal', {v4("DstAddr")}, {v4("SrcAddr")}) ip,
      sumIf(Bytes*SamplingRate, InIfBoundary='external' AND OutIfBoundary='internal') db,
      sumIf(Bytes*SamplingRate, InIfBoundary='internal' AND OutIfBoundary='external') ub,
      count() fc
    FROM flows
    PREWHERE TimeReceived >= now()-INTERVAL {minutes} MINUTE
    WHERE (
      (InIfBoundary='external' AND OutIfBoundary='internal' AND {df})
      OR
      (InIfBoundary='internal' AND OutIfBoundary='external' AND {sf})
    )
    GROUP BY ip
    ORDER BY db+ub DESC LIMIT 100
    """).result_rows

    sec = minutes * 60
    def mmav(values):
        return {"min": int(min(values) if values else 0),
                "avg": int(sum(values)/len(values) if values else 0),
                "max": int(max(values) if values else 0)}

    result = {
      "range": range, "download_bytes": total_down, "upload_bytes": total_up,
      "download_bps": {"min": int(wan_stats[0] or 0), "avg": int(wan_stats[1] or 0), "max": int(wan_stats[2] or 0)},
      "upload_bps": {"min": int(wan_stats[3] or 0), "avg": int(wan_stats[4] or 0), "max": int(wan_stats[5] or 0)},
      "offered_download_bps": mmav(offered_down_values),
      "offered_upload_bps": mmav(offered_up_values),
      "flows_per_second": mmav(flow_values),
      "series": [{"t":int(t),"down":int(d*8/bucket),"up":int(u*8/bucket),"flows":round(float(fc)/bucket,2)}
                 for t,d,u,fc in series],
      "top": [{"ip":ip,"download_bytes":int(d),"upload_bytes":int(u),
               "download_bps":int(d*8/sec),"upload_bps":int(u*8/sec),
               "flows_per_second":round(fc/sec,2)} for ip,d,u,fc in top]
    }
    STATS_CACHE[range] = (now, result)
    return result


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


def queue_table_ready():
    try:
        return client().query("EXISTS TABLE queue_stats").result_rows[0][0] == 1
    except Exception:
        return False

@app.get("/api/queues")
def queues():
    if not queue_table_ready():
        return []
    rows = client().query("""
    SELECT
      queue_id,
      argMax(queue_name, ts) queue_name,
      argMax(targets, ts) targets,
      argMax(upload_bps, ts) upload_bps,
      argMax(download_bps, ts) download_bps,
      argMax(upload_bytes, ts) upload_bytes,
      argMax(download_bytes, ts) download_bytes,
      argMax(upload_packets, ts) upload_packets,
      argMax(download_packets, ts) download_packets,
      argMax(upload_dropped, ts) upload_dropped,
      argMax(download_dropped, ts) download_dropped,
      argMax(upload_limit, ts) upload_limit,
      argMax(download_limit, ts) download_limit,
      argMax(comment, ts) comment,
      max(ts) last_seen
    FROM queue_stats
    WHERE ts >= now() - INTERVAL 2 MINUTE
    GROUP BY queue_id
    ORDER BY queue_name
    """).result_rows
    return [{
      "queue_id": qid, "name": name, "targets": targets,
      "upload_bps": int(ubps), "download_bps": int(dbps),
      "upload_bytes": int(ubytes), "download_bytes": int(dbytes),
      "upload_packets": int(upackets), "download_packets": int(dpackets),
      "upload_dropped": int(udrop), "download_dropped": int(ddrop),
      "upload_limit": int(ulimit), "download_limit": int(dlimit),
      "comment": comment, "last_seen": seen.isoformat()
    } for qid,name,targets,ubps,dbps,ubytes,dbytes,upackets,dpackets,udrop,ddrop,ulimit,dlimit,comment,seen in rows]

@app.get("/api/interface/live")
def interface_live():
    global ROUTER_API, ROUTER_POOL
    try:
        wan_name = os.getenv("MIKROTIK_WAN_INTERFACE", "ether1")
        with ROUTER_LOCK:
            api = router_api()
            monitor = api.get_resource("/interface").call(
                "monitor-traffic", {"interface": wan_name, "once": ""}
            )
        if not monitor:
            raise RuntimeError(f"WAN interface {wan_name} monitor returned no data")
        iface = monitor[0]
        return {
            "available": True,
            "download_bps": int(iface.get("rx-bits-per-second", 0) or 0),
            "upload_bps": int(iface.get("tx-bits-per-second", 0) or 0),
            "interface": wan_name,
            "rx_pps": int(iface.get("rx-packets-per-second", 0) or 0),
            "tx_pps": int(iface.get("tx-packets-per-second", 0) or 0),
            "tx_drops_pps": int(iface.get("tx-queue-drops-per-second", 0) or 0),
            "t": int(time.time())
        }
    except Exception as exc:
        try:
            if ROUTER_POOL: ROUTER_POOL.disconnect()
        except Exception:
            pass
        ROUTER_POOL = ROUTER_API = None
        raise HTTPException(status_code=503, detail=str(exc))

@app.get("/api/queue/live")
def queue_live():
    global ROUTER_API, ROUTER_POOL
    try:
        with ROUTER_LOCK:
            api = router_api()
            queues = api.get_resource("/queue/simple").get()
        live_queues = []
        for q in queues:
            if q.get("disabled") == "true" or q.get("dynamic") == "true":
                continue
            raw_targets = [x.strip() for x in str(q.get("target", "")).split(",") if x.strip()]
            customer_targets = [x.split("/", 1)[0] for x in raw_targets if is_customer_target(x)]
            if not customer_targets:
                continue
            upload, download = queue_pair(q.get("rate"))
            live_queues.append({"name": str(q.get("name", "")), "targets": customer_targets,
                                "upload_bps": upload, "download_bps": download})
        return {"available": True, "queue_data": live_queues, "t": int(time.time())}
    except Exception as exc:
        try:
            if ROUTER_POOL: ROUTER_POOL.disconnect()
        except Exception:
            pass
        ROUTER_POOL = ROUTER_API = None
        raise HTTPException(status_code=503, detail=str(exc))

@app.get("/api/interface/history")
def interface_history(range: str = Query("1h")):
    minutes, bucket = range_values(range)
    wan_name = os.getenv("MIKROTIK_WAN_INTERFACE", "ether1")
    try:
        exists = client().query("EXISTS TABLE interface_stats").result_rows[0][0]
    except Exception:
        exists = 0
    if not exists:
        return []
    rows = client().query(f"""
    SELECT toUnixTimestamp(toStartOfInterval(ts, INTERVAL {bucket} SECOND)) t,
           avg(rx_bps) down, avg(tx_bps) up
    FROM interface_stats
    WHERE ts >= now() - INTERVAL {minutes} MINUTE AND interface = %(iface)s
    GROUP BY t ORDER BY t
    """, parameters={"iface": wan_name}).result_rows
    return [{"t": int(t), "down": int(d), "up": int(u)} for t,d,u in rows]

@app.get("/api/queue/history")
def queue_history(range: str = Query("1h")):
    minutes, bucket = range_values(range)
    if not queue_table_ready():
        return []
    rows = client().query(f"""
    SELECT t,
      sum(download_bps)/greatest(uniqExact(ts),1) down,
      sum(upload_bps)/greatest(uniqExact(ts),1) up
    FROM (
      SELECT ts, toUnixTimestamp(toStartOfInterval(ts, INTERVAL {bucket} SECOND)) t,
             queue_id, download_bps, upload_bps
      FROM queue_stats
      WHERE ts >= now() - INTERVAL {minutes} MINUTE
    )
    GROUP BY t ORDER BY t
    """).result_rows
    return [{"t": int(t), "down": int(d), "up": int(u)} for t,d,u in rows]

@app.get("/api/queue/customer/{ip}/history")
def queue_customer_history(ip: str, range: str = Query("1h")):
    ip = valid_ip(ip)
    minutes, bucket = range_values(range)
    if not queue_table_ready():
        return []
    rows = client().query(f"""
    SELECT t,
      sum(download_bps)/greatest(uniqExact(ts),1) down,
      sum(upload_bps)/greatest(uniqExact(ts),1) up
    FROM (
      SELECT ts, toUnixTimestamp(toStartOfInterval(ts, INTERVAL {bucket} SECOND)) t,
             download_bps, upload_bps
      FROM queue_stats
      WHERE ts >= now() - INTERVAL {minutes} MINUTE AND has(targets, %(ip)s)
    )
    GROUP BY t ORDER BY t
    """, parameters={"ip": ip}).result_rows
    return [{"t": int(t), "down": int(d), "up": int(u)} for t,d,u in rows]


@app.get("/static/{filename}")
def static_file(filename: str):
    if filename not in {"app.js", "style.css"}:
        raise HTTPException(status_code=404)
    return FileResponse(STATIC_DIR / filename, headers={"Cache-Control": "no-store"})

@app.get("/")
def index():
    return FileResponse(STATIC_DIR / "index.html", headers={"Cache-Control": "no-store"})
