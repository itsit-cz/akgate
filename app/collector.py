import os
import re
import signal
import socket
import time
from datetime import datetime, timezone
from ipaddress import ip_address, ip_network

import routeros_api

from .db import client

INTERVAL = max(2.0, float(os.getenv("MIKROTIK_POLL_INTERVAL", "5")))
HOST = os.getenv("MIKROTIK_HOST", "")
PORT = int(os.getenv("MIKROTIK_PORT", "8728"))
USER = os.getenv("MIKROTIK_USER", "")
PASSWORD = os.getenv("MIKROTIK_PASSWORD", "")
WAN_INTERFACE = os.getenv("MIKROTIK_WAN_INTERFACE", "ether1")
RUNNING = True
CUSTOMER_NETWORKS = [ip_network(x.strip()) for x in os.getenv("CUSTOMER_NETWORKS", "").split(",") if x.strip()]

WAN_SCHEMA = """
CREATE TABLE IF NOT EXISTS interface_stats (
    ts DateTime64(3, 'UTC'),
    interface String,
    rx_bps UInt64,
    tx_bps UInt64,
    rx_bytes UInt64,
    tx_bytes UInt64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (interface, ts)
TTL ts + INTERVAL 400 DAY
"""

SCHEMA = """
CREATE TABLE IF NOT EXISTS queue_stats (
    ts DateTime64(3, 'UTC'),
    queue_id String,
    queue_name String,
    targets Array(String),
    upload_bps UInt64,
    download_bps UInt64,
    upload_bytes UInt64,
    download_bytes UInt64,
    upload_packets UInt64,
    download_packets UInt64,
    upload_dropped UInt64,
    download_dropped UInt64,
    upload_limit UInt64,
    download_limit UInt64,
    comment String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (ts, queue_id)
TTL ts + INTERVAL 400 DAY
"""

def pair(value):
    try:
        a, b = str(value or "0/0").split("/", 1)
        return int(a), int(b)
    except (ValueError, TypeError):
        return 0, 0

def targets(value):
    out = []
    for part in str(value or "").split(","):
        m = re.match(r"\s*([^/\s]+)", part)
        if m:
            out.append(m.group(1))
    return out

def connect():
    pool = routeros_api.RouterOsApiPool(
        HOST, username=USER, password=PASSWORD, port=PORT,
        plaintext_login=True, use_ssl=False,
    )
    return pool, pool.get_api()

def collect(api):
    now = datetime.now(timezone.utc)
    rows = []
    for q in api.get_resource("/queue/simple").get():
        if q.get("disabled") == "true" or q.get("dynamic") == "true":
            continue
        ips = targets(q.get("target"))
        if not ips:
            continue
        if CUSTOMER_NETWORKS:
            customer_ips = []
            for value in ips:
                try:
                    addr = ip_address(value)
                    if any(addr in net for net in CUSTOMER_NETWORKS):
                        customer_ips.append(value)
                except ValueError:
                    pass
            if not customer_ips:
                continue
            ips = customer_ips
        ubps, dbps = pair(q.get("rate"))
        ubytes, dbytes = pair(q.get("bytes"))
        upackets, dpackets = pair(q.get("packets"))
        udropped, ddropped = pair(q.get("dropped"))
        ulimit, dlimit = pair(q.get("max-limit"))
        rows.append([
            now, str(q.get("id", "")), str(q.get("name", "")), ips,
            ubps, dbps, ubytes, dbytes, upackets, dpackets,
            udropped, ddropped, ulimit, dlimit, str(q.get("comment", "")),
        ])
    return rows

def collect_wan(api):
    now = datetime.now(timezone.utc)
    # Live bitrate exists only in /interface monitor-traffic, not /interface print.
    monitor = api.get_resource("/interface").call(
        "monitor-traffic", {"interface": WAN_INTERFACE, "once": ""}
    )
    if not monitor:
        raise RuntimeError(f"WAN interface {WAN_INTERFACE} monitor returned no data")
    m = monitor[0]
    iface_rows = api.get_resource("/interface").get(name=WAN_INTERFACE)
    iface = iface_rows[0] if iface_rows else {}
    return [
        now, WAN_INTERFACE,
        int(m.get("rx-bits-per-second", 0) or 0),
        int(m.get("tx-bits-per-second", 0) or 0),
        int(iface.get("rx-byte", 0) or 0),
        int(iface.get("tx-byte", 0) or 0),
    ]

def stop(*_):
    global RUNNING
    RUNNING = False

def main():
    if not HOST or not USER or not PASSWORD:
        raise SystemExit("MIKROTIK_HOST, MIKROTIK_USER and MIKROTIK_PASSWORD are required")
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    ch = client()
    ch.command(SCHEMA)
    ch.command(WAN_SCHEMA)
    pool = api = None
    while RUNNING:
        started = time.monotonic()
        try:
            if api is None:
                pool, api = connect()
                print(f"RouterOS connected: {HOST}:{PORT}", flush=True)
            rows = collect(api)
            wan = collect_wan(api)
            if rows:
                ch.insert(
                    "queue_stats", rows,
                    column_names=[
                        "ts","queue_id","queue_name","targets","upload_bps","download_bps",
                        "upload_bytes","download_bytes","upload_packets","download_packets",
                        "upload_dropped","download_dropped","upload_limit","download_limit","comment"
                    ],
                )
            ch.insert("interface_stats", [wan], column_names=["ts","interface","rx_bps","tx_bps","rx_bytes","tx_bytes"])
            print(f"queue_stats: {len(rows)} queues | {WAN_INTERFACE}: RX {wan[2]} TX {wan[3]} bps", flush=True)
        except Exception as exc:
            print(f"collector error: {type(exc).__name__}: {exc}", flush=True)
            try:
                if pool:
                    pool.disconnect()
            except Exception:
                pass
            pool = api = None
            time.sleep(2)
        elapsed = time.monotonic() - started
        time.sleep(max(0.2, INTERVAL - elapsed))
    if pool:
        try:
            pool.disconnect()
        except Exception:
            pass

if __name__ == "__main__":
    main()
