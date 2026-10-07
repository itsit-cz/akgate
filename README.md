# AKGATE Dashboard

ISP traffic dashboard combining Akvorado/ClickHouse flow analytics with real post-shaping MikroTik Simple Queue traffic.

## v0.6.0

- real download/upload from MikroTik Simple Queue
- automatic client names, limits and dropped counters from RouterOS
- queue history stored in ClickHouse every 5 seconds
- Akvorado remains the source for flows, ASN, protocols and ports
- notes remain local and searchable
- RouterOS collector is read-only

## Quick start

Copy the environment file:

```bash
cp .env.example .env
```

Set ClickHouse values and the RouterOS monitor account in `.env`:

```text
MIKROTIK_HOST=86.49.51.1
MIKROTIK_PORT=8728
MIKROTIK_USER=akgate-monitor
MIKROTIK_PASSWORD=CHANGE_ME
MIKROTIK_POLL_INTERVAL=5
```

Then:

```bash
docker compose up -d --build
docker compose logs -f akgate-collector
```

The collector creates `queue_stats` automatically. Historical real-traffic graphs start filling from the moment v0.6.0 is deployed.

Open `http://SERVER:8082`.

> Never commit `.env` or credentials.
