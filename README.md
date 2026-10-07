# AKGATE Dashboard

Lightweight ISP traffic dashboard for Akvorado + ClickHouse.

## v0.1

- total download/upload
- traffic history
- top customer IPs
- flow rate
- customer detail API
- Docker deployment

The dashboard is read-only. It does not modify Akvorado or flow data.

## Quick start

Copy the environment file:

```bash
cp .env.example .env
```

Adjust `.env`, then:

```bash
docker compose up -d --build
```

Open `http://SERVER:8082`.

> Never commit `.env` or credentials.
