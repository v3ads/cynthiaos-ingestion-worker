# cynthiaos-ingestion-worker

CynthiaOS Bronze ingestion worker. Receives AppFolio webhook events, validates HMAC signatures, deduplicates payloads, and writes raw events to the Bronze layer in Neon Postgres.

> **Status:** Scaffold only (TASK-011). Business logic is not yet implemented.

## Quick Start

```bash
cp .env.example .env
npm install
npm run dev
```

Health check: `GET http://localhost:3001/health`

## Scripts

| Command | Purpose |
|---------|---------|
| `npm run dev` | Run with hot-reload (ts-node-dev) |
| `npm run build` | Compile TypeScript to `dist/` |
| `npm start` | Run compiled output |
| `npm run typecheck` | Type-check without emitting |

## Docker

```bash
docker build -t cynthiaos-ingestion-worker .
docker run -p 3001:3001 --env-file .env cynthiaos-ingestion-worker
```

## Railway Deployment

- **Start command:** `node dist/index.js`
- **Build command:** `npm ci && npm run build`
- **Port:** Set `PORT` environment variable in Railway dashboard

## Environment Variables

See `.env.example` for all required variables.
