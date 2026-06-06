# OpenClaw Live Lens

Live Lens is an observability-only OpenClaw plugin for timing agent turns. It records bounded spans from OpenClaw typed hooks and exposes a stub endpoint where plugins such as `libravdb-memory` can publish child timing spans.

## What It Records

- message received / sent hook observations
- prompt-build and pre-run shape
- model call start / end timing
- LLM input/output token and size metadata
- tool call timing
- compaction hook observations
- agent-end timing
- externally posted plugin spans, currently intended for LibraVDB

By default it does not store prompt text, message text, tool output, credentials, session keys, or other content-like fields. Session keys are hashed before storage.

## LibraVDB Span Ingest Stub

Post timing spans to:

```text
POST /api/openclaw-lens/libravdb/spans
```

Suggested body:

```json
{
  "runId": "run-id",
  "sessionKeyHash": "already-hashed-session-key",
  "spans": [
    {
      "name": "libravdb.daemon.assembleContextInternal",
      "phase": "assemble",
      "durationMs": 631,
      "attributes": {
        "messageCount": 17,
        "requestBytes": 7412,
        "responseBytes": 32380,
        "cacheHit": false
      }
    }
  ]
}
```

The route is Gateway-authenticated. If LibraVDB emits through HTTP, pass the normal Gateway auth header rather than creating a public endpoint.

## Query Spans

```text
GET /api/openclaw-lens/spans?limit=100
GET /api/openclaw-lens/spans?runId=<run-id>&limit=200
```

Health check:

```text
GET /api/openclaw-lens/health
```

## Config

```json
{
  "plugins": {
    "allow": ["live-lens"],
    "entries": {
      "live-lens": {
        "enabled": true,
        "config": {
          "databasePath": "./data/openclaw-live-lens.sqlite",
          "recordHooks": true,
          "captureContent": false
        }
      }
    }
  }
}
```
