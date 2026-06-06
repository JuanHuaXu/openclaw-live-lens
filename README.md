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

## Dashboard

Open the dashboard from the machine running the OpenClaw Gateway:

```text
http://127.0.0.1:<gateway-port>/openclaw-lens/dashboard
```

For example, with a gateway on port `18789`:

```text
http://127.0.0.1:18789/openclaw-lens/dashboard
```

The dashboard and its local JSON data endpoint are unauthenticated, but they
reject non-loopback clients. Open it directly from the gateway host, or through a
localhost tunnel that terminates on that host.

The dashboard auto-fetches span data in place without a full page refresh. Use
the **Filters** panel to narrow what you see:

- `Run ID`: show spans for a specific run.
- `Session`: show spans for a session key or a 24-character session hash.
- `Limit`: cap the number of rows returned.
- `Refresh`: fetch the current filtered data immediately.
- `Clear Filter`: clear `Run ID` and `Session`, then reload the table.
- `Auto`: keep polling automatically while the page is visible.

Use the **Tests** panel to generate data:

- `Run E2E`: creates a local synthetic smoke-test run with sanitized spans for a
  memory lookup, tool call, model call, LLM input/output metadata, and completed
  agent turn. It validates Live Lens storage and dashboard rendering without
  dispatching a real model request.
- `Clear E2E`: removes only dashboard-generated E2E test spans. It does not
  clear filters and does not remove real live-test or production telemetry.
- `Tools`: affects only `Live Test`. When checked, the live test asks the model
  to call the `live_lens_probe` plugin tool once so tool-call telemetry is
  included.
- `Live Test`: intentionally side-effecting. It creates a fresh local dashboard
  session, sends a real Gateway `sessions.send` test prompt, and filters the
  dashboard to that session's telemetry. With `Tools` unchecked it tests message
  and reply telemetry only; with `Tools` checked it also tests one real plugin
  tool call.

Selecting a row opens span details on the right, including run/session metadata,
call IDs, and redacted attributes.

## Query Spans

The dashboard uses this local loopback JSON endpoint:

```text
GET /openclaw-lens/spans?limit=100
GET /openclaw-lens/spans?runId=<run-id>&limit=200
GET /openclaw-lens/spans?sessionKey=<session-key>&limit=100
GET /openclaw-lens/spans?sessionHash=<session-hash>&limit=100
```

The local endpoint rejects non-loopback clients. It is intended for the
dashboard and local inspection.

The gateway-authenticated JSON query endpoint remains available:

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
