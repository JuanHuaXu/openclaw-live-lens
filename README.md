# OpenClaw Live Lens

Live Lens is an observability-only OpenClaw plugin for timing agent turns. It records bounded spans from OpenClaw typed hooks, accepts external timing spans from provider proxies and plugins, and derives performance report rows from the stored trace.

## Versioning

Live Lens versions use `Major.Feature.Patch`.

## What It Records

- message received / sent hook observations
- prompt-build and pre-run shape
- model call start / end timing
- LLM input/output token and size metadata
- tool call timing
- compaction hook observations
- agent-end timing
- externally posted spans from provider proxies or plugins
- derived report rows for simple one-shot turns, tool-continuation turns, and sub-cost summaries

By default it does not store prompt text, message text, tool output, credentials, session keys, or other content-like fields. Session keys are hashed before storage.

## Generic Span Ingest

Post timing spans to:

```text
POST /api/openclaw-lens/ingest/spans
```

Local debugging processes on the Gateway host can also post to the loopback-only route:

```text
POST /openclaw-lens/ingest/spans
```

The loopback route requires an explicit local-ingest marker header:

```text
X-OpenClaw-Live-Lens-Local-Ingest: 1
```

Suggested body for an Ollama proxy span:

```json
{
  "runId": "run-id",
  "sessionKeyHash": "already-hashed-session-key",
  "spans": [
    {
      "source": "ollama-proxy",
      "name": "ollama.http",
      "phase": "model",
      "startedAtMs": 1770000000000,
      "durationMs": 638,
      "attributes": {
        "case": "Live config + LibraVDB",
        "promptEvalCount": 20205,
        "toolCount": 28,
        "requestPayloadBytes": 152000,
        "timeToFirstByteMs": 320
      }
    }
  ]
}
```

The route accepts up to 200 spans per request. It preserves metric-shaped fields and redacts content-shaped fields such as prompts, URLs, paths, session keys, tokens, and message text unless `captureContent` is explicitly enabled.

## LibraVDB Span Ingest

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

This is a compatibility alias for the generic ingest route with `source` defaulting to `libravdb`. It is Gateway-authenticated. If LibraVDB emits through HTTP, pass the normal Gateway auth header rather than creating a public endpoint.

## Performance Report

Live Lens can derive the report tables used for response-latency investigations:

```text
GET /openclaw-lens/report?runId=<run-id>
GET /openclaw-lens/report?sessionHash=<session-hash>
GET /openclaw-lens/report?runId=<run-id>&format=json
GET /api/openclaw-lens/report?runId=<run-id>
```

The local `/openclaw-lens/report` route defaults to a rendered HTML report for
browser use. Add `format=json` or send an `Accept: application/json` request to
receive JSON. The authenticated `/api/openclaw-lens/report` route defaults to
JSON, with `format=html` available for operator tooling that wants the rendered
view.

The report data includes:

- `simpleRows`: one row per run/session with total duration, model HTTP duration, non-model overhead, input tokens, and tool count.
- `toolRows`: continuation timing for runs with at least two model calls: pre-first-model, first model, tool duration, post-tool gap, and second model.
- `hookRows`: OpenClaw hook-derived span counts and timing metadata, so hook integration is visible alongside provider and plugin spans.
- `subcostRows`: grouped duration statistics for child spans such as LibraVDB assemble/compaction phases.

When both provider HTTP spans and OpenClaw model-call hook spans exist, the
report prefers provider HTTP spans so the model/network portion is not
double-counted. Provider HTTP spans must look model-related by name, source,
phase, or model/provider attributes; unrelated HTTP spans are ignored for model
timing and remain visible as ordinary sub-costs. If provider spans are absent,
the report falls back to `openclaw.model_call` duration spans.

Runless hook spans can be attached to nearby run-scoped spans in the same
session, but only when the nearest run is not ambiguous. If two candidate runs
are too close together, the span remains session-scoped instead of being
attributed to the wrong run.

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

- `Open Report`: open the rendered report page for the current `Run ID`,
  `Session`, and `Limit` filters.
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
GET /api/openclaw-lens/report?runId=<run-id>
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
