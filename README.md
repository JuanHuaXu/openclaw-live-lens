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

## Manual Hook Span Payloads

You can generate a report manually by posting sanitized OpenClaw hook-shaped
spans to the generic ingest endpoint. Use this when testing Live Lens without
running a full live agent turn, or when replaying redacted timing evidence from
logs.

Post to the Gateway-authenticated endpoint:

```sh
curl -sS "$GATEWAY_ORIGIN/api/openclaw-lens/ingest/spans" \
  -H "Content-Type: application/json" \
  -H "$GATEWAY_AUTH_HEADER" \
  --data-binary @manual-spans.json
```

For local-only dashboard debugging on the Gateway host, use the loopback route
instead:

```sh
curl -sS "http://127.0.0.1:<gateway-port>/openclaw-lens/ingest/spans" \
  -H "Content-Type: application/json" \
  -H "X-OpenClaw-Live-Lens-Local-Ingest: 1" \
  --data-binary @manual-spans.json
```

Use placeholders or pre-hashed values only. Do not include real prompt text,
message text, tool output, session keys, channel IDs, user IDs, local paths,
hostnames, IP addresses, tokens, or credentials in manual payloads.

These are the OpenClaw hook span names Live Lens uses in reports:

| OpenClaw hook | Span name | Phase | Timing shape |
| --- | --- | --- | --- |
| `message_received` | `openclaw.message_received` | `message` | point event; report can derive observed duration to the next span |
| `before_prompt_build` | `openclaw.before_prompt_build` | `context` | point event; report can derive observed duration to the next span |
| `before_agent_run` | `openclaw.before_agent_run` | `agent` | point event |
| `model_call_started` | `openclaw.model_call` | `model` | point event with `attributes.status: "started"` |
| `model_call_ended` | `openclaw.model_call` | `model` | duration span with `attributes.status: "ended"` |
| `llm_input` | `openclaw.llm_input` | `model` | point event with input shape metadata |
| `llm_output` | `openclaw.llm_output` | `model` | point event with output/usage metadata |
| `before_tool_call` | `openclaw.before_tool_call` | `tool` | point event with tool metadata |
| `after_tool_call` | `openclaw.tool_call` | `tool` | duration span with tool result metadata |
| `before_compaction` | `openclaw.before_compaction` | `compaction` | point event |
| `after_compaction` | `openclaw.after_compaction` | `compaction` | point event |
| `message_sent` | `openclaw.message_sent` | `message` | point event |
| `agent_end` | `openclaw.agent_run` | `agent` | duration span for the whole turn |

Minimal one-shot turn payload:

```json
{
  "runId": "manual-pong-run",
  "sessionKeyHash": "example-session-hash",
  "agentId": "main",
  "channelId": "example-channel",
  "spans": [
    {
      "source": "openclaw-hook",
      "name": "openclaw.message_received",
      "phase": "message",
      "startedAtMs": 1770000000000,
      "attributes": {
        "provider": "example-channel-provider"
      }
    },
    {
      "source": "openclaw-hook",
      "name": "openclaw.before_prompt_build",
      "phase": "context",
      "startedAtMs": 1770000000100,
      "attributes": {
        "messageCount": 3,
        "promptChars": 42,
        "contextTokenBudget": 262144
      }
    },
    {
      "source": "openclaw-hook",
      "name": "openclaw.before_agent_run",
      "phase": "agent",
      "startedAtMs": 1770000000200,
      "attributes": {
        "messageCount": 3,
        "systemPromptChars": 12000,
        "promptChars": 42
      }
    },
    {
      "source": "openclaw-hook",
      "name": "openclaw.llm_input",
      "phase": "model",
      "startedAtMs": 1770000000300,
      "attributes": {
        "provider": "example-model-provider",
        "model": "example-model",
        "historyCount": 3,
        "toolCount": 20,
        "promptChars": 42,
        "systemPromptChars": 12000
      }
    },
    {
      "source": "openclaw-hook",
      "name": "openclaw.model_call",
      "phase": "model",
      "callId": "manual-model-call-1",
      "startedAtMs": 1770000000400,
      "attributes": {
        "status": "started",
        "provider": "example-model-provider",
        "model": "example-model",
        "api": "example-api",
        "transport": "http"
      }
    },
    {
      "source": "openclaw-hook",
      "name": "openclaw.model_call",
      "phase": "model",
      "callId": "manual-model-call-1",
      "startedAtMs": 1770000000400,
      "endedAtMs": 1770000001200,
      "durationMs": 800,
      "attributes": {
        "status": "ended",
        "provider": "example-model-provider",
        "model": "example-model",
        "outcome": "success",
        "requestPayloadBytes": 50000,
        "responseStreamBytes": 500,
        "timeToFirstByteMs": 500
      }
    },
    {
      "source": "openclaw-hook",
      "name": "openclaw.llm_output",
      "phase": "model",
      "startedAtMs": 1770000001250,
      "attributes": {
        "provider": "example-model-provider",
        "model": "example-model",
        "outputTextCount": 1,
        "usageInput": 12000,
        "usageOutput": 4,
        "usageTotal": 12004
      }
    },
    {
      "source": "openclaw-hook",
      "name": "openclaw.message_sent",
      "phase": "message",
      "startedAtMs": 1770000001300,
      "attributes": {
        "success": true
      }
    },
    {
      "source": "openclaw-hook",
      "name": "openclaw.agent_run",
      "phase": "agent",
      "startedAtMs": 1770000000000,
      "endedAtMs": 1770000001400,
      "durationMs": 1400,
      "attributes": {
        "success": true,
        "messageCount": 4,
        "hasError": false
      }
    }
  ]
}
```

Tool-continuation payload additions:

```json
{
  "spans": [
    {
      "source": "openclaw-hook",
      "name": "openclaw.before_tool_call",
      "phase": "tool",
      "toolCallId": "manual-tool-call-1",
      "startedAtMs": 1770000001300,
      "attributes": {
        "toolName": "example_tool",
        "toolKind": "plugin",
        "toolInputKind": "json",
        "paramKeys": ["query"],
        "derivedPathCount": 0
      }
    },
    {
      "source": "openclaw-hook",
      "name": "openclaw.tool_call",
      "phase": "tool",
      "toolCallId": "manual-tool-call-1",
      "startedAtMs": 1770000001320,
      "endedAtMs": 1770000001620,
      "durationMs": 300,
      "attributes": {
        "toolName": "example_tool",
        "toolKind": "plugin",
        "toolInputKind": "json",
        "hasError": false,
        "resultKind": "object",
        "paramKeys": ["query"]
      }
    },
    {
      "source": "openclaw-hook",
      "name": "openclaw.model_call",
      "phase": "model",
      "callId": "manual-model-call-2",
      "startedAtMs": 1770000001900,
      "endedAtMs": 1770000002500,
      "durationMs": 600,
      "attributes": {
        "status": "ended",
        "provider": "example-model-provider",
        "model": "example-model",
        "outcome": "success"
      }
    }
  ]
}
```

Compaction payload additions:

```json
{
  "spans": [
    {
      "source": "openclaw-hook",
      "name": "openclaw.before_compaction",
      "phase": "compaction",
      "startedAtMs": 1770000002600,
      "attributes": {
        "messageCount": 25,
        "compactingCount": 12,
        "tokenCount": 24000
      }
    },
    {
      "source": "openclaw-hook",
      "name": "openclaw.after_compaction",
      "phase": "compaction",
      "startedAtMs": 1770000002800,
      "attributes": {
        "messageCount": 14,
        "compactedCount": 12,
        "tokenCount": 9000
      }
    }
  ]
}
```

Optional provider HTTP spans can be posted alongside hook spans when you want
the report to prefer measured provider/network timing over `openclaw.model_call`
hook duration:

```json
{
  "spans": [
    {
      "source": "model-proxy",
      "name": "provider.http",
      "phase": "model",
      "startedAtMs": 1770000000400,
      "durationMs": 800,
      "attributes": {
        "provider": "example-model-provider",
        "model": "example-model",
        "promptEvalCount": 12000,
        "toolCount": 20,
        "requestPayloadBytes": 50000,
        "timeToFirstByteMs": 500
      }
    }
  ]
}
```

## Dashboard

Open the dashboard from the machine running the OpenClaw Gateway:

```text
http://127.0.0.1:<gateway-port>/openclaw-lens/dashboard
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
