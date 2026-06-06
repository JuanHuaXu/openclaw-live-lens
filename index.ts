import { createHash, randomUUID } from "node:crypto";
import { mkdirSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";
import path from "node:path";
import type { IncomingMessage, ServerResponse } from "node:http";
import { definePluginEntry } from "openclaw/plugin-sdk/plugin-entry";
import type { OpenClawPluginApi } from "openclaw/plugin-sdk/plugin-entry";

const PLUGIN_ID = "live-lens";
const DEFAULT_DATABASE_PATH = "./data/openclaw-live-lens.sqlite";
const DEFAULT_MAX_ATTRIBUTE_CHARS = 6_000;
const DEFAULT_MAX_INGEST_BYTES = 256 * 1024;
const MAX_SPANS_PER_INGEST = 200;
const MAX_SPAN_NAME_CHARS = 160;
const MAX_ATTR_STRING_CHARS = 1_000;
const SENSITIVE_KEY_RE =
  /(content|prompt|message|body|text|token|secret|password|credential|cookie|authorization|apikey|api_key|email|phone|address|sessionkey)/i;

type LensConfig = {
  enabled: boolean;
  databasePath: string;
  recordHooks: boolean;
  captureContent: boolean;
  maxAttributeChars: number;
  maxIngestBytes: number;
};

type SpanInput = {
  name?: unknown;
  source?: unknown;
  phase?: unknown;
  runId?: unknown;
  callId?: unknown;
  toolCallId?: unknown;
  parentSpanId?: unknown;
  sessionKey?: unknown;
  sessionKeyHash?: unknown;
  agentId?: unknown;
  channelId?: unknown;
  startedAtMs?: unknown;
  endedAtMs?: unknown;
  durationMs?: unknown;
  attributes?: unknown;
};

type SpanRecord = {
  spanId: string;
  parentSpanId?: string;
  source: string;
  name: string;
  phase?: string;
  runId?: string;
  callId?: string;
  toolCallId?: string;
  sessionHash?: string;
  agentId?: string;
  channelId?: string;
  startedAtMs: number;
  endedAtMs?: number;
  durationMs?: number;
  attributes: Record<string, unknown>;
};

type LensStore = {
  insert(span: SpanRecord): void;
  list(params: { limit: number; runId?: string; sessionHash?: string }): unknown[];
  close(): void;
};

function normalizeConfig(value: unknown): LensConfig {
  const cfg = isRecord(value) ? value : {};
  return {
    enabled: readBoolean(cfg.enabled, true),
    databasePath: readString(cfg.databasePath, DEFAULT_DATABASE_PATH),
    recordHooks: readBoolean(cfg.recordHooks, true),
    captureContent: readBoolean(cfg.captureContent, false),
    maxAttributeChars: clampNumber(cfg.maxAttributeChars, DEFAULT_MAX_ATTRIBUTE_CHARS, 500, 50_000),
    maxIngestBytes: clampNumber(cfg.maxIngestBytes, DEFAULT_MAX_INGEST_BYTES, 4_096, 1_048_576),
  };
}

function createStore(dbPath: string): LensStore {
  mkdirSync(path.dirname(dbPath), { recursive: true });
  const db = new DatabaseSync(dbPath);
  db.exec(`
    PRAGMA journal_mode = WAL;
    CREATE TABLE IF NOT EXISTS spans (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      span_id TEXT NOT NULL,
      parent_span_id TEXT,
      source TEXT NOT NULL,
      name TEXT NOT NULL,
      phase TEXT,
      run_id TEXT,
      call_id TEXT,
      tool_call_id TEXT,
      session_hash TEXT,
      agent_id TEXT,
      channel_id TEXT,
      started_at_ms INTEGER NOT NULL,
      ended_at_ms INTEGER,
      duration_ms REAL,
      attributes_json TEXT NOT NULL,
      created_at_ms INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_lens_spans_run ON spans(run_id, started_at_ms);
    CREATE INDEX IF NOT EXISTS idx_lens_spans_session ON spans(session_hash, started_at_ms);
    CREATE INDEX IF NOT EXISTS idx_lens_spans_created ON spans(created_at_ms);
  `);

  const insertStmt = db.prepare(`
    INSERT INTO spans (
      span_id, parent_span_id, source, name, phase, run_id, call_id,
      tool_call_id, session_hash, agent_id, channel_id, started_at_ms,
      ended_at_ms, duration_ms, attributes_json, created_at_ms
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);
  const listStmt = db.prepare(`
    SELECT span_id, parent_span_id, source, name, phase, run_id, call_id,
      tool_call_id, session_hash, agent_id, channel_id, started_at_ms,
      ended_at_ms, duration_ms, attributes_json, created_at_ms
    FROM spans
    WHERE (? IS NULL OR run_id = ?)
      AND (? IS NULL OR session_hash = ?)
    ORDER BY created_at_ms DESC, id DESC
    LIMIT ?
  `);

  return {
    insert(span) {
      insertStmt.run(
        span.spanId,
        span.parentSpanId ?? null,
        span.source,
        span.name,
        span.phase ?? null,
        span.runId ?? null,
        span.callId ?? null,
        span.toolCallId ?? null,
        span.sessionHash ?? null,
        span.agentId ?? null,
        span.channelId ?? null,
        Math.round(span.startedAtMs),
        span.endedAtMs === undefined ? null : Math.round(span.endedAtMs),
        span.durationMs ?? null,
        JSON.stringify(span.attributes),
        Date.now(),
      );
    },
    list(params) {
      const rows = listStmt.all(
        params.runId ?? null,
        params.runId ?? null,
        params.sessionHash ?? null,
        params.sessionHash ?? null,
        params.limit,
      );
      return rows.map((row) => {
        const record = row as Record<string, unknown>;
        return {
          spanId: record.span_id,
          parentSpanId: record.parent_span_id ?? undefined,
          source: record.source,
          name: record.name,
          phase: record.phase ?? undefined,
          runId: record.run_id ?? undefined,
          callId: record.call_id ?? undefined,
          toolCallId: record.tool_call_id ?? undefined,
          sessionHash: record.session_hash ?? undefined,
          agentId: record.agent_id ?? undefined,
          channelId: record.channel_id ?? undefined,
          startedAtMs: record.started_at_ms,
          endedAtMs: record.ended_at_ms ?? undefined,
          durationMs: record.duration_ms ?? undefined,
          attributes: safeJsonParse(record.attributes_json),
          createdAtMs: record.created_at_ms,
        };
      });
    },
    close() {
      db.close();
    },
  };
}

function registerHookSpans(api: OpenClawPluginApi, store: LensStore, config: LensConfig) {
  if (!config.enabled || !config.recordHooks) {
    return;
  }

  api.on("message_received", (event, ctx) => {
    recordSpan(store, config, {
      source: "openclaw-hook",
      name: "openclaw.message_received",
      phase: "message",
      runId: ctx.runId,
      sessionKey: ctx.sessionKey,
      channelId: ctx.channelId,
      attributes: {
        channelId: readEventString(event, "channelId"),
        provider: readEventString(event, "provider"),
      },
    });
  });

  api.on("before_prompt_build", (event, ctx) => {
    recordSpan(store, config, {
      source: "openclaw-hook",
      name: "openclaw.before_prompt_build",
      phase: "context",
      runId: ctx.runId,
      sessionKey: ctx.sessionKey,
      agentId: ctx.agentId,
      channelId: ctx.channelId,
      attributes: {
        messageCount: Array.isArray(event.messages) ? event.messages.length : undefined,
        promptChars: typeof event.prompt === "string" ? event.prompt.length : undefined,
        contextTokenBudget: ctx.contextTokenBudget,
        contextWindowSource: ctx.contextWindowSource,
      },
    });
  });

  api.on("before_agent_run", (event, ctx) => {
    recordSpan(store, config, {
      source: "openclaw-hook",
      name: "openclaw.before_agent_run",
      phase: "agent",
      runId: ctx.runId,
      sessionKey: ctx.sessionKey,
      agentId: ctx.agentId,
      channelId: ctx.channelId,
      attributes: {
        messageCount: Array.isArray(event.messages) ? event.messages.length : undefined,
        systemPromptChars: typeof event.systemPrompt === "string" ? event.systemPrompt.length : undefined,
        promptChars: typeof event.prompt === "string" ? event.prompt.length : undefined,
      },
    });
  });

  api.on("model_call_started", (event, ctx) => {
    recordSpan(store, config, {
      source: "openclaw-hook",
      name: "openclaw.model_call",
      phase: "model",
      runId: event.runId,
      callId: event.callId,
      sessionKey: event.sessionKey ?? ctx.sessionKey,
      agentId: ctx.agentId,
      channelId: ctx.channelId,
      startedAtMs: Date.now(),
      attributes: {
        status: "started",
        provider: event.provider,
        model: event.model,
        api: event.api,
        transport: event.transport,
        contextTokenBudget: event.contextTokenBudget,
      },
    });
  });

  api.on("model_call_ended", (event, ctx) => {
    const endedAtMs = Date.now();
    recordSpan(store, config, {
      source: "openclaw-hook",
      name: "openclaw.model_call",
      phase: "model",
      runId: event.runId,
      callId: event.callId,
      sessionKey: event.sessionKey ?? ctx.sessionKey,
      agentId: ctx.agentId,
      channelId: ctx.channelId,
      startedAtMs: endedAtMs - event.durationMs,
      endedAtMs,
      durationMs: event.durationMs,
      attributes: {
        status: "ended",
        provider: event.provider,
        model: event.model,
        outcome: event.outcome,
        errorCategory: event.errorCategory,
        failureKind: event.failureKind,
        requestPayloadBytes: event.requestPayloadBytes,
        responseStreamBytes: event.responseStreamBytes,
        timeToFirstByteMs: event.timeToFirstByteMs,
        contextTokenBudget: event.contextTokenBudget,
      },
    });
  });

  api.on("llm_input", (event, ctx) => {
    recordSpan(store, config, {
      source: "openclaw-hook",
      name: "openclaw.llm_input",
      phase: "model",
      runId: event.runId,
      sessionKey: ctx.sessionKey,
      agentId: ctx.agentId,
      channelId: ctx.channelId,
      attributes: {
        provider: event.provider,
        model: event.model,
        systemPromptChars: typeof event.systemPrompt === "string" ? event.systemPrompt.length : undefined,
        promptChars: typeof event.prompt === "string" ? event.prompt.length : undefined,
        historyCount: Array.isArray(event.historyMessages) ? event.historyMessages.length : undefined,
        imagesCount: event.imagesCount,
        toolCount: Array.isArray(event.tools) ? event.tools.length : undefined,
      },
    });
  });

  api.on("llm_output", (event, ctx) => {
    recordSpan(store, config, {
      source: "openclaw-hook",
      name: "openclaw.llm_output",
      phase: "model",
      runId: event.runId,
      sessionKey: ctx.sessionKey,
      agentId: ctx.agentId,
      channelId: ctx.channelId,
      attributes: {
        provider: event.provider,
        model: event.model,
        harnessId: event.harnessId,
        outputTextCount: event.assistantTexts.length,
        usageInput: event.usage?.input,
        usageOutput: event.usage?.output,
        usageTotal: event.usage?.total,
        usageCacheRead: event.usage?.cacheRead,
        usageCacheWrite: event.usage?.cacheWrite,
        contextTokenBudget: event.contextTokenBudget,
      },
    });
  });

  api.on("before_tool_call", (event, ctx) => {
    recordSpan(store, config, {
      source: "openclaw-hook",
      name: "openclaw.before_tool_call",
      phase: "tool",
      runId: event.runId ?? ctx.runId,
      toolCallId: event.toolCallId ?? ctx.toolCallId,
      sessionKey: ctx.sessionKey,
      agentId: ctx.agentId,
      channelId: ctx.channelId,
      attributes: {
        toolName: event.toolName,
        toolKind: event.toolKind,
        toolInputKind: event.toolInputKind,
        paramKeys: Object.keys(event.params ?? {}).sort(),
        derivedPathCount: event.derivedPaths?.length,
      },
    });
  });

  api.on("after_tool_call", (event, ctx) => {
    const endedAtMs = Date.now();
    const durationMs = typeof event.durationMs === "number" ? event.durationMs : undefined;
    recordSpan(store, config, {
      source: "openclaw-hook",
      name: "openclaw.tool_call",
      phase: "tool",
      runId: event.runId ?? ctx.runId,
      toolCallId: event.toolCallId ?? ctx.toolCallId,
      sessionKey: ctx.sessionKey,
      agentId: ctx.agentId,
      channelId: ctx.channelId,
      startedAtMs: durationMs === undefined ? endedAtMs : endedAtMs - durationMs,
      endedAtMs,
      durationMs,
      attributes: {
        toolName: event.toolName,
        toolKind: ctx.toolKind,
        toolInputKind: ctx.toolInputKind,
        hasError: Boolean(event.error),
        resultKind: describeValue(event.result),
        paramKeys: Object.keys(event.params ?? {}).sort(),
      },
    });
  });

  api.on("before_compaction", (event, ctx) => {
    recordSpan(store, config, {
      source: "openclaw-hook",
      name: "openclaw.before_compaction",
      phase: "compaction",
      runId: ctx.runId,
      sessionKey: ctx.sessionKey,
      channelId: ctx.channelId,
      attributes: {
        messageCount: event.messageCount,
        compactingCount: event.compactingCount,
        tokenCount: event.tokenCount,
      },
    });
  });

  api.on("after_compaction", (event, ctx) => {
    recordSpan(store, config, {
      source: "openclaw-hook",
      name: "openclaw.after_compaction",
      phase: "compaction",
      runId: ctx.runId,
      sessionKey: ctx.sessionKey,
      agentId: ctx.agentId,
      channelId: ctx.channelId,
      attributes: {
        messageCount: event.messageCount,
        compactedCount: event.compactedCount,
        tokenCount: event.tokenCount,
      },
    });
  });

  api.on("message_sent", (event, ctx) => {
    recordSpan(store, config, {
      source: "openclaw-hook",
      name: "openclaw.message_sent",
      phase: "message",
      runId: ctx.runId,
      sessionKey: ctx.sessionKey,
      channelId: ctx.channelId,
      attributes: {
        success: readEventBoolean(event, "success"),
        channelId: readEventString(event, "channelId"),
      },
    });
  });

  api.on("agent_end", (event, ctx) => {
    const endedAtMs = Date.now();
    const durationMs = typeof event.durationMs === "number" ? event.durationMs : undefined;
    recordSpan(store, config, {
      source: "openclaw-hook",
      name: "openclaw.agent_run",
      phase: "agent",
      runId: event.runId ?? ctx.runId,
      sessionKey: ctx.sessionKey,
      agentId: ctx.agentId,
      channelId: ctx.channelId,
      startedAtMs: durationMs === undefined ? endedAtMs : endedAtMs - durationMs,
      endedAtMs,
      durationMs,
      attributes: {
        success: event.success,
        messageCount: Array.isArray(event.messages) ? event.messages.length : undefined,
        hasError: Boolean(event.error),
      },
    });
  });
}

function registerHttpRoutes(api: OpenClawPluginApi, store: LensStore, config: LensConfig) {
  api.registerHttpRoute({
    path: "/api/openclaw-lens/health",
    auth: "gateway",
    match: "exact",
    gatewayRuntimeScopeSurface: "trusted-operator",
    handler: (_req, res) => {
      sendJson(res, 200, {
        ok: true,
        pluginId: PLUGIN_ID,
        enabled: config.enabled,
        recordHooks: config.recordHooks,
        captureContent: config.captureContent,
      });
      return true;
    },
  });

  api.registerHttpRoute({
    path: "/api/openclaw-lens/spans",
    auth: "gateway",
    match: "exact",
    gatewayRuntimeScopeSurface: "trusted-operator",
    handler: (req, res) => {
      const url = new URL(req.url ?? "", "http://localhost");
      const limit = clampNumber(Number(url.searchParams.get("limit") ?? 100), 100, 1, 500);
      const runId = normalizeOptionalString(url.searchParams.get("runId"));
      const sessionKey = normalizeOptionalString(url.searchParams.get("sessionKey"));
      const sessionHash = normalizeOptionalString(url.searchParams.get("sessionHash")) ??
        (sessionKey ? hashValue(sessionKey) : undefined);
      sendJson(res, 200, {
        ok: true,
        spans: store.list({ limit, runId, sessionHash }),
      });
      return true;
    },
  });

  api.registerHttpRoute({
    path: "/api/openclaw-lens/libravdb/spans",
    auth: "gateway",
    match: "exact",
    gatewayRuntimeScopeSurface: "trusted-operator",
    handler: async (req, res) => {
      if (req.method !== "POST") {
        sendJson(res, 405, { ok: false, error: "method_not_allowed" });
        return true;
      }
      if (!config.enabled) {
        sendJson(res, 202, { ok: true, accepted: 0, disabled: true });
        return true;
      }
      const body = await readRequestBody(req, config.maxIngestBytes);
      const payload = safeJsonParse(body);
      const spans = normalizeIngestedSpans(payload);
      let accepted = 0;
      for (const span of spans.slice(0, MAX_SPANS_PER_INGEST)) {
        recordSpan(store, config, {
          ...span,
          source: normalizeOptionalString(span.source) ?? "libravdb",
        });
        accepted += 1;
      }
      sendJson(res, 202, {
        ok: true,
        accepted,
        dropped: Math.max(0, spans.length - accepted),
      });
      return true;
    },
  });
}

function recordSpan(store: LensStore, config: LensConfig, input: SpanInput): void {
  const name = normalizeString(input.name, "unknown").slice(0, MAX_SPAN_NAME_CHARS);
  const source = normalizeString(input.source, "unknown").slice(0, MAX_SPAN_NAME_CHARS);
  const startedAtMs = readFiniteNumber(input.startedAtMs) ?? Date.now();
  const durationMs = readFiniteNumber(input.durationMs);
  const endedAtMs = readFiniteNumber(input.endedAtMs) ??
    (durationMs === undefined ? undefined : startedAtMs + durationMs);
  const sessionHash = normalizeOptionalString(input.sessionKeyHash) ??
    hashOptional(normalizeOptionalString(input.sessionKey));
  store.insert({
    spanId: randomUUID(),
    parentSpanId: normalizeOptionalString(input.parentSpanId),
    source,
    name,
    phase: normalizeOptionalString(input.phase),
    runId: normalizeOptionalString(input.runId),
    callId: normalizeOptionalString(input.callId),
    toolCallId: normalizeOptionalString(input.toolCallId),
    sessionHash,
    agentId: normalizeOptionalString(input.agentId),
    channelId: normalizeOptionalString(input.channelId),
    startedAtMs,
    endedAtMs,
    durationMs,
    attributes: sanitizeAttributes(input.attributes, config),
  });
}

function normalizeIngestedSpans(payload: unknown): SpanInput[] {
  const record = isRecord(payload) ? payload : {};
  const rawSpans = Array.isArray(record.spans) ? record.spans : [record];
  const envelope = {
    runId: record.runId,
    sessionKey: record.sessionKey,
    sessionKeyHash: record.sessionKeyHash,
    agentId: record.agentId,
    channelId: record.channelId,
  };
  return rawSpans.filter(isRecord).map((span) => ({
    ...envelope,
    ...span,
    attributes: isRecord(span.attributes)
      ? { ...envelope, ...span.attributes }
      : envelope,
  }));
}

function sanitizeAttributes(value: unknown, config: LensConfig): Record<string, unknown> {
  const normalized = sanitizeValue(value, config, 0);
  const record = isRecord(normalized) ? normalized : { value: normalized };
  const json = JSON.stringify(record);
  if (json.length <= config.maxAttributeChars) {
    return record;
  }
  return {
    truncated: true,
    originalChars: json.length,
    preview: json.slice(0, config.maxAttributeChars),
  };
}

function sanitizeValue(value: unknown, config: LensConfig, depth: number): unknown {
  if (depth > 4) {
    return "[max-depth]";
  }
  if (value == null || typeof value === "boolean" || typeof value === "number") {
    return value;
  }
  if (typeof value === "string") {
    return value.length > MAX_ATTR_STRING_CHARS
      ? `${value.slice(0, MAX_ATTR_STRING_CHARS)}...[truncated]`
      : value;
  }
  if (Array.isArray(value)) {
    return value.slice(0, 50).map((item) => sanitizeValue(item, config, depth + 1));
  }
  if (!isRecord(value)) {
    return describeValue(value);
  }
  const out: Record<string, unknown> = {};
  for (const [key, raw] of Object.entries(value).slice(0, 100)) {
    if (!config.captureContent && SENSITIVE_KEY_RE.test(key)) {
      if (/sessionkey/i.test(key) && typeof raw === "string") {
        out[`${key}Hash`] = hashValue(raw);
      } else {
        out[key] = "[redacted]";
      }
      continue;
    }
    out[key] = sanitizeValue(raw, config, depth + 1);
  }
  return out;
}

function readRequestBody(req: IncomingMessage, maxBytes: number): Promise<string> {
  return new Promise((resolve, reject) => {
    let total = 0;
    const chunks: Buffer[] = [];
    req.on("data", (chunk: Buffer) => {
      total += chunk.byteLength;
      if (total > maxBytes) {
        reject(new Error(`request body exceeds ${maxBytes} bytes`));
        req.destroy();
        return;
      }
      chunks.push(chunk);
    });
    req.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
    req.on("error", reject);
  });
}

function sendJson(res: ServerResponse, statusCode: number, payload: unknown): void {
  const body = JSON.stringify(payload);
  res.statusCode = statusCode;
  res.setHeader("content-type", "application/json; charset=utf-8");
  res.setHeader("content-length", Buffer.byteLength(body));
  res.end(body);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function normalizeString(value: unknown, fallback: string): string {
  return normalizeOptionalString(value) ?? fallback;
}

function normalizeOptionalString(value: unknown): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function readString(value: unknown, fallback: string): string {
  return normalizeOptionalString(value) ?? fallback;
}

function readBoolean(value: unknown, fallback: boolean): boolean {
  return typeof value === "boolean" ? value : fallback;
}

function readFiniteNumber(value: unknown): number | undefined {
  return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

function clampNumber(value: unknown, fallback: number, min: number, max: number): number {
  const candidate = typeof value === "number" && Number.isFinite(value) ? value : fallback;
  return Math.max(min, Math.min(max, Math.floor(candidate)));
}

function hashOptional(value: string | undefined): string | undefined {
  return value ? hashValue(value) : undefined;
}

function hashValue(value: string): string {
  return createHash("sha256").update(value).digest("hex").slice(0, 24);
}

function safeJsonParse(value: unknown): unknown {
  if (typeof value !== "string") {
    return {};
  }
  try {
    return JSON.parse(value);
  } catch {
    return {};
  }
}

function describeValue(value: unknown): string {
  if (value === null) {
    return "null";
  }
  if (Array.isArray(value)) {
    return `array:${value.length}`;
  }
  return typeof value;
}

function readEventString(event: unknown, key: string): string | undefined {
  return isRecord(event) ? normalizeOptionalString(event[key]) : undefined;
}

function readEventBoolean(event: unknown, key: string): boolean | undefined {
  if (!isRecord(event)) {
    return undefined;
  }
  const value = event[key];
  return typeof value === "boolean" ? value : undefined;
}

export default definePluginEntry({
  id: PLUGIN_ID,
  name: "Live Lens",
  description: "Record bounded timing spans for OpenClaw and plugin hot-path debugging.",
  register(api) {
    const config = normalizeConfig(api.pluginConfig);
    const dbPath = path.resolve(api.rootDir ?? process.cwd(), config.databasePath);
    const store = createStore(dbPath);

    registerHookSpans(api, store, config);
    registerHttpRoutes(api, store, config);

    api.lifecycle.registerRuntimeLifecycle({
      id: "close-live-lens-db",
      description: "Close Live Lens SQLite store during plugin shutdown.",
      cleanup: () => store.close(),
    });

    api.logger.info(`[live-lens] recording ${config.enabled ? "enabled" : "disabled"} at ${dbPath}`);
  },
});
