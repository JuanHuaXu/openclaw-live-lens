import { execFile } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { mkdirSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";
import path from "node:path";
import { performance } from "node:perf_hooks";
import type { IncomingMessage, ServerResponse } from "node:http";
import { definePluginEntry } from "openclaw/plugin-sdk/plugin-entry";
import type { AnyAgentTool, OpenClawPluginApi } from "openclaw/plugin-sdk/plugin-entry";

const PLUGIN_ID = "live-lens";
const DEFAULT_DATABASE_PATH = "./data/openclaw-live-lens.sqlite";
const DEFAULT_MAX_ATTRIBUTE_CHARS = 6_000;
const DEFAULT_MAX_INGEST_BYTES = 256 * 1024;
const MAX_SPANS_PER_INGEST = 200;
const MAX_PENDING_SPANS = 5_000;
const MAX_SPAN_NAME_CHARS = 160;
const MAX_ATTR_STRING_CHARS = 1_000;
const RUNLESS_HOOK_ATTACH_WINDOW_MS = 5 * 60 * 1000;
const RUNLESS_HOOK_AMBIGUITY_MS = 1_000;
const LOOPBACK_INGEST_HEADER = "x-openclaw-live-lens-local-ingest";
const LOCAL_DASHBOARD_PATH = "/openclaw-lens/dashboard";
const LOCAL_SPANS_PATH = "/openclaw-lens/spans";
const LOCAL_REPORT_PATH = "/openclaw-lens/report";
const LOCAL_E2E_TEST_PATH = "/openclaw-lens/e2e-test";
const LOCAL_LIVE_TEST_PATH = "/openclaw-lens/live-test";
const LOCAL_INGEST_SPANS_PATH = "/openclaw-lens/ingest/spans";
const LOCAL_CLEAR_TEST_SPANS_PATH = "/openclaw-lens/test-spans/clear";
const API_SPANS_PATH = "/api/openclaw-lens/spans";
const API_REPORT_PATH = "/api/openclaw-lens/report";
const API_INGEST_SPANS_PATH = "/api/openclaw-lens/ingest/spans";
const API_LIBRAVDB_SPANS_PATH = "/api/openclaw-lens/libravdb/spans";
const LIVE_LENS_FAVICON_HREF =
  "data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 64 64'%3E%3Crect width='64' height='64' rx='14' fill='%23171715'/%3E%3Ctext x='32' y='42' text-anchor='middle' font-size='34'%3E%F0%9F%94%8D%3C/text%3E%3C/svg%3E";
const LIVE_TEST_SESSION_PREFIX = "agent:main:dashboard:live-lens-live-test";
const LIVE_TEST_TOOL_NAME = "live_lens_probe";
const LIVE_TEST_MESSAGE =
  "Live Lens live test. Reply with exactly: Live Lens telemetry test complete.";
const LIVE_TEST_TOOL_MESSAGE_PREFIX =
  "Live Lens live test. First call the live_lens_probe tool exactly once with the provided nonce, then reply with exactly: Live Lens telemetry test complete.";
const SENSITIVE_KEY_RE =
  /(content|prompt|message|body|text|query|url|uri|path|file|token|secret|password|credential|cookie|authorization|apikey|api_key|email|phone|address|sessionkey)/i;
const METRIC_KEY_RE =
  /(count|chars|bytes|ms|duration|elapsed|size|length|tokens?|tokenbudget|resultcount|cachehit|status|success|haserror|kind|phase|provider|model|api|transport|outcome|failurekind|toolname|toolkind|toolinputkind|paramkeys|derivedpathcount|hash)$/i;

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

type SpanView = SpanRecord & {
  createdAtMs: number;
};

type DashboardDurationSource = HookObservation["source"] | "stored" | "paired-ended" | "point";

type DashboardSpanView = SpanView & {
  observedDurationMs?: number;
  durationSource?: DashboardDurationSource;
};

type ReportRow = Record<string, unknown>;

type PerformanceReport = {
  ok: true;
  generatedAtMs: number;
  spanCount: number;
  simpleRows: ReportRow[];
  toolRows: ReportRow[];
  hookRows: ReportRow[];
  subcostRows: ReportRow[];
};

type ReportGroup = {
  key: string;
  spans: SpanView[];
};

type HookObservation = {
  durationMs: number;
  source: "emitted" | "derived-next-span";
};

type LensStore = {
  insert(span: SpanRecord): void;
  list(params: { limit: number; runId?: string; sessionHash?: string }): SpanView[];
  deleteTestSpans(): number;
  close(): void;
};

type LiveTestResult = {
  runId: string;
  sessionKey: string;
  sessionHash: string;
  toolName?: string;
  toolNonce?: string;
  createdSession: boolean;
  sendAccepted: boolean;
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

function createStore(dbPath: string, onError: (error: unknown) => void): LensStore {
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
  const deleteTestSpansStmt = db.prepare(`
    DELETE FROM spans
    WHERE attributes_json LIKE '%"testSeed":true%'
  `);
  const pending: SpanRecord[] = [];
  let drainHandle: NodeJS.Immediate | undefined;
  let closed = false;

  const flushPending = () => {
    if (pending.length === 0) {
      return;
    }
    const batch = pending.splice(0, pending.length);
    try {
      db.exec("BEGIN");
      for (const span of batch) {
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
          telemetryNowMs(),
        );
      }
      db.exec("COMMIT");
    } catch (error) {
      try {
        db.exec("ROLLBACK");
      } catch {
        // Ignore rollback failures; the original write error is the useful signal.
      }
      onError(error);
    }
  };

  const scheduleDrain = () => {
    if (drainHandle || closed) {
      return;
    }
    drainHandle = setImmediate(() => {
      drainHandle = undefined;
      if (!closed) {
        flushPending();
      }
    });
  };

  return {
    insert(span) {
      if (closed) {
        return;
      }
      pending.push(span);
      if (pending.length > MAX_PENDING_SPANS) {
        pending.splice(0, pending.length - MAX_PENDING_SPANS);
      }
      scheduleDrain();
    },
    list(params) {
      flushPending();
      const rows = listStmt.all(
        params.runId ?? null,
        params.runId ?? null,
        params.sessionHash ?? null,
        params.sessionHash ?? null,
        params.limit,
      );
      return rows.map((row) => {
        const record = row as Record<string, unknown>;
        const startedAtMs = readFiniteNumber(record.started_at_ms) ?? 0;
        const parsedAttributes = safeJsonParse(record.attributes_json);
        return {
          spanId: normalizeString(record.span_id, ""),
          parentSpanId: normalizeOptionalString(record.parent_span_id),
          source: normalizeString(record.source, "unknown"),
          name: normalizeString(record.name, "unknown"),
          phase: normalizeOptionalString(record.phase),
          runId: normalizeOptionalString(record.run_id),
          callId: normalizeOptionalString(record.call_id),
          toolCallId: normalizeOptionalString(record.tool_call_id),
          sessionHash: normalizeOptionalString(record.session_hash),
          agentId: normalizeOptionalString(record.agent_id),
          channelId: normalizeOptionalString(record.channel_id),
          startedAtMs,
          endedAtMs: readFiniteNumber(record.ended_at_ms),
          durationMs: readFiniteNumber(record.duration_ms),
          attributes: isRecord(parsedAttributes) ? parsedAttributes : {},
          createdAtMs: readFiniteNumber(record.created_at_ms) ?? startedAtMs,
        };
      });
    },
    deleteTestSpans() {
      flushPending();
      const result = deleteTestSpansStmt.run() as { changes?: bigint | number };
      return Number(result.changes ?? 0);
    },
    close() {
      if (drainHandle) {
        clearImmediate(drainHandle);
        drainHandle = undefined;
      }
      flushPending();
      closed = true;
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
      startedAtMs: telemetryNowMs(),
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
    const endedAtMs = telemetryNowMs();
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
    const endedAtMs = telemetryNowMs();
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
        toolKind: readEventString(event, "toolKind") ?? ctx.toolKind,
        toolInputKind: readEventString(event, "toolInputKind") ?? ctx.toolInputKind,
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
    const endedAtMs = telemetryNowMs();
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

function registerLiveTestTool(api: OpenClawPluginApi): void {
  const tool: AnyAgentTool = {
    name: LIVE_TEST_TOOL_NAME,
    label: "Live Lens Probe",
    description:
      "Probe tool used by the Live Lens dashboard live test to emit real tool-call telemetry.",
    parameters: {
      type: "object",
      properties: {
        nonce: {
          type: "string",
          description: "Opaque nonce copied from the Live Lens live-test prompt.",
        },
      },
      required: ["nonce"],
      additionalProperties: false,
    } as AnyAgentTool["parameters"],
    async execute(_toolCallId, params) {
      const nonce = isRecord(params) ? normalizeOptionalString(params.nonce) : undefined;
      return {
        content: [
          {
            type: "text",
            text: `Live Lens probe acknowledged${nonce ? ` nonce ${nonce}` : ""}.`,
          },
        ],
        details: {
          ok: true,
          toolName: LIVE_TEST_TOOL_NAME,
          noncePresent: Boolean(nonce),
        },
      };
    },
  };
  api.registerTool(tool);
}

function registerHttpRoutes(api: OpenClawPluginApi, store: LensStore, config: LensConfig) {
  api.registerHttpRoute({
    path: LOCAL_DASHBOARD_PATH,
    auth: "plugin",
    match: "exact",
    handler: (req, res) => {
      if (!isLoopbackRequest(req)) {
        sendJson(res, 403, { ok: false, error: "localhost_only" });
        return true;
      }
      sendHtml(res, 200, renderDashboardHtml());
      return true;
    },
  });

  api.registerHttpRoute({
    path: LOCAL_SPANS_PATH,
    auth: "plugin",
    match: "exact",
    handler: (req, res) => {
      if (!isLoopbackRequest(req)) {
        sendJson(res, 403, { ok: false, error: "localhost_only" });
        return true;
      }
      sendSpansResponse(req, res, store);
      return true;
    },
  });

  api.registerHttpRoute({
    path: LOCAL_REPORT_PATH,
    auth: "plugin",
    match: "exact",
    handler: (req, res) => {
      if (!isLoopbackRequest(req)) {
        sendJson(res, 403, { ok: false, error: "localhost_only" });
        return true;
      }
      sendReportResponse(req, res, store, "html");
      return true;
    },
  });

  api.registerHttpRoute({
    path: LOCAL_INGEST_SPANS_PATH,
    auth: "plugin",
    match: "exact",
    handler: async (req, res) => {
      if (!isLoopbackRequest(req)) {
        sendJson(res, 403, { ok: false, error: "localhost_only" });
        return true;
      }
      if (!hasLoopbackIngestHeader(req)) {
        sendJson(res, 403, {
          ok: false,
          error: "local_ingest_header_required",
          header: LOOPBACK_INGEST_HEADER,
        });
        return true;
      }
      await handleSpanIngest(req, res, store, config, "external");
      return true;
    },
  });

  api.registerHttpRoute({
    path: LOCAL_E2E_TEST_PATH,
    auth: "plugin",
    match: "exact",
    handler: (req, res) => {
      if (!isLoopbackRequest(req)) {
        sendJson(res, 403, { ok: false, error: "localhost_only" });
        return true;
      }
      if (req.method !== "POST") {
        sendJson(res, 405, { ok: false, error: "method_not_allowed" });
        return true;
      }
      const result = recordDashboardE2eTest(store, config);
      sendJson(res, 202, { ok: true, ...result });
      return true;
    },
  });

  api.registerHttpRoute({
    path: LOCAL_LIVE_TEST_PATH,
    auth: "plugin",
    match: "exact",
    handler: async (req, res) => {
      if (!isLoopbackRequest(req)) {
        sendJson(res, 403, { ok: false, error: "localhost_only" });
        return true;
      }
      if (req.method !== "POST") {
        sendJson(res, 405, { ok: false, error: "method_not_allowed" });
        return true;
      }
      try {
        const url = new URL(req.url ?? "", "http://localhost");
        const result = await runLiveGatewayTest({ includeTools: url.searchParams.get("tools") === "1" });
        sendJson(res, 202, { ok: true, ...result });
      } catch (error) {
        sendJson(res, 502, {
          ok: false,
          error: "live_test_failed",
          message: error instanceof Error ? error.message : String(error),
        });
      }
      return true;
    },
  });

  api.registerHttpRoute({
    path: LOCAL_CLEAR_TEST_SPANS_PATH,
    auth: "plugin",
    match: "exact",
    handler: (req, res) => {
      if (!isLoopbackRequest(req)) {
        sendJson(res, 403, { ok: false, error: "localhost_only" });
        return true;
      }
      if (req.method !== "POST") {
        sendJson(res, 405, { ok: false, error: "method_not_allowed" });
        return true;
      }
      const deleted = store.deleteTestSpans();
      sendJson(res, 202, { ok: true, deleted });
      return true;
    },
  });

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
    path: API_SPANS_PATH,
    auth: "gateway",
    match: "exact",
    gatewayRuntimeScopeSurface: "trusted-operator",
    handler: (req, res) => {
      sendSpansResponse(req, res, store);
      return true;
    },
  });

  api.registerHttpRoute({
    path: API_REPORT_PATH,
    auth: "gateway",
    match: "exact",
    gatewayRuntimeScopeSurface: "trusted-operator",
    handler: (req, res) => {
      sendReportResponse(req, res, store, "json");
      return true;
    },
  });

  api.registerHttpRoute({
    path: API_INGEST_SPANS_PATH,
    auth: "gateway",
    match: "exact",
    gatewayRuntimeScopeSurface: "trusted-operator",
    handler: async (req, res) => {
      await handleSpanIngest(req, res, store, config, "external");
      return true;
    },
  });

  api.registerHttpRoute({
    path: API_LIBRAVDB_SPANS_PATH,
    auth: "gateway",
    match: "exact",
    gatewayRuntimeScopeSurface: "trusted-operator",
    handler: async (req, res) => {
      await handleSpanIngest(req, res, store, config, "libravdb");
      return true;
    },
  });
}

function recordDashboardE2eTest(
  store: LensStore,
  config: LensConfig,
): { runId: string; accepted: number } {
  const now = telemetryNowMs();
  const runId = `e2e-test-${now}`;
  const sessionKey = `live-lens:e2e:${randomUUID()}`;
  const base = {
    runId,
    sessionKey,
    agentId: "local-dashboard-e2e",
    channelId: "local",
  };
  const attrs = {
    testSeed: true,
    e2eTest: true,
    generatedBy: "dashboard",
    runKind: "local-smoke",
  };
  const spans: SpanInput[] = [
    {
      ...base,
      source: "openclaw-hook",
      name: "openclaw.message_received",
      phase: "message",
      startedAtMs: now,
      attributes: {
        ...attrs,
        provider: "dashboard",
        messageCount: 1,
      },
    },
    {
      ...base,
      source: "openclaw-hook",
      name: "openclaw.before_prompt_build",
      phase: "context",
      startedAtMs: now + 30,
      attributes: {
        ...attrs,
        messageCount: 1,
        promptChars: 84,
        contextTokenBudget: 4096,
        contextWindowSource: "dashboard-e2e",
      },
    },
    {
      ...base,
      source: "libravdb",
      name: "libravdb.daemon.assembleContextInternal",
      phase: "memory",
      startedAtMs: now + 80,
      endedAtMs: now + 206,
      durationMs: 126,
      attributes: {
        ...attrs,
        messageCount: 1,
        requestBytes: 512,
        responseBytes: 2048,
        derivedPathCount: 2,
        cacheHit: false,
      },
    },
    {
      ...base,
      source: "openclaw-hook",
      name: "openclaw.before_tool_call",
      phase: "tool",
      toolCallId: `tool-${runId}`,
      startedAtMs: now + 230,
      attributes: {
        ...attrs,
        toolName: "live_lens_e2e_tool",
        toolKind: "local",
        toolInputKind: "json",
        paramKeys: ["operation"],
        derivedPathCount: 0,
      },
    },
    {
      ...base,
      source: "openclaw-hook",
      name: "openclaw.tool_call",
      phase: "tool",
      toolCallId: `tool-${runId}`,
      startedAtMs: now + 242,
      endedAtMs: now + 319,
      durationMs: 77,
      attributes: {
        ...attrs,
        toolName: "live_lens_e2e_tool",
        toolKind: "local",
        toolInputKind: "json",
        hasError: false,
        resultKind: "object",
        paramKeys: ["operation"],
      },
    },
    {
      ...base,
      source: "openclaw-hook",
      name: "openclaw.llm_input",
      phase: "model",
      startedAtMs: now + 360,
      attributes: {
        ...attrs,
        provider: "dashboard",
        model: "local-e2e",
        systemPromptChars: 64,
        promptChars: 84,
        historyCount: 1,
        imagesCount: 0,
        toolCount: 1,
        contextTokenBudget: 4096,
      },
    },
    {
      ...base,
      source: "openclaw-hook",
      name: "openclaw.model_call",
      phase: "model",
      callId: `model-${runId}`,
      startedAtMs: now + 390,
      attributes: {
        ...attrs,
        status: "started",
        provider: "dashboard",
        model: "local-e2e",
        api: "responses",
        transport: "local-smoke",
        contextTokenBudget: 4096,
      },
    },
    {
      ...base,
      source: "openclaw-hook",
      name: "openclaw.model_call",
      phase: "model",
      callId: `model-${runId}`,
      startedAtMs: now + 390,
      endedAtMs: now + 812,
      durationMs: 422,
      attributes: {
        ...attrs,
        status: "ended",
        provider: "dashboard",
        model: "local-e2e",
        outcome: "success",
        requestPayloadBytes: 1536,
        responseStreamBytes: 768,
        timeToFirstByteMs: 118,
        contextTokenBudget: 4096,
      },
    },
    {
      ...base,
      source: "openclaw-hook",
      name: "openclaw.llm_output",
      phase: "model",
      startedAtMs: now + 828,
      attributes: {
        ...attrs,
        provider: "dashboard",
        model: "local-e2e",
        harnessId: "dashboard-e2e",
        outputTextCount: 1,
        usageInput: 128,
        usageOutput: 42,
        usageTotal: 170,
        usageCacheRead: 0,
        usageCacheWrite: 0,
        contextTokenBudget: 4096,
      },
    },
    {
      ...base,
      source: "openclaw-hook",
      name: "openclaw.agent_run",
      phase: "agent",
      startedAtMs: now,
      endedAtMs: now + 930,
      durationMs: 930,
      attributes: {
        ...attrs,
        success: true,
        messageCount: 1,
        hasError: false,
      },
    },
  ];
  for (const span of spans) {
    recordSpan(store, config, span);
  }
  return { runId, accepted: spans.length };
}

async function runLiveGatewayTest(options: { includeTools: boolean }): Promise<LiveTestResult> {
  const runId = `live-lens-live-test-${telemetryNowMs()}-${randomUUID().slice(0, 8)}`;
  const sessionKey = `${LIVE_TEST_SESSION_PREFIX}-${runId.slice("live-lens-live-test-".length)}`;
  const toolNonce = options.includeTools ? randomUUID().slice(0, 12) : undefined;
  let createdSession = false;
  try {
    await runOpenClawGatewayCall("sessions.create", {
      key: sessionKey,
      agentId: "main",
      label: `Live Lens live test ${runId.slice("live-lens-live-test-".length)}`,
    });
    createdSession = true;
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    if (!/already exists|already has|duplicate|exists/i.test(message)) {
      throw error;
    }
  }
  const send = await runOpenClawGatewayCall("sessions.send", {
    key: sessionKey,
    message: options.includeTools
      ? `${LIVE_TEST_TOOL_MESSAGE_PREFIX} Nonce: ${toolNonce}.`
      : LIVE_TEST_MESSAGE,
    idempotencyKey: runId,
  });
  const acceptedRunId = normalizeOptionalString(send.runId) ?? runId;
  return {
    runId: acceptedRunId,
    sessionKey,
    sessionHash: hashValue(sessionKey),
    ...(options.includeTools ? { toolName: LIVE_TEST_TOOL_NAME, toolNonce } : {}),
    createdSession,
    sendAccepted: true,
  };
}

function runOpenClawGatewayCall(method: string, params: Record<string, unknown>): Promise<Record<string, unknown>> {
  return new Promise((resolve, reject) => {
    execFile(
      "openclaw",
      ["gateway", "call", method, "--json", "--timeout", "15000", "--params", JSON.stringify(params)],
      {
        timeout: 20_000,
        maxBuffer: 1024 * 1024,
      },
      (error, stdout, stderr) => {
        const parsed = parseJson(stdout.trim());
        const payload = parsed.ok && isRecord(parsed.value) ? parsed.value : undefined;
        if (error) {
          reject(new Error(extractGatewayCallError(payload, stderr || stdout || error.message)));
          return;
        }
        if (!payload) {
          reject(new Error("Gateway call returned non-JSON output."));
          return;
        }
        if (payload.ok === false) {
          const err = isRecord(payload.error) ? payload.error : {};
          reject(new Error(normalizeOptionalString(err.message) ?? `Gateway method ${method} failed.`));
          return;
        }
        resolve(isRecord(payload.payload) ? payload.payload : {});
      },
    );
  });
}

function extractGatewayCallError(payload: Record<string, unknown> | undefined, fallback: string): string {
  if (payload && isRecord(payload.error)) {
    const message = normalizeOptionalString(payload.error.message);
    if (message) {
      return message;
    }
  }
  return fallback.trim() || "Gateway call failed.";
}

function sendSpansResponse(req: IncomingMessage, res: ServerResponse, store: LensStore): void {
  const url = new URL(req.url ?? "", "http://localhost");
  const limit = clampNumber(Number(url.searchParams.get("limit") ?? 100), 100, 1, 500);
  const runId = normalizeOptionalString(url.searchParams.get("runId"));
  const sessionKey = normalizeOptionalString(url.searchParams.get("sessionKey"));
  const sessionHash = normalizeOptionalString(url.searchParams.get("sessionHash")) ??
    (sessionKey ? hashValue(sessionKey) : undefined);
  const spans = listReportSpans(store, { limit, runId, sessionHash });
  sendJson(res, 200, {
    ok: true,
    spans: enrichDashboardSpans(spans),
  });
}

function sendReportResponse(
  req: IncomingMessage,
  res: ServerResponse,
  store: LensStore,
  defaultFormat: "html" | "json",
): void {
  if (req.method && req.method !== "GET") {
    sendJson(res, 405, { ok: false, error: "method_not_allowed" });
    return;
  }
  const url = new URL(req.url ?? "", "http://localhost");
  const limit = clampNumber(Number(url.searchParams.get("limit") ?? 500), 500, 1, 1_000);
  const runId = normalizeOptionalString(url.searchParams.get("runId"));
  const sessionKey = normalizeOptionalString(url.searchParams.get("sessionKey"));
  const sessionHash = normalizeOptionalString(url.searchParams.get("sessionHash")) ??
    (sessionKey ? hashValue(sessionKey) : undefined);
  const spans = listReportSpans(store, { limit, runId, sessionHash });
  const report = buildPerformanceReport(spans);
  if (resolveReportFormat(req, url, defaultFormat) === "html") {
    sendHtml(res, 200, renderReportHtml(report, url));
    return;
  }
  sendJson(res, 200, report);
}

function listReportSpans(
  store: LensStore,
  params: { limit: number; runId?: string; sessionHash?: string },
): SpanView[] {
  const primary = store.list(params);
  if (!params.runId || params.sessionHash || primary.length === 0) {
    return primary;
  }
  const byId = new Map(primary.map((span) => [span.spanId, span]));
  const windows = buildSessionWindows(primary);
  for (const sessionHash of windows.keys()) {
    const sessionSpans = store.list({ limit: params.limit, sessionHash });
    const runGroups = reportGroupsFromMap(buildExplicitRunGroups(sessionSpans));
    for (const span of sessionSpans) {
      if (span.runId || byId.has(span.spanId)) {
        continue;
      }
      const group = findNearestReportRunGroup(span, runGroups);
      if (group?.key === params.runId) {
        byId.set(span.spanId, span);
      }
    }
  }
  return [...byId.values()];
}

function enrichDashboardSpans(spans: SpanView[]): DashboardSpanView[] {
  const sorted = [...spans].sort(compareSpanTime);
  const observations = buildHookObservations(groupReportSpans(sorted));
  return spans.map((span) => {
    const observation = observations.get(span.spanId);
    const pairedEnded = observation || hasDuration(span) ? undefined : findPairedEndedSpan(span, sorted);
    const durationMs = observation?.durationMs ?? span.durationMs ?? pairedEnded?.durationMs ?? 0;
    const durationSource: DashboardDurationSource = observation?.source ??
      (hasDuration(span) ? "stored" : pairedEnded ? "paired-ended" : "point");
    return removeUndefined({
      ...span,
      observedDurationMs: roundDuration(durationMs),
      durationSource,
    }) as DashboardSpanView;
  });
}

function findPairedEndedSpan(span: SpanView, spans: SpanView[]): SpanView | undefined {
  if (readSpanString(span, ["status"]) !== "started") {
    return undefined;
  }
  let best: { span: SpanView; distanceMs: number } | undefined;
  for (const candidate of spans) {
    if (!hasDuration(candidate) || readSpanString(candidate, ["status"]) !== "ended") {
      continue;
    }
    if (lifecycleSpanKey(candidate) !== lifecycleSpanKey(span)) {
      continue;
    }
    const distanceMs = distanceToWindow(span, readSpanWindow([candidate]));
    if (!best || distanceMs < best.distanceMs) {
      best = { span: candidate, distanceMs };
    }
  }
  return best?.span;
}

function lifecycleSpanKey(span: SpanView): string {
  return [
    span.runId ?? "",
    span.sessionHash ?? "",
    span.callId ?? "",
    span.toolCallId ?? "",
    span.source,
    span.name,
    span.phase ?? "",
  ].join("\0");
}

async function handleSpanIngest(
  req: IncomingMessage,
  res: ServerResponse,
  store: LensStore,
  config: LensConfig,
  defaultSource: string,
): Promise<void> {
  if (req.method !== "POST") {
    sendJson(res, 405, { ok: false, error: "method_not_allowed" });
    return;
  }
  if (!config.enabled) {
    sendJson(res, 202, { ok: true, accepted: 0, disabled: true });
    return;
  }
  const bodyResult = await readRequestBodyResult(req, config.maxIngestBytes);
  if (!bodyResult.ok) {
    sendJson(res, bodyResult.tooLarge ? 413 : 400, {
      ok: false,
      error: bodyResult.tooLarge ? "body_too_large" : "body_read_failed",
    });
    return;
  }
  const payload = parseJson(bodyResult.body);
  if (!payload.ok) {
    sendJson(res, 400, { ok: false, error: "invalid_json" });
    return;
  }
  const spans = normalizeIngestedSpans(payload.value);
  if (spans.length === 0) {
    sendJson(res, 400, { ok: false, error: "no_valid_spans" });
    return;
  }
  let accepted = 0;
  for (const span of spans.slice(0, MAX_SPANS_PER_INGEST)) {
    recordSpan(store, config, {
      ...span,
      source: normalizeOptionalString(span.source) ?? defaultSource,
    });
    accepted += 1;
  }
  sendJson(res, 202, {
    ok: true,
    accepted,
    dropped: Math.max(0, spans.length - accepted),
  });
}

function resolveReportFormat(
  req: IncomingMessage,
  url: URL,
  defaultFormat: "html" | "json",
): "html" | "json" {
  const format = normalizeOptionalString(url.searchParams.get("format"))?.toLowerCase();
  if (format === "html" || format === "json") {
    return format;
  }
  const rawAccept = (req as IncomingMessage & { headers?: { accept?: unknown } }).headers?.accept;
  const accept = (Array.isArray(rawAccept) ? rawAccept.join(",") : normalizeOptionalString(rawAccept))
    ?.toLowerCase() ?? "";
  if (accept.includes("text/html") && !accept.includes("application/json")) {
    return "html";
  }
  if (accept.includes("application/json") && !accept.includes("text/html")) {
    return "json";
  }
  return defaultFormat;
}

function buildPerformanceReport(spans: SpanView[]): PerformanceReport {
  const sorted = [...spans].sort(compareSpanTime);
  const groups = groupReportSpans(sorted);
  return {
    ok: true,
    generatedAtMs: telemetryNowMs(),
    spanCount: sorted.length,
    simpleRows: [...groups.values()].map(buildSimpleReportRow),
    toolRows: [...groups.values()].map(buildToolReportRow).filter((row): row is ReportRow => row !== undefined),
    hookRows: buildHookRows(sorted, groups),
    subcostRows: buildSubcostRows(sorted),
  };
}

function groupReportSpans(spans: SpanView[]): Map<string, SpanView[]> {
  const groups = buildExplicitRunGroups(spans);
  const runGroups = reportGroupsFromMap(groups);
  const deferred = spans.filter((span) => !span.runId);
  for (const span of deferred) {
    const group = findNearestReportRunGroup(span, runGroups);
    if (group) {
      group.spans.push(span);
      continue;
    }
    const key = span.sessionHash ? `session:${span.sessionHash}` : "ungrouped";
    const fallback = groups.get(key);
    if (fallback) {
      fallback.push(span);
    } else {
      groups.set(key, [span]);
    }
  }
  return groups;
}

function reportGroupsFromMap(groups: Map<string, SpanView[]>): ReportGroup[] {
  return [...groups.entries()].map(([key, spans]) => ({ key, spans }));
}

function buildExplicitRunGroups(spans: SpanView[]): Map<string, SpanView[]> {
  const groups = new Map<string, SpanView[]>();
  for (const span of spans) {
    if (!span.runId) {
      continue;
    }
    const key = span.runId;
    let group = groups.get(key);
    if (group) {
      group.push(span);
    } else {
      group = [span];
      groups.set(key, group);
    }
  }
  return groups;
}

function findNearestReportRunGroup(span: SpanView, groups: ReportGroup[]): ReportGroup | undefined {
  if (!span.sessionHash) {
    return undefined;
  }
  const candidates: Array<{ group: ReportGroup; distanceMs: number }> = [];
  for (const group of groups) {
    if (group.spans.every((candidate) => candidate.sessionHash !== span.sessionHash)) {
      continue;
    }
    const distanceMs = distanceToWindow(span, readSpanWindow(group.spans));
    if (distanceMs > RUNLESS_HOOK_ATTACH_WINDOW_MS) {
      continue;
    }
    candidates.push({ group, distanceMs });
  }
  candidates.sort((a, b) => a.distanceMs - b.distanceMs);
  const [best, second] = candidates;
  if (!best) {
    return undefined;
  }
  if (second && second.distanceMs - best.distanceMs <= RUNLESS_HOOK_AMBIGUITY_MS) {
    return undefined;
  }
  return best.group;
}

function buildHookObservations(reportGroups: Map<string, SpanView[]>): Map<string, HookObservation> {
  const observations = new Map<string, HookObservation>();
  for (const spans of reportGroups.values()) {
    const sorted = [...spans].sort(compareSpanTime);
    for (let index = 0; index < sorted.length; index += 1) {
      const span = sorted[index];
      if (span.source !== "openclaw-hook") {
        continue;
      }
      const durationMs = span.durationMs;
      if (typeof durationMs === "number" && Number.isFinite(durationMs)) {
        observations.set(span.spanId, {
          durationMs,
          source: "emitted",
        });
        continue;
      }
      const next = sorted.slice(index + 1).find((candidate) => candidate.startedAtMs >= span.startedAtMs);
      if (next) {
        observations.set(span.spanId, {
          durationMs: Math.max(0, next.startedAtMs - span.startedAtMs),
          source: "derived-next-span",
        });
      }
    }
  }
  return observations;
}

function latestSpan(spans: SpanView[]): SpanView {
  return spans.reduce((best, span) => span.startedAtMs > best.startedAtMs ? span : best, spans[0]);
}

function buildSessionWindows(spans: SpanView[]): Map<string, { startMs: number; endMs: number }> {
  const windows = new Map<string, { startMs: number; endMs: number }>();
  for (const span of spans) {
    if (!span.sessionHash) {
      continue;
    }
    const existing = windows.get(span.sessionHash);
    const spanWindow = readSpanWindow([span]);
    windows.set(span.sessionHash, existing
      ? {
        startMs: Math.min(existing.startMs, spanWindow.startMs),
        endMs: Math.max(existing.endMs, spanWindow.endMs),
      }
      : spanWindow);
  }
  return windows;
}

function readSpanWindow(spans: SpanView[]): { startMs: number; endMs: number } {
  return {
    startMs: Math.min(...spans.map((span) => span.startedAtMs)),
    endMs: Math.max(...spans.map(spanEndMs)),
  };
}

function distanceToWindow(span: SpanView, window: { startMs: number; endMs: number }): number {
  const spanEnd = spanEndMs(span);
  if (spanEnd < window.startMs) {
    return window.startMs - spanEnd;
  }
  if (span.startedAtMs > window.endMs) {
    return span.startedAtMs - window.endMs;
  }
  return 0;
}

function buildSimpleReportRow(spans: SpanView[]): ReportRow {
  const modelSpans = selectModelTimingSpans(spans);
  const totalMs = roundDuration(readTurnDuration(spans));
  const modelMs = roundDuration(sumDurations(modelSpans));
  return removeUndefined({
    case: readReportLabel(spans),
    totalMs,
    ollamaHttpMs: modelMs,
    nonOllamaOverheadMs: totalMs !== undefined && modelMs !== undefined ? Math.max(0, totalMs - modelMs) : undefined,
    inputTokens: readFirstSpanNumber(spans, ["promptEvalCount", "prompt_eval_count", "inputTokens", "usageInput"]),
    tools: readFirstSpanNumber(spans, ["toolCount", "tools"]),
    runId: spans.find((span) => span.runId)?.runId,
    sessionHash: spans.find((span) => span.sessionHash)?.sessionHash,
  });
}

function buildToolReportRow(spans: SpanView[]): ReportRow | undefined {
  const modelSpans = selectModelTimingSpans(spans);
  if (modelSpans.length < 2) {
    return undefined;
  }
  const firstModel = modelSpans[0];
  const secondModel = modelSpans[1];
  const firstEnd = spanEndMs(firstModel);
  const secondStart = secondModel.startedAtMs;
  const toolSpans = selectToolSpans(spans)
    .filter((span) => spanEndMs(span) >= firstEnd && span.startedAtMs <= secondStart)
    .sort(compareSpanTime);
  const toolEnd = toolSpans.length > 0
    ? Math.max(...toolSpans.map(spanEndMs))
    : firstEnd;
  const turnStart = readTurnStart(spans);
  return removeUndefined({
    runShape: readReportLabel(spans),
    totalMs: roundDuration(readTurnDuration(spans)),
    preFirstOllamaMs: roundDuration(firstModel.startedAtMs - turnStart),
    firstOllamaMs: roundDuration(firstModel.durationMs),
    toolMs: toolSpans.length > 0 ? roundDuration(sumDurations(toolSpans)) : undefined,
    toolDoneToSecondOllamaMs: roundDuration(Math.max(0, secondStart - toolEnd)),
    secondOllamaMs: roundDuration(secondModel.durationMs),
    toolName: toolSpans.map((span) => readSpanString(span, ["toolName"])).find(Boolean),
    runId: spans.find((span) => span.runId)?.runId,
  });
}

function buildHookRows(spans: SpanView[], reportGroups: Map<string, SpanView[]>): ReportRow[] {
  const observations = buildHookObservations(reportGroups);
  const groups = new Map<string, SpanView[]>();
  for (const span of spans.filter((candidate) => candidate.source === "openclaw-hook")) {
    const key = `${span.phase ?? ""}\n${span.name}`;
    const group = groups.get(key);
    if (group) {
      group.push(span);
    } else {
      groups.set(key, [span]);
    }
  }
  return [...groups.values()]
    .map((group) => {
      const observedGroup = group.filter((span) => observations.has(span.spanId));
      const latest = latestSpan(observedGroup.length > 0 ? observedGroup : group);
      const durations = group
        .map((span) => span.durationMs)
        .filter((value): value is number => typeof value === "number" && Number.isFinite(value));
      const observedDurations = group
        .map((span) => observations.get(span.spanId)?.durationMs)
        .filter((value): value is number => typeof value === "number" && Number.isFinite(value));
      const latestObservation = observations.get(latest.spanId);
      return removeUndefined({
        phase: latest.phase,
        name: latest.name,
        count: group.length,
        latestAtMs: latest.startedAtMs,
        latestEmittedMs: roundDuration(latest.durationMs),
        latestObservedMs: roundDuration(latestObservation?.durationMs),
        maxEmittedMs: durations.length > 0 ? roundDuration(Math.max(...durations)) : undefined,
        maxObservedMs: observedDurations.length > 0 ? roundDuration(Math.max(...observedDurations)) : undefined,
        observationSource: latestObservation?.source,
        runId: latest.runId,
      });
    })
    .sort((a, b) => Number(b.latestAtMs ?? 0) - Number(a.latestAtMs ?? 0));
}

function buildSubcostRows(spans: SpanView[]): ReportRow[] {
  const groups = new Map<string, SpanView[]>();
  for (const span of spans.filter(hasDuration)) {
    const key = `${span.source}\n${span.phase ?? ""}\n${span.name}`;
    const group = groups.get(key);
    if (group) {
      group.push(span);
    } else {
      groups.set(key, [span]);
    }
  }
  return [...groups.values()]
    .map((group) => {
      const durations = group
        .map((span) => span.durationMs)
        .filter((value): value is number => typeof value === "number" && Number.isFinite(value));
      const latest = group.reduce((best, span) => span.startedAtMs > best.startedAtMs ? span : best, group[0]);
      return removeUndefined({
        source: latest.source,
        phase: latest.phase,
        name: latest.name,
        count: group.length,
        latestMs: roundDuration(latest.durationMs),
        minMs: roundDuration(Math.min(...durations)),
        maxMs: roundDuration(Math.max(...durations)),
        averageMs: roundDuration(durations.reduce((sum, value) => sum + value, 0) / durations.length),
      });
    })
    .sort((a, b) => Number(b.maxMs ?? 0) - Number(a.maxMs ?? 0));
}

function selectModelTimingSpans(spans: SpanView[]): SpanView[] {
  const providerHttp = spans.filter((span) => hasDuration(span) && isProviderHttpSpan(span));
  const modelCalls = spans.filter((span) =>
    hasDuration(span) && span.name === "openclaw.model_call" && readSpanString(span, ["status"]) !== "started"
  );
  return (providerHttp.length > 0 ? providerHttp : modelCalls).sort(compareSpanTime);
}

function selectToolSpans(spans: SpanView[]): SpanView[] {
  return spans.filter((span) =>
    hasDuration(span) && (span.name === "openclaw.tool_call" || span.phase === "tool")
  );
}

function isProviderHttpSpan(span: SpanView): boolean {
  const name = span.name.toLowerCase();
  const source = span.source.toLowerCase();
  const provider = readSpanString(span, ["provider"]);
  const model = readSpanString(span, ["model"]);
  const api = readSpanString(span, ["api"]);
  const hasModelSignal = Boolean(provider || model || api || span.phase === "model");
  if (source.includes("ollama")) {
    return hasModelSignal || name.includes("ollama");
  }
  return name.includes("http") &&
    hasModelSignal &&
    (name.includes("ollama") || name.includes("model") || name.includes("provider"));
}

function readTurnStart(spans: SpanView[]): number {
  const agentRun = spans.find((span) => span.name === "openclaw.agent_run" && typeof span.durationMs === "number");
  return agentRun?.startedAtMs ?? Math.min(...spans.map((span) => span.startedAtMs));
}

function readTurnDuration(spans: SpanView[]): number | undefined {
  const agentRun = spans.find((span) => span.name === "openclaw.agent_run" && typeof span.durationMs === "number");
  if (typeof agentRun?.durationMs === "number") {
    return agentRun.durationMs;
  }
  if (spans.length === 0) {
    return undefined;
  }
  const startedAtMs = Math.min(...spans.map((span) => span.startedAtMs));
  const endedAtMs = Math.max(...spans.map(spanEndMs));
  return endedAtMs - startedAtMs;
}

function readReportLabel(spans: SpanView[]): string {
  for (const key of ["case", "runShape", "label", "scenario"]) {
    const value = spans.map((span) => readSpanString(span, [key])).find(Boolean);
    if (value) {
      return value;
    }
  }
  const toolName = spans.map((span) => readSpanString(span, ["toolName"])).find(Boolean);
  if (toolName) {
    return `${toolName} run`;
  }
  const hookLabel = readHookTelemetryLabel(spans);
  if (hookLabel) {
    return hookLabel;
  }
  const telemetryLabel = readTelemetryLabel(spans);
  if (telemetryLabel) {
    return telemetryLabel;
  }
  return spans.find((span) => span.runId)?.runId ?? "Unlabeled run";
}

function readHookTelemetryLabel(spans: SpanView[]): string | undefined {
  if (spans.length === 0 || !spans.every((span) => span.source === "openclaw-hook")) {
    return undefined;
  }
  const names = uniqueSorted(spans.map((span) => span.name));
  if (names.length === 0) {
    return "OpenClaw hook telemetry";
  }
  if (names.length === 1) {
    return `OpenClaw hook: ${names[0]}`;
  }
  return `OpenClaw hooks: ${names[0]} + ${names.length - 1} more`;
}

function readTelemetryLabel(spans: SpanView[]): string | undefined {
  if (spans.length === 0) {
    return undefined;
  }
  const sources = uniqueSorted(spans.map((span) => span.source));
  if (sources.length === 1) {
    return `${sources[0]} telemetry`;
  }
  return `Telemetry: ${sources[0]} + ${sources.length - 1} more`;
}

function uniqueSorted(values: string[]): string[] {
  return [...new Set(values.filter((value) => value.trim()))].sort((a, b) => a.localeCompare(b));
}

function readFirstSpanNumber(spans: SpanView[], keys: string[]): number | undefined {
  for (const span of spans) {
    const attrs = span.attributes;
    for (const key of keys) {
      const value = attrs[key];
      if (typeof value === "number" && Number.isFinite(value)) {
        return value;
      }
    }
  }
  return undefined;
}

function readSpanString(span: SpanView, keys: string[]): string | undefined {
  for (const key of keys) {
    const value = span.attributes[key];
    if (typeof value === "string" && value.trim()) {
      return value;
    }
  }
  return undefined;
}

function hasDuration(span: SpanView): boolean {
  return typeof span.durationMs === "number" && Number.isFinite(span.durationMs);
}

function sumDurations(spans: SpanView[]): number | undefined {
  const durations = spans
    .map((span) => span.durationMs)
    .filter((value): value is number => typeof value === "number" && Number.isFinite(value));
  if (durations.length === 0) {
    return undefined;
  }
  return durations.reduce((sum, value) => sum + value, 0);
}

function spanEndMs(span: SpanView): number {
  return span.endedAtMs ?? span.startedAtMs + (span.durationMs ?? 0);
}

function roundDuration(value: number | undefined): number | undefined {
  return typeof value === "number" && Number.isFinite(value) ? Math.round(value) : undefined;
}

function compareSpanTime(a: SpanView, b: SpanView): number {
  return a.startedAtMs - b.startedAtMs || a.createdAtMs - b.createdAtMs;
}

function removeUndefined(value: Record<string, unknown>): Record<string, unknown> {
  return Object.fromEntries(Object.entries(value).filter(([, item]) => item !== undefined));
}

function recordSpan(store: LensStore, config: LensConfig, input: SpanInput): void {
  const name = normalizeString(input.name, "unknown").slice(0, MAX_SPAN_NAME_CHARS);
  const source = normalizeString(input.source, "unknown").slice(0, MAX_SPAN_NAME_CHARS);
  const timing = normalizeSpanTiming(input);
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
    ...timing,
    attributes: sanitizeAttributes(input.attributes, config),
  });
}

function normalizeSpanTiming(input: SpanInput): Pick<SpanRecord, "startedAtMs" | "endedAtMs" | "durationMs"> {
  const durationMs = readFiniteNumber(input.durationMs);
  const explicitStartedAtMs = readFiniteNumber(input.startedAtMs);
  const explicitEndedAtMs = readFiniteNumber(input.endedAtMs);
  if (explicitStartedAtMs !== undefined) {
    return {
      startedAtMs: explicitStartedAtMs,
      endedAtMs: explicitEndedAtMs ??
        (durationMs === undefined ? undefined : explicitStartedAtMs + durationMs),
      durationMs,
    };
  }
  if (explicitEndedAtMs !== undefined) {
    return {
      startedAtMs: durationMs === undefined ? explicitEndedAtMs : explicitEndedAtMs - durationMs,
      endedAtMs: explicitEndedAtMs,
      durationMs,
    };
  }
  const endedAtMs = telemetryNowMs();
  return {
    startedAtMs: durationMs === undefined ? endedAtMs : endedAtMs - durationMs,
    endedAtMs: durationMs === undefined ? undefined : endedAtMs,
    durationMs,
  };
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
  return rawSpans
    .filter(isRecord)
    .filter((span) => normalizeOptionalString(span.name))
    .map((span) => ({
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
    if (raw === undefined) {
      continue;
    }
    if (shouldRedactAttribute(key, config)) {
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

function shouldRedactAttribute(key: string, config: LensConfig): boolean {
  if (config.captureContent) {
    return false;
  }
  return SENSITIVE_KEY_RE.test(key) && !METRIC_KEY_RE.test(key);
}

function readRequestBodyResult(
  req: IncomingMessage,
  maxBytes: number,
): Promise<{ ok: true; body: string } | { ok: false; tooLarge: boolean }> {
  return new Promise((resolve) => {
    let total = 0;
    let settled = false;
    const chunks: Buffer[] = [];
    const settle = (value: { ok: true; body: string } | { ok: false; tooLarge: boolean }) => {
      if (settled) {
        return;
      }
      settled = true;
      resolve(value);
    };
    req.on("data", (chunk: Buffer) => {
      total += chunk.byteLength;
      if (total > maxBytes) {
        settle({ ok: false, tooLarge: true });
        req.destroy();
        return;
      }
      chunks.push(chunk);
    });
    req.on("end", () => settle({ ok: true, body: Buffer.concat(chunks).toString("utf8") }));
    req.on("error", () => settle({ ok: false, tooLarge: false }));
  });
}

function parseJson(value: string): { ok: true; value: unknown } | { ok: false } {
  try {
    return { ok: true, value: JSON.parse(value) };
  } catch {
    return { ok: false };
  }
}

function sendJson(res: ServerResponse, statusCode: number, payload: unknown): void {
  const body = JSON.stringify(payload);
  res.statusCode = statusCode;
  res.setHeader("content-type", "application/json; charset=utf-8");
  res.setHeader("content-length", Buffer.byteLength(body));
  res.end(body);
}

function sendHtml(res: ServerResponse, statusCode: number, body: string): void {
  res.statusCode = statusCode;
  res.setHeader("content-type", "text/html; charset=utf-8");
  res.setHeader("content-length", Buffer.byteLength(body));
  res.end(body);
}

function isLoopbackRequest(req: IncomingMessage): boolean {
  const remoteAddress = normalizeOptionalString(req.socket.remoteAddress) ?? "";
  return isLoopbackAddress(remoteAddress);
}

function hasLoopbackIngestHeader(req: IncomingMessage): boolean {
  const value = req.headers[LOOPBACK_INGEST_HEADER];
  return value === "1" || value === "true" || value === "yes";
}

function isLoopbackAddress(value: string): boolean {
  const address = value.trim().toLowerCase();
  if (!address) {
    return false;
  }
  if (address === "localhost" || address === "::1" || address === "0:0:0:0:0:0:0:1") {
    return true;
  }
  if (address.startsWith("::ffff:")) {
    return isLoopbackAddress(address.slice("::ffff:".length));
  }
  return /^127(?:\.\d{1,3}){3}$/.test(address);
}

function renderReportHtml(report: PerformanceReport, url: URL): string {
  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Live Lens Report</title>
  <link rel="icon" href="${LIVE_LENS_FAVICON_HREF}">
  <style>
    :root {
      color-scheme: dark;
      --bg: #0d0d0c;
      --panel: #171715;
      --panel-2: #1f1f1c;
      --ink: #f2f0e8;
      --muted: #a8a29a;
      --line: #34332f;
      --accent: #2dd4bf;
      --code-bg: #080908;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: var(--bg);
      color: var(--ink);
      font: 14px/1.45 ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      padding: 18px 24px;
      border-bottom: 1px solid var(--line);
      background: var(--panel);
      position: sticky;
      top: 0;
      z-index: 2;
    }
    h1 { margin: 0; font-size: 20px; letter-spacing: 0; }
    main { padding: 20px 24px 28px; display: grid; gap: 16px; }
    section {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      overflow: hidden;
    }
    h2 {
      margin: 0;
      padding: 13px 14px;
      color: var(--muted);
      background: var(--panel-2);
      font-size: 12px;
      letter-spacing: 0;
      text-transform: uppercase;
    }
    .meta {
      display: flex;
      flex-wrap: wrap;
      gap: 10px 16px;
      color: var(--muted);
      padding: 12px 14px;
      border-bottom: 1px solid var(--line);
    }
    a {
      color: var(--accent);
      text-decoration: none;
      font-weight: 700;
    }
    a:hover { text-decoration: underline; }
    .table-wrap { overflow: auto; }
    table { width: 100%; border-collapse: collapse; min-width: 760px; }
    th, td {
      padding: 9px 12px;
      border-top: 1px solid var(--line);
      text-align: left;
      white-space: nowrap;
      vertical-align: top;
    }
    th {
      color: var(--muted);
      background: var(--panel-2);
      font-size: 12px;
    }
    .empty { padding: 14px; color: var(--muted); }
    pre {
      margin: 0;
      padding: 12px;
      background: var(--code-bg);
      border-top: 1px solid var(--line);
      overflow: auto;
      max-height: 420px;
      font-size: 12px;
      line-height: 1.45;
    }
  </style>
</head>
<body>
  <header>
    <h1>Live Lens Report</h1>
    <a href="${escapeHtmlText(reportFormatHref(url, "json"))}">View JSON</a>
  </header>
  <main>
    <section>
      <h2>Summary</h2>
      <div class="meta">
        <span>Generated: ${escapeHtmlText(formatReportValue(report.generatedAtMs, "generatedAtMs"))}</span>
        <span>Spans: ${report.spanCount.toLocaleString()}</span>
        <span>Simple rows: ${report.simpleRows.length.toLocaleString()}</span>
        <span>Tool rows: ${report.toolRows.length.toLocaleString()}</span>
        <span>Hook rows: ${report.hookRows.length.toLocaleString()}</span>
      </div>
    </section>
    ${renderReportTable("Simple PONG / One-Shot Runs", report.simpleRows, [
      ["case", "Case"],
      ["totalMs", "Total"],
      ["ollamaHttpMs", "Ollama HTTP"],
      ["nonOllamaOverheadMs", "Non-Ollama"],
      ["inputTokens", "Input tokens"],
      ["tools", "Tools"],
      ["runId", "Run"],
    ])}
    ${renderReportTable("Tool Runs", report.toolRows, [
      ["runShape", "Run shape"],
      ["totalMs", "Total"],
      ["preFirstOllamaMs", "Pre-first Ollama"],
      ["firstOllamaMs", "First Ollama"],
      ["toolMs", "Tool"],
      ["toolDoneToSecondOllamaMs", "Tool done to second Ollama"],
      ["secondOllamaMs", "Second Ollama"],
      ["toolName", "Tool name"],
      ["runId", "Run"],
    ])}
    ${renderReportTable("Hook Integration", report.hookRows, [
      ["phase", "Phase"],
      ["name", "Hook span"],
      ["count", "Count"],
      ["latestAtMs", "Latest"],
      ["latestEmittedMs", "Latest emitted"],
      ["latestObservedMs", "Latest observed"],
      ["maxEmittedMs", "Max emitted"],
      ["maxObservedMs", "Max observed"],
      ["observationSource", "Source"],
      ["runId", "Run"],
    ])}
    ${renderReportTable("Sub-Costs", report.subcostRows, [
      ["source", "Source"],
      ["phase", "Phase"],
      ["name", "Name"],
      ["count", "Count"],
      ["latestMs", "Latest"],
      ["minMs", "Min"],
      ["maxMs", "Max"],
      ["averageMs", "Average"],
    ])}
    <section>
      <h2>Raw JSON</h2>
      <pre>${escapeHtmlText(JSON.stringify(report, null, 2))}</pre>
    </section>
  </main>
</body>
</html>`;
}

function renderReportTable(
  title: string,
  rows: ReportRow[],
  columns: Array<[key: string, label: string]>,
): string {
  if (rows.length === 0) {
    return `<section><h2>${escapeHtmlText(title)}</h2><div class="empty">No rows.</div></section>`;
  }
  return `<section>
    <h2>${escapeHtmlText(title)}</h2>
    <div class="table-wrap">
      <table>
        <thead><tr>${columns.map(([, label]) => `<th>${escapeHtmlText(label)}</th>`).join("")}</tr></thead>
        <tbody>
          ${rows.map((row) =>
            `<tr>${columns.map(([key]) => `<td>${escapeHtmlText(formatReportValue(row[key], key))}</td>`).join("")}</tr>`
          ).join("")}
        </tbody>
      </table>
    </div>
  </section>`;
}

function reportFormatHref(url: URL, format: "html" | "json"): string {
  const params = new URLSearchParams(url.searchParams);
  params.set("format", format);
  const query = params.toString();
  return `${url.pathname || LOCAL_REPORT_PATH}${query ? `?${query}` : ""}`;
}

function formatReportValue(value: unknown, key = ""): string {
  if (value === undefined || value === null || value === "") {
    return "-";
  }
  if (typeof value === "number") {
    if (/atms$/i.test(key) || key === "generatedAtMs") {
      return new Date(value).toLocaleString();
    }
    if (/ms$/i.test(key)) {
      return `${value.toLocaleString(undefined, { maximumFractionDigits: 1 })} ms`;
    }
    return value.toLocaleString();
  }
  return String(value);
}

function escapeHtmlText(value: unknown): string {
  return String(value).replace(/[&<>"']/g, (char) =>
    ({
      "&": "&amp;",
      "<": "&lt;",
      ">": "&gt;",
      '"': "&quot;",
      "'": "&#39;",
    })[char] ?? char
  );
}

function renderDashboardHtml(): string {
  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Live Lens 🔍</title>
  <link rel="icon" href="${LIVE_LENS_FAVICON_HREF}">
  <style>
    :root {
      color-scheme: dark;
      --bg: #0d0d0c;
      --panel: #171715;
      --panel-2: #1f1f1c;
      --field: #111210;
      --ink: #f2f0e8;
      --muted: #a8a29a;
      --line: #34332f;
      --accent: #2dd4bf;
      --accent-ink: #06201d;
      --danger: #ef4444;
      --danger-ink: #fff7f7;
      --warn: #f59e0b;
      --hover: #20312d;
      --pill-bg: #282622;
      --pill-ink: #e7e1d5;
      --code-bg: #080908;
      --shadow: 0 18px 44px rgba(0, 0, 0, 0.38);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: var(--bg);
      color: var(--ink);
      font: 14px/1.45 ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      padding: 18px 24px;
      border-bottom: 1px solid var(--line);
      background: var(--panel);
      position: sticky;
      top: 0;
      z-index: 5;
    }
    h1 {
      margin: 0;
      font-size: 20px;
      font-weight: 700;
      letter-spacing: 0;
    }
    main {
      padding: 20px 24px 28px;
      display: grid;
      gap: 16px;
    }
    .panel, .stats, .content {
      background: var(--panel);
      border: 1px solid var(--line);
      box-shadow: var(--shadow);
      border-radius: 8px;
    }
    .control-panels {
      display: grid;
      grid-template-columns: 1fr;
      gap: 12px;
    }
    .panel {
      padding: 14px;
      min-width: 0;
    }
    .panel h2 {
      margin: 0 0 10px;
      color: var(--muted);
      font-size: 12px;
      font-weight: 800;
      letter-spacing: 0;
      text-transform: uppercase;
    }
    .filter-controls {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      align-items: end;
    }
    .test-controls {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      align-items: end;
    }
    .filter-controls label:not(.toggle) {
      flex: 1 1 190px;
    }
    .filter-controls label.limit-field {
      flex: 0 1 110px;
    }
    .filter-controls button, .filter-controls .button-link, .test-controls button, .test-controls .button-link {
      flex: 0 0 auto;
      white-space: nowrap;
    }
    .toggle {
      flex: 0 0 auto;
    }
    label {
      display: grid;
      gap: 5px;
      color: var(--muted);
      font-size: 12px;
      font-weight: 650;
      min-width: 0;
    }
    input, select, button, .button-link {
      min-height: 36px;
      border: 1px solid var(--line);
      border-radius: 6px;
      background: var(--field);
      color: var(--ink);
      font: inherit;
    }
    input, select {
      width: 100%;
      min-width: 0;
      padding: 0 10px;
    }
    input::placeholder {
      color: #777268;
      opacity: 1;
    }
    input:focus, select:focus, button:focus-visible, .button-link:focus-visible {
      outline: 2px solid rgba(45, 212, 191, 0.55);
      outline-offset: 2px;
      border-color: var(--accent);
    }
    .toggle {
      min-height: 36px;
      display: inline-flex;
      align-items: center;
      gap: 8px;
      color: var(--ink);
      font-size: 13px;
      font-weight: 700;
      cursor: pointer;
      user-select: none;
    }
    .toggle input {
      min-height: 0;
      width: 16px;
      height: 16px;
      margin: 0;
      accent-color: var(--accent);
    }
    button, .button-link {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: 8px;
      padding: 0 12px;
      cursor: pointer;
      font-weight: 700;
      text-decoration: none;
    }
    button.primary {
      background: var(--accent);
      border-color: var(--accent);
      color: var(--accent-ink);
    }
    button.danger {
      background: var(--danger);
      border-color: var(--danger);
      color: var(--danger-ink);
    }
    button.danger:hover {
      background: #dc2626;
      border-color: #dc2626;
    }
    button:not(.primary):not(.danger):hover, .button-link:hover {
      border-color: #575248;
      background: #22231f;
    }
    button:disabled {
      cursor: progress;
      opacity: 0.68;
    }
    .stats {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 1px;
      overflow: hidden;
      background: var(--line);
    }
    .stat {
      background: var(--panel);
      padding: 14px;
      min-width: 0;
    }
    .stat strong {
      display: block;
      font-size: 22px;
      line-height: 1.15;
    }
    .stat span {
      color: var(--muted);
      font-size: 12px;
      font-weight: 650;
    }
    .content {
      display: grid;
      grid-template-columns: minmax(0, 1.4fr) minmax(320px, 0.8fr);
      min-height: 460px;
      overflow: hidden;
    }
    .table-wrap {
      overflow: auto;
      border-right: 1px solid var(--line);
    }
    table {
      width: 100%;
      border-collapse: collapse;
      min-width: 760px;
    }
    th, td {
      padding: 10px 12px;
      border-bottom: 1px solid var(--line);
      text-align: left;
      vertical-align: top;
      white-space: nowrap;
    }
    th {
      position: sticky;
      top: 0;
      background: var(--panel-2);
      color: var(--muted);
      z-index: 1;
      font-size: 12px;
    }
    tr {
      cursor: pointer;
    }
    tr:hover td, tr.selected td {
      background: var(--hover);
    }
    .pill {
      display: inline-flex;
      align-items: center;
      max-width: 180px;
      min-height: 24px;
      padding: 2px 8px;
      border-radius: 999px;
      background: var(--pill-bg);
      color: var(--pill-ink);
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .details {
      padding: 14px;
      overflow: auto;
      min-width: 0;
    }
    .details h2 {
      margin: 0 0 8px;
      font-size: 16px;
      letter-spacing: 0;
      overflow-wrap: anywhere;
    }
    .muted {
      color: var(--muted);
    }
    .error {
      color: var(--warn);
      font-weight: 700;
    }
    dl {
      display: grid;
      grid-template-columns: 112px minmax(0, 1fr);
      gap: 7px 10px;
      margin: 14px 0;
    }
    dt {
      color: var(--muted);
      font-weight: 700;
    }
    dd {
      margin: 0;
      min-width: 0;
      overflow-wrap: anywhere;
    }
    pre {
      margin: 0;
      padding: 12px;
      background: var(--code-bg);
      color: #f5f2eb;
      border: 1px solid var(--line);
      border-radius: 6px;
      overflow: auto;
      max-height: 360px;
      font-size: 12px;
      line-height: 1.45;
    }
    @media (min-width: 1180px) {
      .control-panels {
        grid-template-columns: minmax(0, 1.4fr) minmax(360px, 0.6fr);
      }
    }
    @media (max-width: 860px) {
      header {
        align-items: flex-start;
        flex-direction: column;
      }
      main {
        padding: 14px;
      }
      .filter-controls label:not(.toggle),
      .filter-controls .limit-field,
      .filter-controls button,
      .filter-controls .button-link,
      .test-controls .toggle,
      .test-controls button,
      .test-controls .button-link {
        flex: 1 1 100%;
      }
      .stats {
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }
      .content {
        grid-template-columns: 1fr;
      }
      .table-wrap {
        border-right: 0;
        border-bottom: 1px solid var(--line);
      }
    }
  </style>
</head>
<body>
  <header>
    <h1>Live Lens 🔍</h1>
    <div id="status" class="muted">Ready</div>
  </header>
  <main>
    <section class="control-panels" aria-label="Dashboard controls">
      <div class="panel" aria-label="Filter controls">
        <h2>Filters</h2>
        <div class="filter-controls">
          <label>Run ID
            <input id="runId" autocomplete="off" placeholder="Any run">
          </label>
          <label>Session
            <input id="session" autocomplete="off" placeholder="Session key or hash">
          </label>
          <label class="limit-field">Limit
            <select id="limit">
              <option>50</option>
              <option selected>100</option>
              <option>200</option>
              <option>500</option>
            </select>
          </label>
          <button id="refresh" class="primary" type="button">Refresh</button>
          <button id="clearFilter" type="button">Clear Filter</button>
          <label class="toggle"><input id="autoRefresh" type="checkbox" checked> Auto</label>
        </div>
      </div>
      <div class="panel" aria-label="Test controls">
        <h2>Tests</h2>
        <div class="test-controls">
          <label class="toggle"><input id="liveTestTools" type="checkbox"> Tools</label>
          <a id="reportLink" class="button-link" href="${LOCAL_REPORT_PATH}?format=html" target="_blank" rel="noreferrer">Open Report</a>
          <button id="clear" type="button">Clear E2E</button>
          <button id="e2eTest" type="button">Run E2E</button>
          <button id="liveTest" class="danger" type="button">Live Test</button>
        </div>
      </div>
    </section>
    <section class="stats" aria-label="Span summary">
      <div class="stat"><strong id="total">0</strong><span>Total spans</span></div>
      <div class="stat"><strong id="latest">-</strong><span>Latest span</span></div>
      <div class="stat"><strong id="slowest">-</strong><span>Slowest duration</span></div>
      <div class="stat"><strong id="sources">0</strong><span>Sources</span></div>
    </section>
    <section class="content">
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Time</th>
              <th>Name</th>
              <th>Phase</th>
              <th>Duration</th>
              <th>Run</th>
              <th>Source</th>
            </tr>
          </thead>
          <tbody id="rows">
            <tr><td colspan="6" class="muted">No spans loaded.</td></tr>
          </tbody>
        </table>
      </div>
      <aside class="details" aria-live="polite">
        <h2 id="detailTitle">Select a span</h2>
        <div id="detailMeta" class="muted">Span attributes will appear here.</div>
        <div id="detailBody"></div>
      </aside>
    </section>
  </main>
  <script>
    const state = { spans: [], selectedId: "" };
    const $ = (id) => document.getElementById(id);
    const rows = $("rows");
    const status = $("status");
    const refreshButton = $("refresh");
    const reportLink = $("reportLink");
    const clearFilterButton = $("clearFilter");
    const e2eTestButton = $("e2eTest");
    const liveTestButton = $("liveTest");
    const liveTestToolsInput = $("liveTestTools");
    const autoRefreshInput = $("autoRefresh");
    const AUTO_REFRESH_MS = 5000;
    let refreshTimer = 0;
    let loading = false;

    function setStatus(text, kind = "muted") {
      status.className = kind;
      status.textContent = text;
    }

    function formatTime(value) {
      if (typeof value !== "number") return "-";
      return new Date(value).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
    }

    function formatDuration(value) {
      return typeof value === "number" ? value.toLocaleString(undefined, { maximumFractionDigits: 1 }) + " ms" : "-";
    }

    function displayDuration(span) {
      return typeof span?.observedDurationMs === "number" ? span.observedDurationMs : span?.durationMs;
    }

    function formatDashboardDuration(span) {
      if (span?.durationSource === "point") return "point";
      const suffix = span?.durationSource === "derived-next-span" ? " derived" : "";
      const pairedSuffix = span?.durationSource === "paired-ended" ? " paired" : "";
      const duration = formatDuration(displayDuration(span));
      return duration === "-" ? duration : duration + suffix + pairedSuffix;
    }

    function text(value) {
      return value === undefined || value === null || value === "" ? "-" : String(value);
    }

    function updateStats() {
      $("total").textContent = state.spans.length;
      $("latest").textContent = state.spans[0] ? formatTime(state.spans[0].createdAtMs) : "-";
      const slowest = state.spans.reduce((best, span) => {
        const displayMs = displayDuration(span);
        const duration = typeof displayMs === "number" ? displayMs : -1;
        return duration > best ? duration : best;
      }, -1);
      $("slowest").textContent = slowest >= 0 ? formatDuration(slowest) : "-";
      $("sources").textContent = new Set(state.spans.map((span) => span.source).filter(Boolean)).size;
    }

    function filterParams() {
      const params = new URLSearchParams();
      params.set("limit", $("limit").value);
      const runId = $("runId").value.trim();
      const session = $("session").value.trim();
      if (runId) params.set("runId", runId);
      if (session) params.set(session.length === 24 && /^[a-f0-9]+$/i.test(session) ? "sessionHash" : "sessionKey", session);
      return params;
    }

    function reportParams() {
      const params = filterParams();
      params.set("format", "html");
      return params;
    }

    function updateReportLink() {
      reportLink.href = "${LOCAL_REPORT_PATH}?" + reportParams().toString();
    }

    function renderRows() {
      if (state.spans.length === 0) {
        rows.innerHTML = '<tr><td colspan="6" class="muted">No spans found.</td></tr>';
        renderDetails(undefined);
        return;
      }
      rows.replaceChildren(...state.spans.map((span) => {
        const row = document.createElement("tr");
        row.className = span.spanId === state.selectedId ? "selected" : "";
        row.addEventListener("click", () => {
          state.selectedId = span.spanId;
          renderRows();
          renderDetails(span);
        });
        row.innerHTML = [
          "<td>" + formatTime(span.createdAtMs ?? span.startedAtMs) + "</td>",
          '<td><span class="pill" title="' + escapeHtml(text(span.name)) + '">' + escapeHtml(text(span.name)) + "</span></td>",
          "<td>" + escapeHtml(text(span.phase)) + "</td>",
          "<td>" + escapeHtml(formatDashboardDuration(span)) + "</td>",
          "<td>" + escapeHtml(text(span.runId)) + "</td>",
          "<td>" + escapeHtml(text(span.source)) + "</td>",
        ].join("");
        return row;
      }));
    }

    function renderDetails(span) {
      if (!span) {
        $("detailTitle").textContent = "Select a span";
        $("detailMeta").textContent = "Span attributes will appear here.";
        $("detailBody").innerHTML = "";
        return;
      }
      $("detailTitle").textContent = span.name ?? "Unnamed span";
      $("detailMeta").textContent = [span.phase, formatDashboardDuration(span), span.source].filter(Boolean).join(" | ");
      const meta = [
        ["Run ID", span.runId],
        ["Session", span.sessionHash],
        ["Call ID", span.callId],
        ["Tool Call", span.toolCallId],
        ["Agent", span.agentId],
        ["Channel", span.channelId],
        ["Started", span.startedAtMs ? new Date(span.startedAtMs).toLocaleString() : undefined],
        ["Raw Duration", formatDuration(span.durationMs)],
        ["Displayed Duration", formatDuration(displayDuration(span))],
        ["Duration Source", span.durationSource],
      ];
      $("detailBody").innerHTML =
        "<dl>" + meta.map(([label, value]) => "<dt>" + label + "</dt><dd>" + escapeHtml(text(value)) + "</dd>").join("") + "</dl>" +
        "<pre>" + escapeHtml(JSON.stringify(span.attributes ?? {}, null, 2)) + "</pre>";
    }

    async function loadSpans(options = {}) {
      if (loading) return;
      loading = true;
      refreshButton.disabled = true;
      if (!options.quiet) setStatus("Loading...");
      const params = new URLSearchParams();
      for (const [key, value] of filterParams()) params.set(key, value);
      updateReportLink();
      try {
        const response = await fetch("${LOCAL_SPANS_PATH}?" + params.toString(), { headers: { accept: "application/json" } });
        if (!response.ok) throw new Error("HTTP " + response.status);
        const payload = await response.json();
        state.spans = Array.isArray(payload.spans) ? payload.spans : [];
        state.selectedId = state.spans.some((span) => span.spanId === state.selectedId) ? state.selectedId : "";
        updateStats();
        renderRows();
        renderDetails(state.spans.find((span) => span.spanId === state.selectedId) ?? state.spans[0]);
        if (!state.selectedId && state.spans[0]) state.selectedId = state.spans[0].spanId;
        renderRows();
        setStatus("Updated " + new Date().toLocaleTimeString());
      } catch (error) {
        setStatus("Load failed: " + error.message, "error");
      } finally {
        loading = false;
        refreshButton.disabled = false;
      }
    }

    async function runE2eTest() {
      e2eTestButton.disabled = true;
      setStatus("Running E2E smoke...");
      try {
        const response = await fetch("${LOCAL_E2E_TEST_PATH}", { method: "POST", headers: { accept: "application/json" } });
        if (!response.ok) throw new Error("HTTP " + response.status);
        const payload = await response.json();
        if (payload && payload.runId) {
          $("runId").value = payload.runId;
        }
        await loadSpans();
      } catch (error) {
        setStatus("E2E failed: " + error.message, "error");
      } finally {
        e2eTestButton.disabled = false;
      }
    }

    async function runLiveTest() {
      liveTestButton.disabled = true;
      setStatus(liveTestToolsInput.checked ? "Starting real live test turn with tool probe..." : "Starting real live test turn...");
      try {
        const params = new URLSearchParams();
        if (liveTestToolsInput.checked) params.set("tools", "1");
        const response = await fetch("${LOCAL_LIVE_TEST_PATH}" + (params.size ? "?" + params.toString() : ""), { method: "POST", headers: { accept: "application/json" } });
        if (!response.ok) {
          let detail = "HTTP " + response.status;
          try {
            const payload = await response.json();
            if (payload && payload.message) detail = payload.message;
          } catch {}
          throw new Error(detail);
        }
        const payload = await response.json();
        if (payload && payload.sessionHash) {
          $("runId").value = "";
          $("session").value = payload.sessionHash;
        }
        await loadSpans();
        setStatus(liveTestToolsInput.checked ? "Live test started; filtering by its message, tool, and reply telemetry..." : "Live test started; filtering by its message and reply telemetry...");
      } catch (error) {
        setStatus("Live test failed: " + error.message, "error");
      } finally {
        liveTestButton.disabled = false;
      }
    }

    async function clearDashboard() {
      setStatus("Clearing E2E spans...");
      try {
        const response = await fetch("${LOCAL_CLEAR_TEST_SPANS_PATH}", { method: "POST", headers: { accept: "application/json" } });
        if (!response.ok) throw new Error("HTTP " + response.status);
        await loadSpans();
      } catch (error) {
        setStatus("Clear failed: " + error.message, "error");
      }
    }

    async function clearFilters() {
      $("runId").value = "";
      $("session").value = "";
      setStatus("Filters cleared");
      await loadSpans();
    }

    function scheduleAutoRefresh() {
      window.clearInterval(refreshTimer);
      if (!autoRefreshInput.checked) return;
      refreshTimer = window.setInterval(() => {
        if (document.visibilityState === "visible") {
          loadSpans({ quiet: true });
        }
      }, AUTO_REFRESH_MS);
    }

    function escapeHtml(value) {
      return String(value).replace(/[&<>"']/g, (char) => ({
        "&": "&amp;",
        "<": "&lt;",
        ">": "&gt;",
        '"': "&quot;",
        "'": "&#39;",
      })[char]);
    }

    refreshButton.addEventListener("click", loadSpans);
    clearFilterButton.addEventListener("click", clearFilters);
    e2eTestButton.addEventListener("click", runE2eTest);
    liveTestButton.addEventListener("click", runLiveTest);
    autoRefreshInput.addEventListener("change", () => {
      scheduleAutoRefresh();
      setStatus(autoRefreshInput.checked ? "Auto refresh on" : "Auto refresh paused");
    });
    document.addEventListener("visibilitychange", () => {
      if (document.visibilityState === "visible" && autoRefreshInput.checked) {
        loadSpans({ quiet: true });
      }
    });
    $("clear").addEventListener("click", () => {
      clearDashboard();
    });
    $("runId").addEventListener("keydown", (event) => { if (event.key === "Enter") loadSpans(); });
    $("session").addEventListener("keydown", (event) => { if (event.key === "Enter") loadSpans(); });
    $("runId").addEventListener("input", updateReportLink);
    $("session").addEventListener("input", updateReportLink);
    $("limit").addEventListener("change", updateReportLink);
    updateReportLink();
    loadSpans();
    scheduleAutoRefresh();
  </script>
</body>
</html>`;
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

function telemetryNowMs(): number {
  // Use one monotonic-derived timestamp source for telemetry intervals. Date.now()
  // can jump under NTP, which corrupts reconstructed starts like end - duration.
  return Math.round(performance.timeOrigin + performance.now());
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
    const store = createStore(dbPath, (error) => {
      api.logger.error(`[live-lens] store write failed: ${String(error)}`);
    });

    registerLiveTestTool(api);
    registerHookSpans(api, store, config);
    registerHttpRoutes(api, store, config);

    api.lifecycle.registerRuntimeLifecycle({
      id: "close-live-lens-db",
      description: "Close Live Lens SQLite store during plugin shutdown.",
      cleanup: () => store.close(),
    });

    const displayDbPath = api.rootDir ? path.relative(api.rootDir, dbPath) : path.basename(dbPath);
    api.logger.info(
      `[live-lens] recording ${config.enabled ? "enabled" : "disabled"} at ${displayDbPath}`,
    );
  },
});
