import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { Readable } from "node:stream";
import test from "node:test";

import plugin from "../dist/index.js";

function hashValue(value) {
  return createHash("sha256").update(value).digest("hex").slice(0, 24);
}

function createHarness(config = {}) {
  const hooks = new Map();
  const routes = [];
  const tools = [];
  const lifecycles = [];
  const errors = [];
  const api = {
    rootDir: config.rootDir,
    pluginConfig: config.pluginConfig ?? {},
    logger: {
      info() {},
      warn() {},
      error(message) {
        errors.push(String(message));
      },
    },
    on(name, handler) {
      hooks.set(name, handler);
    },
    registerHttpRoute(route) {
      routes.push(route);
    },
    registerTool(tool) {
      tools.push(tool);
    },
    lifecycle: {
      registerRuntimeLifecycle(lifecycle) {
        lifecycles.push(lifecycle);
      },
    },
  };
  plugin.register(api);
  return {
    errors,
    hooks,
    lifecycles,
    tools,
    route(pathname) {
      const route = routes.find((candidate) => candidate.path === pathname);
      assert.ok(route, `missing route ${pathname}`);
      return route;
    },
    async cleanup() {
      for (const lifecycle of lifecycles) {
        await lifecycle.cleanup?.();
      }
    },
  };
}

async function invoke(
  route,
  { method = "GET", url = route.path, body = "", remoteAddress = "127.0.0.1" } = {},
) {
  const req = Readable.from(body ? [Buffer.from(body)] : []);
  req.method = method;
  req.url = url;
  req.socket = { remoteAddress };
  let responseBody = "";
  const headers = {};
  const res = {
    statusCode: 200,
    setHeader(name, value) {
      headers[String(name).toLowerCase()] = value;
    },
    end(value = "") {
      responseBody = Buffer.isBuffer(value) ? value.toString("utf8") : String(value);
    },
  };
  await route.handler(req, res);
  return {
    statusCode: res.statusCode,
    headers,
    body: responseBody,
    json: String(headers["content-type"] ?? "").includes("application/json")
      ? JSON.parse(responseBody)
      : undefined,
  };
}

test("hook spans keep metrics while redacting content-shaped fields", async () => {
  const rootDir = await mkdtemp(path.join(os.tmpdir(), "live-lens-"));
  const harness = createHarness({ rootDir });
  try {
    const hook = harness.hooks.get("before_prompt_build");
    assert.equal(typeof hook, "function");
    hook(
      { prompt: "hello", messages: [{ role: "user" }, { role: "assistant" }] },
      {
        runId: "run-one",
        sessionKey: "agent:main:discord:channel:123",
        agentId: "main",
        channelId: "123",
        contextTokenBudget: 2048,
      },
    );

    const result = await invoke(harness.route("/api/openclaw-lens/spans"), {
      url: "/api/openclaw-lens/spans?runId=run-one",
    });
    assert.equal(result.statusCode, 200);
    const [span] = result.json.spans;
    assert.equal(span.name, "openclaw.before_prompt_build");
    assert.equal(span.attributes.messageCount, 2);
    assert.equal(span.attributes.promptChars, 5);
    assert.equal(span.attributes.contextTokenBudget, 2048);
    assert.equal(span.sessionHash, hashValue("agent:main:discord:channel:123"));
  } finally {
    await harness.cleanup();
    await rm(rootDir, { recursive: true, force: true });
  }
});

test("live test probe tool is registered and hook telemetry keeps tool metadata", async () => {
  const rootDir = await mkdtemp(path.join(os.tmpdir(), "live-lens-"));
  const harness = createHarness({ rootDir });
  try {
    const tool = harness.tools.find((candidate) => candidate.name === "live_lens_probe");
    assert.ok(tool, "missing live_lens_probe tool");
    assert.equal(tool.label, "Live Lens Probe");
    assert.equal(tool.parameters.required.includes("nonce"), true);
    const toolResult = await tool.execute("tool-call-one", { nonce: "abc123" });
    assert.equal(toolResult.details.ok, true);
    assert.match(toolResult.content[0].text, /abc123/);

    const beforeTool = harness.hooks.get("before_tool_call");
    const afterTool = harness.hooks.get("after_tool_call");
    assert.equal(typeof beforeTool, "function");
    assert.equal(typeof afterTool, "function");
    const ctx = {
      runId: "live-run-one",
      sessionKey: "agent:main:dashboard:live-lens-live-test-test",
      agentId: "main",
      channelId: "local",
    };
    beforeTool(
      {
        toolName: "live_lens_probe",
        toolKind: "plugin",
        toolInputKind: "json",
        toolCallId: "tool-call-one",
        params: { nonce: "abc123" },
      },
      ctx,
    );
    afterTool(
      {
        toolName: "live_lens_probe",
        toolKind: "plugin",
        toolInputKind: "json",
        toolCallId: "tool-call-one",
        params: { nonce: "abc123" },
        result: { ok: true },
        durationMs: 12,
      },
      ctx,
    );

    const result = await invoke(harness.route("/openclaw-lens/spans"), {
      url: "/openclaw-lens/spans?runId=live-run-one",
    });
    assert.equal(result.statusCode, 200);
    const toolSpans = result.json.spans.filter((span) => span.attributes.toolName === "live_lens_probe");
    assert.equal(toolSpans.length, 2);
    assert.equal(toolSpans.some((span) => span.name === "openclaw.before_tool_call"), true);
    const afterSpan = toolSpans.find((span) => span.name === "openclaw.tool_call");
    assert.ok(afterSpan);
    assert.equal(afterSpan.attributes.toolKind, "plugin");
    assert.equal(afterSpan.attributes.toolInputKind, "json");
    assert.equal(afterSpan.attributes.hasError, false);
  } finally {
    await harness.cleanup();
    await rm(rootDir, { recursive: true, force: true });
  }
});

test("dashboard route serves async spans view", async () => {
  const rootDir = await mkdtemp(path.join(os.tmpdir(), "live-lens-"));
  const harness = createHarness({ rootDir });
  try {
    const route = harness.route("/openclaw-lens/dashboard");
    const result = await invoke(route);

    assert.equal(result.statusCode, 200);
    assert.match(String(result.headers["content-type"]), /^text\/html/);
    assert.match(result.body, /<title>Live Lens 🔍<\/title>/);
    assert.match(result.body, /rel="icon"/);
    assert.match(result.body, /<h1>Live Lens 🔍<\/h1>/);
    assert.match(result.body, /fetch\("\/openclaw-lens\/spans\?/);
    assert.match(result.body, /class="control-panels"/);
    assert.match(result.body, /aria-label="Filter controls"/);
    assert.match(result.body, /aria-label="Test controls"/);
    assert.match(result.body, /id="e2eTest"/);
    assert.match(result.body, /id="liveTest" class="danger"/);
    assert.match(result.body, /id="liveTestTools" type="checkbox"/);
    assert.match(result.body, /> Tools<\/label>/);
    assert.match(result.body, /id="clearFilter"/);
    assert.match(result.body, />Clear Filter<\/button>/);
    assert.match(result.body, />Clear E2E<\/button>/);
    assert.match(result.body, /fetch\("\/openclaw-lens\/e2e-test"/);
    assert.match(result.body, /fetch\("\/openclaw-lens\/live-test"/);
    assert.match(result.body, /fetch\("\/openclaw-lens\/test-spans\/clear"/);
    assert.match(result.body, /setInterval\(\(\) =>/);
    assert.match(result.body, /id="autoRefresh"/);
    assert.match(result.body, /async function clearFilters\(\) \{/);
    assert.match(result.body, /if \(liveTestToolsInput\.checked\) params\.set\("tools", "1"\);/);
    assert.match(result.body, /fetch\("\/openclaw-lens\/live-test" \+ \(params\.size \? "\?" \+ params\.toString\(\) : ""\)/);
    assert.match(result.body, /clearFilterButton\.addEventListener\("click", clearFilters\)/);
    const clearDashboardBody =
      result.body.match(/async function clearDashboard\(\) \{([\s\S]*?)\n    \}/)?.[1] ?? "";
    const clearFiltersBody = result.body.match(/async function clearFilters\(\) \{([\s\S]*?)\n    \}/)?.[1] ?? "";
    assert.doesNotMatch(clearDashboardBody, /\$\("runId"\)\.value = "";/);
    assert.doesNotMatch(clearDashboardBody, /\$\("session"\)\.value = "";/);
    assert.match(clearFiltersBody, /\$\("runId"\)\.value = "";/);
    assert.match(clearFiltersBody, /\$\("session"\)\.value = "";/);
  } finally {
    await harness.cleanup();
    await rm(rootDir, { recursive: true, force: true });
  }
});

test("local e2e endpoint creates a full turn trace", async () => {
  const rootDir = await mkdtemp(path.join(os.tmpdir(), "live-lens-"));
  const harness = createHarness({ rootDir });
  try {
    const created = await invoke(harness.route("/openclaw-lens/e2e-test"), {
      method: "POST",
    });
    assert.equal(created.statusCode, 202);
    assert.equal(created.json.accepted, 10);
    assert.match(created.json.runId, /^e2e-test-/);

    const result = await invoke(harness.route("/openclaw-lens/spans"), {
      url: `/openclaw-lens/spans?runId=${created.json.runId}`,
    });
    assert.equal(result.statusCode, 200);
    assert.equal(result.json.spans.length, 10);
    const names = new Set(result.json.spans.map((span) => span.name));
    assert.equal(names.has("openclaw.message_received"), true);
    assert.equal(names.has("openclaw.before_prompt_build"), true);
    assert.equal(names.has("libravdb.daemon.assembleContextInternal"), true);
    assert.equal(names.has("openclaw.before_tool_call"), true);
    assert.equal(names.has("openclaw.tool_call"), true);
    assert.equal(names.has("openclaw.llm_input"), true);
    assert.equal(names.has("openclaw.model_call"), true);
    assert.equal(names.has("openclaw.llm_output"), true);
    assert.equal(names.has("openclaw.agent_run"), true);
    assert.equal(
      result.json.spans.every((span) => span.attributes.testSeed === true && span.attributes.e2eTest === true),
      true,
    );
    assert.equal(
      result.json.spans.some((span) => span.attributes.prompt || span.attributes.message || span.attributes.text),
      false,
    );
    const memorySpan = result.json.spans.find((span) => span.name === "libravdb.daemon.assembleContextInternal");
    assert.equal(memorySpan.phase, "memory");
    assert.equal(memorySpan.attributes.cacheHit, false);
    const toolSpan = result.json.spans.find((span) => span.name === "openclaw.tool_call");
    assert.equal(toolSpan.attributes.toolName, "live_lens_e2e_tool");
    assert.equal(toolSpan.attributes.hasError, false);
    const agentSpan = result.json.spans.find((span) => span.name === "openclaw.agent_run");
    assert.equal(agentSpan.attributes.success, true);

    const method = await invoke(harness.route("/openclaw-lens/e2e-test"), {
      method: "GET",
    });
    assert.equal(method.statusCode, 405);
    assert.equal(method.json.error, "method_not_allowed");

    const liveMethod = await invoke(harness.route("/openclaw-lens/live-test"), {
      method: "GET",
    });
    assert.equal(liveMethod.statusCode, 405);
    assert.equal(liveMethod.json.error, "method_not_allowed");
  } finally {
    await harness.cleanup();
    await rm(rootDir, { recursive: true, force: true });
  }
});

test("local clear endpoint removes only dashboard test spans", async () => {
  const rootDir = await mkdtemp(path.join(os.tmpdir(), "live-lens-"));
  const harness = createHarness({ rootDir });
  try {
    const testOne = await invoke(harness.route("/openclaw-lens/e2e-test"), {
      method: "POST",
    });
    const testTwo = await invoke(harness.route("/openclaw-lens/e2e-test"), {
      method: "POST",
    });
    assert.equal(testOne.statusCode, 202);
    assert.equal(testTwo.statusCode, 202);

    const regularHook = harness.hooks.get("message_received");
    assert.equal(typeof regularHook, "function");
    regularHook(
      { provider: "test", channelId: "local" },
      { runId: "real-run", sessionKey: "agent:main:local:real", channelId: "local" },
    );

    const cleared = await invoke(harness.route("/openclaw-lens/test-spans/clear"), {
      method: "POST",
    });
    assert.equal(cleared.statusCode, 202);
    assert.equal(cleared.json.deleted, 20);

    const testRows = await invoke(harness.route("/openclaw-lens/spans"), {
      url: "/openclaw-lens/spans?limit=20",
    });
    assert.equal(
      testRows.json.spans.some((span) => span.attributes.e2eTest === true),
      false,
    );
    assert.equal(
      testRows.json.spans.some((span) => span.name === "openclaw.message_received"),
      true,
    );

    const method = await invoke(harness.route("/openclaw-lens/test-spans/clear"), {
      method: "GET",
    });
    assert.equal(method.statusCode, 405);
    assert.equal(method.json.error, "method_not_allowed");
  } finally {
    await harness.cleanup();
    await rm(rootDir, { recursive: true, force: true });
  }
});

test("local dashboard routes reject non-loopback clients", async () => {
  const rootDir = await mkdtemp(path.join(os.tmpdir(), "live-lens-"));
  const harness = createHarness({ rootDir });
  try {
    const dashboard = await invoke(harness.route("/openclaw-lens/dashboard"), {
      remoteAddress: "203.0.113.10",
    });
    assert.equal(dashboard.statusCode, 403);
    assert.equal(dashboard.json.error, "localhost_only");

    const spans = await invoke(harness.route("/openclaw-lens/spans"), {
      remoteAddress: "203.0.113.10",
    });
    assert.equal(spans.statusCode, 403);
    assert.equal(spans.json.error, "localhost_only");

    const e2eTest = await invoke(harness.route("/openclaw-lens/e2e-test"), {
      method: "POST",
      remoteAddress: "203.0.113.10",
    });
    assert.equal(e2eTest.statusCode, 403);
    assert.equal(e2eTest.json.error, "localhost_only");

    const liveTest = await invoke(harness.route("/openclaw-lens/live-test"), {
      method: "POST",
      remoteAddress: "203.0.113.10",
    });
    assert.equal(liveTest.statusCode, 403);
    assert.equal(liveTest.json.error, "localhost_only");

    const clear = await invoke(harness.route("/openclaw-lens/test-spans/clear"), {
      method: "POST",
      remoteAddress: "203.0.113.10",
    });
    assert.equal(clear.statusCode, 403);
    assert.equal(clear.json.error, "localhost_only");
  } finally {
    await harness.cleanup();
    await rm(rootDir, { recursive: true, force: true });
  }
});

test("libravdb ingest rejects invalid JSON and missing span names", async () => {
  const rootDir = await mkdtemp(path.join(os.tmpdir(), "live-lens-"));
  const harness = createHarness({
    rootDir,
    pluginConfig: {
      maxIngestBytes: 64,
    },
  });
  try {
    const route = harness.route("/api/openclaw-lens/libravdb/spans");
    const invalid = await invoke(route, { method: "POST", body: "not-json" });
    assert.equal(invalid.statusCode, 400);
    assert.equal(invalid.json.error, "invalid_json");

    const missingName = await invoke(route, {
      method: "POST",
      body: JSON.stringify({ spans: [{ durationMs: 1 }] }),
    });
    assert.equal(missingName.statusCode, 400);
    assert.equal(missingName.json.error, "no_valid_spans");

    const tooLarge = await invoke(route, {
      method: "POST",
      body: JSON.stringify({
        spans: [
          {
            name: "libravdb.tooLarge",
            attributes: {
              padding: "x".repeat(5000),
            },
          },
        ],
      }),
    });
    assert.equal(tooLarge.statusCode, 413);
    assert.equal(tooLarge.json.error, "body_too_large");
  } finally {
    await harness.cleanup();
    await rm(rootDir, { recursive: true, force: true });
  }
});

test("libravdb ingest stores bounded redacted timing spans", async () => {
  const rootDir = await mkdtemp(path.join(os.tmpdir(), "live-lens-"));
  const harness = createHarness({ rootDir });
  try {
    const ingest = await invoke(harness.route("/api/openclaw-lens/libravdb/spans"), {
      method: "POST",
      body: JSON.stringify({
        runId: "run-two",
        sessionKey: "agent:main:discord:channel:secret",
        spans: [
          {
            name: "libravdb.daemon.assembleContextInternal",
            phase: "assemble",
            durationMs: 631,
            attributes: {
              prompt: "do not store this",
              promptChars: 17,
              query: "private search phrase",
              queryChars: 21,
              sourceUrl: "https://example.invalid/private",
              derivedPathCount: 2,
              messageCount: 3,
              sessionKey: "agent:main:discord:channel:secret",
            },
          },
        ],
      }),
    });
    assert.equal(ingest.statusCode, 202);
    assert.equal(ingest.json.accepted, 1);

    const result = await invoke(harness.route("/api/openclaw-lens/spans"), {
      url: "/api/openclaw-lens/spans?sessionKey=agent%3Amain%3Adiscord%3Achannel%3Asecret",
    });
    const [span] = result.json.spans;
    assert.equal(span.name, "libravdb.daemon.assembleContextInternal");
    assert.equal(span.durationMs, 631);
    assert.equal(span.attributes.prompt, "[redacted]");
    assert.equal(span.attributes.promptChars, 17);
    assert.equal(span.attributes.query, "[redacted]");
    assert.equal(span.attributes.queryChars, 21);
    assert.equal(span.attributes.sourceUrl, "[redacted]");
    assert.equal(span.attributes.derivedPathCount, 2);
    assert.equal(span.attributes.messageCount, 3);
    assert.equal(span.attributes.sessionKey, undefined);
    assert.equal(span.attributes.sessionKeyHash, hashValue("agent:main:discord:channel:secret"));
    assert.equal(span.sessionHash, hashValue("agent:main:discord:channel:secret"));
  } finally {
    await harness.cleanup();
    await rm(rootDir, { recursive: true, force: true });
  }
});
