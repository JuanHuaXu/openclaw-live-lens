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

async function invoke(route, { method = "GET", url = route.path, body = "" } = {}) {
  const req = Readable.from(body ? [Buffer.from(body)] : []);
  req.method = method;
  req.url = url;
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
    json: JSON.parse(responseBody),
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
