import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { test } from "node:test";

import { AppsPatrol, AppsPatrolError, STANDARD_EVENTS, identifyAfterAuth, patrolMiddleware } from "../dist/esm/index.js";

// A fetch that records requests and replays scripted responses.
function scripted(...responses) {
  const requests = [];
  const fetchImpl = async (url, init) => {
    requests.push({ url, method: init.method, headers: init.headers, body: init.body ? JSON.parse(init.body) : undefined });
    const next = responses.length > 1 ? responses.shift() : responses[0];
    if (next instanceof Error) throw next;
    return next;
  };
  fetchImpl.requests = requests;
  fetchImpl.events = () => requests.filter((r) => r.url.endsWith("/v1/events")).flatMap((r) => r.body.events);
  return fetchImpl;
}

const json = (status, body, headers = {}) => new Response(JSON.stringify(body), { status, headers });
const ok = (body = { accepted: 1, duplicates: 0, errors: [] }) => json(202, body);
const apiError = (status, code, headers) => json(status, { error: { code, message: `${code} happened` } }, headers);

// Retries without really waiting, remembering how long each wait would have been.
class Patrol extends AppsPatrol {
  waits = [];
  wait(ms) {
    this.waits.push(ms);
    return Promise.resolve();
  }
}

function create(fetchImpl, options = {}) {
  const errors = [];
  const patrol = new Patrol({
    apiKey: "ap_test",
    baseUrl: "https://patrol.test/",
    fetch: fetchImpl,
    logger: null,
    flushInterval: 3_600_000, // the tests flush explicitly
    onError: (error, context) => errors.push([error, context]),
    ...options,
  });
  return { patrol, errors };
}

test("queues without sending, then sends one batch on flush", async () => {
  const fetchImpl = scripted(ok());
  const { patrol } = create(fetchImpl);

  assert.equal(patrol.track({ shop: "a.myshopify.com", event: "feature_used", properties: { feature: "bulk_edit" } }), true);
  patrol.track({ shop: "b.myshopify.com", event: "app_opened" });
  assert.equal(fetchImpl.requests.length, 0);
  assert.equal(patrol.queueSize, 2);

  assert.equal(await patrol.flush(), true);
  assert.equal(fetchImpl.requests.length, 1);
  const [request] = fetchImpl.requests;
  assert.equal(request.url, "https://patrol.test/v1/events");
  assert.equal(request.headers.Authorization, "Bearer ap_test");
  const [first, second] = fetchImpl.events();
  assert.deepEqual({ shop: first.shop, name: first.name, properties: first.properties }, { shop: "a.myshopify.com", name: "feature_used", properties: { feature: "bulk_edit" } });
  assert.match(first.event_id, /^[0-9a-f]{8}-/);
  assert.ok(Math.abs(Date.parse(first.occurred_at) - Date.now()) < 5000);
  assert.equal(second.name, "app_opened");
});

test("splits more than 100 events into several requests", async () => {
  const fetchImpl = scripted(ok());
  const { patrol } = create(fetchImpl, { flushAt: 100, maxQueue: 1000 });
  // flushAt triggers a background flush at 100; wait for everything to settle
  for (let i = 0; i < 250; i += 1) patrol.track({ shop: "a.myshopify.com", event: `e${i}` });
  await patrol.flush();
  assert.equal(fetchImpl.events().length, 250);
  assert.ok(fetchImpl.requests.every((r) => r.body.events.length <= 100));
});

test("sends immediately with flushAt 1 (serverless)", async () => {
  const fetchImpl = scripted(ok());
  const { patrol } = create(fetchImpl, { flushAt: 1 });
  patrol.track({ shop: "a.myshopify.com", event: "x" });
  await patrol.flush();
  assert.equal(fetchImpl.events().length, 1);
});

test("sends on the timer without keeping the process alive", async () => {
  const fetchImpl = scripted(ok());
  const { patrol } = create(fetchImpl, { flushInterval: 20 });
  patrol.track({ shop: "a.myshopify.com", event: "x" });
  await new Promise((resolve) => setTimeout(resolve, 120));
  assert.equal(fetchImpl.events().length, 1);
});

test("sends shop profiles to the shops endpoint in wire format", async () => {
  const fetchImpl = scripted(json(202, { shop: "a.myshopify.com" }));
  const { patrol } = create(fetchImpl);
  patrol.identify({ shop: "a.myshopify.com", name: "A", country: "DE", shopifyPlan: "basic", utm: { utm_source: "ads" } });
  await patrol.flush();
  assert.equal(fetchImpl.requests[0].url, "https://patrol.test/v1/shops");
  assert.deepEqual(fetchImpl.requests[0].body, { shop: "a.myshopify.com", name: "A", country: "DE", shopify_plan: "basic", utm: { utm_source: "ads" } });
});

test("drops calls without a shop or a name and reports them", () => {
  const { patrol, errors } = create(scripted(ok()));
  assert.equal(patrol.track({ shop: "", event: "x" }), false);
  assert.equal(patrol.track({ shop: "a.myshopify.com", event: "" }), false);
  assert.deepEqual(errors.map(([error]) => error.message), ["shop is required", "event is required"]);
  assert.equal(patrol.queueSize, 0);
});

test("does nothing when disabled or without a key", async () => {
  const fetchImpl = scripted(ok());
  assert.equal(create(fetchImpl, { enabled: false }).patrol.track({ shop: "a.myshopify.com", event: "x" }), false);
  assert.equal(create(fetchImpl, { apiKey: "" }).patrol.track({ shop: "a.myshopify.com", event: "x" }), false);
  assert.equal(fetchImpl.requests.length, 0);
});

test("keeps only the newest items when the queue overflows", async () => {
  const fetchImpl = scripted(ok());
  const { patrol } = create(fetchImpl, { maxQueue: 3 });
  for (let i = 0; i < 5; i += 1) patrol.track({ shop: "a.myshopify.com", event: `e${i}` });
  assert.equal(patrol.dropped, 2);
  await patrol.flush();
  assert.deepEqual(fetchImpl.events().map((e) => e.name), ["e2", "e3", "e4"]);
});

test("appOpened is sent once per shop per day with a stable id", async () => {
  const fetchImpl = scripted(ok());
  const { patrol } = create(fetchImpl);
  const day = new Date("2026-11-17T09:00:00Z");
  assert.equal(patrol.appOpened("a.myshopify.com", day), true);
  assert.equal(patrol.appOpened("a.myshopify.com", new Date("2026-11-17T22:00:00Z")), false);
  patrol.appOpened("b.myshopify.com", day);
  patrol.appOpened("a.myshopify.com", new Date("2026-11-18T01:00:00Z"));
  await patrol.flush();
  assert.deepEqual(fetchImpl.events().map((e) => e.event_id), [
    "app_opened:a.myshopify.com:2026-11-17",
    "app_opened:b.myshopify.com:2026-11-17",
    "app_opened:a.myshopify.com:2026-11-18",
  ]);
});

test("has a helper for each standard name", async () => {
  const fetchImpl = scripted(ok());
  const { patrol } = create(fetchImpl);
  const shop = "a.myshopify.com";
  patrol.onboardingStarted(shop);
  patrol.onboardingCompleted(shop, { steps: 3 });
  patrol.featureUsed(shop, "bulk_edit", { items: 4 });
  patrol.planViewed(shop);
  patrol.upgradeClicked(shop, { plan: "pro" });
  patrol.limitReached(shop, "products");
  patrol.errorShown(shop, "sync_failed");
  await patrol.flush();
  assert.deepEqual(fetchImpl.events().map((e) => e.name), STANDARD_EVENTS.filter((name) => name !== "app_opened"));
  assert.deepEqual(fetchImpl.events()[2].properties, { feature: "bulk_edit", items: 4 });
  assert.deepEqual(fetchImpl.events()[5].properties, { limit: "products" });
});

test("retries network errors and server errors, then delivers", async () => {
  const fetchImpl = scripted(new TypeError("fetch failed"), apiError(503, "server_error"), ok());
  const { patrol, errors } = create(fetchImpl);
  patrol.track({ shop: "a.myshopify.com", event: "x" });
  assert.equal(await patrol.flush(120_000), true);
  assert.equal(fetchImpl.requests.length, 3);
  assert.equal(errors.length, 0);
});

test("waits for Retry-After when rate limited", async () => {
  const fetchImpl = scripted(apiError(429, "rate_limited", { "retry-after": "7" }), ok());
  const { patrol } = create(fetchImpl);
  patrol.track({ shop: "a.myshopify.com", event: "x" });
  assert.equal(await patrol.flush(120_000), true);
  assert.ok(patrol.waits[0] >= 7000 && patrol.waits[0] <= 9000, `waited ${patrol.waits[0]}`);
});

test("keeps events queued when the server stays down", async () => {
  const { patrol, errors } = create(scripted(new TypeError("fetch failed")), { maxRetries: 2 });
  patrol.track({ shop: "a.myshopify.com", event: "x" });
  assert.equal(await patrol.flush(120_000), false);
  assert.equal(patrol.queueSize, 1);
  assert.equal(errors.at(-1)[1].willRetryLater, true);
});

test("does not retry a request the server refuses", async () => {
  const fetchImpl = scripted(apiError(422, "batch_too_large"));
  const { patrol, errors } = create(fetchImpl);
  patrol.track({ shop: "a.myshopify.com", event: "x" });
  assert.equal(await patrol.flush(), true);
  assert.equal(fetchImpl.requests.length, 1);
  assert.equal(errors[0][0].code, "batch_too_large");
  assert.equal(errors[0][0].status, 422);
});

test("reports events the server rejected and treats the batch as delivered", async () => {
  const body = { accepted: 1, duplicates: 0, errors: [{ index: 1, code: "invalid_name", message: "name may use lowercase letters" }] };
  const { patrol, errors } = create(scripted(ok(body)));
  patrol.track({ shop: "a.myshopify.com", event: "good" });
  patrol.track({ shop: "a.myshopify.com", event: "Bad Name" });
  assert.equal(await patrol.flush(), true);
  assert.equal(errors[0][0].code, "invalid_name");
  assert.equal(errors[0][1].event, "Bad Name");
});

test("stops sending after the key is rejected", async () => {
  const fetchImpl = scripted(apiError(401, "unauthorized"));
  const { patrol } = create(fetchImpl);
  patrol.track({ shop: "a.myshopify.com", event: "x" });
  await patrol.flush();
  assert.equal(patrol.track({ shop: "a.myshopify.com", event: "y" }), false);
  assert.equal(fetchImpl.requests.length, 1);
});

test("pauses when the monthly quota is used", async () => {
  const fetchImpl = scripted(apiError(429, "quota_exceeded"));
  const { patrol } = create(fetchImpl);
  patrol.track({ shop: "a.myshopify.com", event: "x" });
  await patrol.flush();
  patrol.track({ shop: "a.myshopify.com", event: "y" });
  await patrol.flush();
  assert.equal(fetchImpl.requests.length, 1);
  assert.equal(patrol.dropped, 1);
});

test("never throws into the caller", async () => {
  const { patrol } = create(scripted(new Error("boom")), {
    maxRetries: 0,
    onError: () => {
      throw new Error("handler failed");
    },
  });
  assert.doesNotThrow(() => patrol.track({ shop: "a.myshopify.com", event: "x" }));
  await assert.doesNotReject(patrol.flush());
});

test("ping returns the key's description or rejects with a clear error", async () => {
  const good = create(scripted(json(200, { ok: true, app: { name: "Cart Holder" } }))).patrol;
  assert.equal((await good.ping()).app.name, "Cart Holder");

  const bad = create(scripted(apiError(401, "unauthorized"))).patrol;
  await assert.rejects(bad.ping(), (error) => error instanceof AppsPatrolError && error.code === "unauthorized" && error.status === 401);

  const down = create(scripted(new TypeError("fetch failed"))).patrol;
  await assert.rejects(down.ping(), /Could not reach Apps Patrol/);
});

test("test mode records calls instead of sending them", async () => {
  const fetchImpl = scripted(ok());
  const patrol = new AppsPatrol({ test: true, fetch: fetchImpl });
  patrol.featureUsed("a.myshopify.com", "bulk_edit");
  patrol.identify({ shop: "a.myshopify.com", country: "DE" });
  await patrol.flush();
  assert.equal(patrol.recorded.events[0].name, "feature_used");
  assert.deepEqual(patrol.recorded.shops, [{ shop: "a.myshopify.com", country: "DE" }]);
  assert.equal(fetchImpl.requests.length, 0);
});

test("identifyAfterAuth identifies the shop and counts it as opened", async () => {
  const patrol = new AppsPatrol({ test: true });
  await identifyAfterAuth(patrol, { source: "app_store" })({ session: { shop: "a.myshopify.com" } });
  assert.deepEqual(patrol.recorded.shops, [{ shop: "a.myshopify.com", source: "app_store" }]);
  assert.equal(patrol.recorded.events[0].name, "app_opened");
});

test("patrolMiddleware tracks the daily open and binds helpers to the shop", () => {
  const patrol = new AppsPatrol({ test: true });
  const res = { locals: { shopify: { session: { shop: "a.myshopify.com" } } } };
  let called = false;
  patrolMiddleware(patrol)({}, res, () => {
    called = true;
  });
  res.locals.patrol.featureUsed("bulk_edit");
  assert.equal(called, true);
  assert.deepEqual(patrol.recorded.events.map((e) => e.name), ["app_opened", "feature_used"]);

  // no shop on the request: passes through untouched
  const anonymous = { locals: {} };
  patrolMiddleware(patrol)({}, anonymous, () => {});
  assert.equal(anonymous.locals.patrol, undefined);
});

test("the CommonJS build exports the same API", () => {
  const require = createRequire(import.meta.url);
  const cjs = require("../dist/cjs/index.js");
  assert.equal(typeof cjs.AppsPatrol, "function");
  assert.deepEqual(cjs.STANDARD_EVENTS, STANDARD_EVENTS);
  assert.equal(new cjs.AppsPatrol({ test: true }).track({ shop: "a.myshopify.com", event: "x" }), true);
});
