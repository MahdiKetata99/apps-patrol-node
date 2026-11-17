# Changelog

## 0.2.0 (2026-11-17)

A rewrite in TypeScript.

- **Breaking:** `track` and `identify` are synchronous and return a boolean; they queue instead of sending.
- **Breaking:** `track` takes `eventId` and `occurredAt` (previously `event_id` and `occurred_at`).
- ES module and CommonJS builds with type declarations. No dependencies.
- Batches of up to 100, sent when `flushAt` items wait or every `flushInterval` milliseconds.
- Retries with backoff for network errors, 5xx and rate limiting, honouring `Retry-After`.
- Events the server rejects and requests it refuses are logged and passed to `onError`.
- `ping()`, `flush()` and `shutdown()`; the queue is flushed when the process exits.
- Helpers for the standard events, including `appOpened` once per shop per day.
- `identifyAfterAuth` for Shopify Remix and React Router apps, `patrolMiddleware` for Express.
- Optional compile-time checking of event names and properties: `new AppsPatrol<Events>()`.
- `test: true` records calls instead of sending them.
- `apiKey` and `baseUrl` default to `APPS_PATROL_KEY` and `APPS_PATROL_URL`.

## 0.1.0

Initial sketch: one request per call, errors swallowed.
