# @apps-patrol/node

Node client for Apps Patrol. Sends usage events and shop profiles from your Shopify app.

- Calls return immediately; batches are sent in the background with retries
- TypeScript, with ES module and CommonJS builds. No dependencies
- Node 20 or newer
- Version 0.2.0, not published to npm yet

The full reference is the `/docs/node` page of your Apps Patrol site. What to track: `/docs/events`.

## Install

```bash
npm install @apps-patrol/node
# or from git before the npm publish:
# npm install github:MahdiKetata99/apps-patrol-node
```

## Set up

Create an ingest key for your app under **Settings → Apps & integration**, then:

```ts
// app/patrol.server.ts
import { AppsPatrol } from "@apps-patrol/node";

export const patrol = new AppsPatrol({
  apiKey: process.env.APPS_PATROL_KEY,
  baseUrl: "https://your-apps-patrol-host",
  enabled: process.env.NODE_ENV === "production",
});
```

Check it once:

```ts
await patrol.ping(); // rejects with AppsPatrolError if the key or the address is wrong
```

## Use

```ts
patrol.identify({ shop: session.shop, name: "Example Store", country: "DE", source: "app_store" });

patrol.featureUsed(session.shop, "bulk_edit");
patrol.track({ shop: session.shop, event: "reservation_created", properties: { items: 3 } });
patrol.appOpened(session.shop); // at most once per shop per day
```

Standard event helpers: `appOpened`, `onboardingStarted`, `onboardingCompleted`, `featureUsed`, `planViewed`, `upgradeClicked`, `limitReached`, `errorShown`. Any other name works with `track`.

Shopify helpers: `identifyAfterAuth(patrol, traits)` for the `afterAuth` hook of the Remix and React Router packages, and `patrolMiddleware(patrol)` for Express.

On serverless platforms use `flushAt: 1` and `ctx.waitUntil(patrol.flush())`, or `await patrol.flush()`.

## Options

| Option | Default | Description |
|---|---|---|
| `apiKey` | `APPS_PATROL_KEY` | The app's ingest key |
| `baseUrl` | `APPS_PATROL_URL` or `http://localhost:3000` | Your Apps Patrol address |
| `enabled` | `true` | `false` makes every call a no-op |
| `flushAt` | `100` | Send when this many items wait; `1` for serverless |
| `flushInterval` | `2000` | Milliseconds between sends |
| `maxQueue` | `10000` | Items kept while the server is unreachable |
| `maxRetries` | `5` | Attempts per request |
| `timeout` | `5000` | Milliseconds per request |
| `onError` | none | `(error, context) => {}` for anything that could not be sent |
| `logger` | `console` | `null` silences it |
| `fetch` | global `fetch` | Replace to use a proxy |
| `test` | `false` | Record calls in `patrol.recorded` instead of sending |

## Behaviour

- Never throws into your app (except `ping`).
- Keeps items queued and retries while the server is down; drops the oldest beyond `maxQueue`.
- Stops sending after a rejected key (401) until restart; pauses for an hour when the monthly quota is used.
- The timer never keeps the process alive; the queue is flushed when the event loop empties.

## Development

```bash
cd packages/apps-patrol-node
npm test
```

`npm test` builds `dist/` with the TypeScript compiler and runs the tests with Node's built-in runner. TypeScript is taken from this package's `node_modules` or, inside the monorepo, from the repository root.

## License

MIT
