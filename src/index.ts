/**
 * Sends usage events and shop profiles from a Shopify app to Apps Patrol.
 *
 * Calls return immediately: items are queued and sent in batches in the
 * background. Nothing here throws into, or blocks, the host app (except
 * `ping`, which exists to surface setup problems).
 */

export const VERSION = "0.2.0";

/** Names with a meaning in Apps Patrol. Any other name is accepted too. */
export const STANDARD_EVENTS = [
  "app_opened",
  "onboarding_started",
  "onboarding_completed",
  "feature_used",
  "plan_viewed",
  "upgrade_clicked",
  "limit_reached",
  "error_shown",
] as const;

export type Properties = Record<string, unknown>;

/** Map of event name to its properties, for compile-time checking of `track`. */
export type EventMap = Record<string, Properties>;

export interface AppsPatrolOptions {
  /** The app's ingest key. Without it every call is a no-op. Defaults to `APPS_PATROL_KEY`. */
  apiKey?: string;
  /** Address of your Apps Patrol instance. Defaults to `APPS_PATROL_URL`. */
  baseUrl?: string;
  /** `false` turns every call into a no-op. */
  enabled?: boolean;
  /** Send when this many items are waiting (1 to 100). Use 1 on serverless platforms. */
  flushAt?: number;
  /** Send at least this often, in milliseconds. */
  flushInterval?: number;
  /** Items kept in memory while the server is unreachable; the oldest are dropped beyond this. */
  maxQueue?: number;
  /** Attempts per request for network errors, 5xx and rate limiting. */
  maxRetries?: number;
  /** Milliseconds allowed per request. */
  timeout?: number;
  /** Called for anything that could not be sent. */
  onError?: (error: AppsPatrolError, context: Record<string, unknown>) => void;
  /** Where problems are logged. Pass `null` to silence. */
  logger?: Pick<Console, "warn" | "error"> | null;
  /** Replace the global `fetch`, for proxies or tests. */
  fetch?: typeof fetch;
  /** Record calls in `recorded` instead of sending them, for the host app's tests. */
  test?: boolean;
}

export interface TrackInput<Name extends string = string, Props extends Properties = Properties> {
  shop: string;
  event: Name;
  properties?: Props;
  /** Defaults to a random UUID. Pass your own to make a retry idempotent. */
  eventId?: string;
  /** Defaults to now. */
  occurredAt?: Date | string;
}

export interface IdentifyInput {
  shop: string;
  name?: string;
  country?: string;
  shopifyPlan?: string;
  email?: string;
  source?: string;
  utm?: Partial<Record<"utm_source" | "utm_medium" | "utm_campaign" | "utm_content" | "utm_term", string>>;
}

export interface PingResult {
  ok: true;
  app: { name: string; public_id: string };
  organization: { name: string; plan: string };
  usage: { events_this_month: number; events_limit: number | null };
}

export class AppsPatrolError extends Error {
  readonly code?: string;
  readonly status?: number;

  constructor(message: string, options: { code?: string; status?: number } = {}) {
    super(message);
    this.name = "AppsPatrolError";
    this.code = options.code;
    this.status = options.status;
  }
}

interface WireEvent {
  event_id: string;
  shop: string;
  name: string;
  occurred_at: string;
  properties: Properties;
}

type Item = { type: "event"; payload: WireEvent } | { type: "shop"; payload: Record<string, unknown> };

interface ApiResponse {
  status: number;
  body: any;
  retryAfter?: number;
}

const MAX_BATCH = 100;
const BACKOFF_SECONDS = [1, 2, 4, 8, 16];
const QUOTA_PAUSE_MS = 60 * 60 * 1000;

// Read `process` through globalThis so the package needs no Node type definitions
// and also loads in runtimes without it.
const proc = (globalThis as { process?: { env?: Record<string, string | undefined>; once?: (event: string, listener: () => void) => void } }).process;

const env = (name: string): string | undefined => proc?.env?.[name];

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

export class AppsPatrol<Events extends EventMap = EventMap> {
  /** Calls recorded in test mode. */
  readonly recorded: { events: WireEvent[]; shops: Record<string, unknown>[] } = { events: [], shops: [] };
  /** Items dropped because the queue was full or sending was paused. */
  dropped = 0;

  private readonly apiKey: string | undefined;
  private readonly baseUrl: string;
  private readonly enabled: boolean;
  private readonly flushAt: number;
  private readonly flushInterval: number;
  private readonly maxQueue: number;
  private readonly maxRetries: number;
  private readonly timeout: number;
  private readonly onError: AppsPatrolOptions["onError"];
  private readonly logger: AppsPatrolOptions["logger"];
  private readonly fetchImpl: typeof fetch | undefined;
  private readonly test: boolean;

  private queue: Item[] = [];
  private timer: ReturnType<typeof setTimeout> | undefined;
  private sending: Promise<void> | undefined;
  private opened = new Map<string, string>();
  private halted = false;
  private pausedUntil = 0;
  private warnedInactive = false;
  private exitHook = false;

  constructor(options: AppsPatrolOptions = {}) {
    this.apiKey = options.apiKey ?? env("APPS_PATROL_KEY");
    this.baseUrl = (options.baseUrl ?? env("APPS_PATROL_URL") ?? "http://localhost:3000").replace(/\/+$/, "");
    this.enabled = options.enabled ?? true;
    this.flushAt = Math.min(Math.max(Math.trunc(options.flushAt ?? MAX_BATCH), 1), MAX_BATCH);
    this.flushInterval = options.flushInterval ?? 2000;
    this.maxQueue = options.maxQueue ?? 10_000;
    this.maxRetries = options.maxRetries ?? 5;
    this.timeout = options.timeout ?? 5000;
    this.onError = options.onError;
    this.logger = options.logger === undefined ? console : options.logger;
    this.fetchImpl = options.fetch ?? (typeof fetch === "function" ? fetch : undefined);
    this.test = options.test ?? false;
  }

  /** Queues one usage event. Returns `true` when queued, `false` when dropped. */
  track<Name extends keyof Events & string>(input: TrackInput<Name, Events[Name]>): boolean {
    if (!this.ready()) return false;
    if (!input?.shop) return this.reject("shop is required", { event: input?.event });
    if (!input.event) return this.reject("event is required", { shop: input.shop });

    const occurredAt = input.occurredAt instanceof Date ? input.occurredAt : new Date(input.occurredAt ?? Date.now());
    return this.enqueue({
      type: "event",
      payload: {
        event_id: String(input.eventId ?? randomId()),
        shop: String(input.shop),
        name: String(input.event),
        occurred_at: (Number.isNaN(occurredAt.getTime()) ? new Date() : occurredAt).toISOString(),
        properties: (input.properties ?? {}) as Properties,
      },
    });
  }

  /** Queues a shop profile and where its install came from. */
  identify(input: IdentifyInput): boolean {
    if (!this.ready()) return false;
    if (!input?.shop) return this.reject("shop is required");

    const { shopifyPlan, ...rest } = input;
    const payload: Record<string, unknown> = { ...rest };
    if (shopifyPlan !== undefined) payload.shopify_plan = shopifyPlan;
    return this.enqueue({ type: "shop", payload });
  }

  // ---- Standard events (a convention; use track for anything else)

  /**
   * Once per shop per day: the event id is derived from the shop and the date,
   * so repeats are neither queued twice here nor stored twice on the server.
   */
  appOpened(shop: string, at: Date = new Date()): boolean {
    if (!shop) return false;
    const day = at.toISOString().slice(0, 10);
    if (this.opened.get(shop) === day) return false;
    if (this.opened.size > 50_000) this.opened.clear();
    this.opened.set(shop, day);
    return this.send(shop, "app_opened", {}, { eventId: `app_opened:${shop}:${day}`, occurredAt: at });
  }

  onboardingStarted(shop: string, properties: Properties = {}): boolean {
    return this.send(shop, "onboarding_started", properties);
  }

  onboardingCompleted(shop: string, properties: Properties = {}): boolean {
    return this.send(shop, "onboarding_completed", properties);
  }

  featureUsed(shop: string, feature: string, properties: Properties = {}): boolean {
    return this.send(shop, "feature_used", { feature, ...properties });
  }

  planViewed(shop: string, properties: Properties = {}): boolean {
    return this.send(shop, "plan_viewed", properties);
  }

  upgradeClicked(shop: string, properties: Properties = {}): boolean {
    return this.send(shop, "upgrade_clicked", properties);
  }

  limitReached(shop: string, limit: string, properties: Properties = {}): boolean {
    return this.send(shop, "limit_reached", { limit, ...properties });
  }

  errorShown(shop: string, code: string, properties: Properties = {}): boolean {
    return this.send(shop, "error_shown", { code, ...properties });
  }

  /** Number of items waiting to be sent. */
  get queueSize(): number {
    return this.queue.length;
  }

  /**
   * Sends everything queued now. Resolves `true` when the queue is empty
   * afterwards. On serverless platforms, pass this promise to `waitUntil`.
   */
  async flush(timeoutMs = 5000): Promise<boolean> {
    this.clearTimer();
    const deadline = Date.now() + timeoutMs;
    while (this.sending) await this.sending;
    this.sending = this.drain(deadline).finally(() => {
      this.sending = undefined;
    });
    await this.sending;
    if (this.queue.length > 0) this.schedule();
    return this.queue.length === 0;
  }

  /** Flushes and stops the background timer. */
  async shutdown(timeoutMs = 5000): Promise<boolean> {
    return this.flush(timeoutMs);
  }

  /** Checks the key against the server. Rejects with `AppsPatrolError` on failure. */
  async ping(): Promise<PingResult> {
    if (this.test) {
      return { ok: true, app: { name: "test", public_id: "test" }, organization: { name: "test", plan: "test" }, usage: { events_this_month: 0, events_limit: null } };
    }
    let response: ApiResponse;
    try {
      response = await this.call("GET", "/v1/ping");
    } catch (error) {
      throw new AppsPatrolError(`Could not reach Apps Patrol at ${this.baseUrl}: ${(error as Error).message}`);
    }
    if (response.status >= 200 && response.status < 300) return response.body as PingResult;
    throw new AppsPatrolError(response.body?.error?.message ?? `Apps Patrol answered ${response.status}`, {
      code: response.body?.error?.code,
      status: response.status,
    });
  }

  // ---- internals

  private send(shop: string, event: string, properties: Properties, extra: { eventId?: string; occurredAt?: Date } = {}): boolean {
    return (this as AppsPatrol<EventMap>).track({ shop, event, properties, ...extra });
  }

  private ready(): boolean {
    if (!this.enabled) return false;
    if (this.test) return true;
    if (!this.apiKey) {
      if (!this.warnedInactive) {
        this.warnedInactive = true;
        this.logger?.warn("[AppsPatrol] No apiKey configured; events are not sent");
      }
      return false;
    }
    return !this.halted;
  }

  private reject(message: string, context: Record<string, unknown> = {}): false {
    this.report(new AppsPatrolError(message), context);
    return false;
  }

  private enqueue(item: Item): true {
    if (this.test) {
      if (item.type === "event") this.recorded.events.push(item.payload);
      else this.recorded.shops.push(item.payload);
      return true;
    }
    this.queue.push(item);
    if (this.queue.length > this.maxQueue) {
      this.queue.shift();
      this.dropped += 1;
    }
    this.registerExitHook();
    if (this.queue.length >= this.flushAt) void this.flush().catch(() => undefined);
    else this.schedule();
    return true;
  }

  private schedule(): void {
    if (this.timer) return;
    this.timer = setTimeout(() => {
      this.timer = undefined;
      void this.flush(30_000).catch(() => undefined);
    }, this.flushInterval);
    // Never keep the process alive just to send analytics.
    (this.timer as { unref?: () => void }).unref?.();
  }

  private clearTimer(): void {
    if (this.timer) clearTimeout(this.timer);
    this.timer = undefined;
  }

  private registerExitHook(): void {
    if (this.exitHook) return;
    this.exitHook = true;
    flushAtExit(this);
  }

  private async drain(deadline: number): Promise<void> {
    while (this.queue.length > 0) {
      const items = this.queue.splice(0, MAX_BATCH);
      if (this.halted || Date.now() < this.pausedUntil) {
        this.dropped += items.length;
        continue;
      }

      const events = items.filter((item): item is Extract<Item, { type: "event" }> => item.type === "event");
      const shops = items.filter((item): item is Extract<Item, { type: "shop" }> => item.type === "shop");
      const keep: Item[] = [];

      if (events.length > 0 && !(await this.sendEvents(events.map((item) => item.payload), deadline))) keep.push(...events);
      for (const shop of shops) {
        if (!(await this.sendShop(shop.payload, deadline))) keep.push(shop);
      }

      if (keep.length > 0) {
        // Could not be delivered before the deadline: put back for the next attempt.
        this.queue.unshift(...keep);
        return;
      }
    }
  }

  /** Resolves `true` when the request is finished (delivered or permanently refused). */
  private async sendEvents(events: WireEvent[], deadline: number): Promise<boolean> {
    const response = await this.request("POST", "/v1/events", { events }, deadline);
    if (!response) return false;

    const errors = response.body?.errors;
    if (response.status === 202 || (response.status === 422 && Array.isArray(errors))) {
      for (const error of Array.isArray(errors) ? errors : []) {
        const event = events[Number(error.index)];
        this.report(new AppsPatrolError(`Event rejected: ${error.message}`, { code: error.code, status: response.status }), {
          event: event?.name,
          shop: event?.shop,
          eventId: event?.event_id,
        });
      }
      return true;
    }
    return this.refused(response, { events: events.length });
  }

  private async sendShop(payload: Record<string, unknown>, deadline: number): Promise<boolean> {
    const response = await this.request("POST", "/v1/shops", payload, deadline);
    if (!response) return false;
    if (response.status >= 200 && response.status < 300) return true;
    return this.refused(response, { shop: payload.shop });
  }

  /** A response that will not improve by retrying. Always "finished". */
  private refused(response: ApiResponse, context: Record<string, unknown>): true {
    const code: string | undefined = response.body?.error?.code;
    if (code === "unauthorized") {
      this.halted = true;
      this.logger?.error("[AppsPatrol] The ingest key was rejected (401). Nothing is sent until the app restarts with a valid key.");
    } else if (code === "quota_exceeded") {
      this.pausedUntil = Date.now() + QUOTA_PAUSE_MS;
      this.logger?.warn("[AppsPatrol] Monthly event quota reached. Events are dropped for the next hour.");
    }
    this.report(new AppsPatrolError(response.body?.error?.message ?? `Apps Patrol answered ${response.status}`, { code, status: response.status }), context);
    return true;
  }

  /** Performs the request with retries. Resolves `undefined` when it could not be completed before the deadline. */
  private async request(method: string, path: string, body: unknown, deadline: number): Promise<ApiResponse | undefined> {
    for (let attempt = 1; ; attempt += 1) {
      let response: ApiResponse | undefined;
      let failure: Error | undefined;
      try {
        response = await this.call(method, path, body);
      } catch (error) {
        failure = error as Error;
      }

      const retryable = !response || response.status >= 500 || response.body?.error?.code === "rate_limited";
      if (response && !retryable) return response;

      const base = response?.retryAfter && response.retryAfter > 0 ? Math.min(response.retryAfter, 60) : BACKOFF_SECONDS[Math.min(attempt - 1, BACKOFF_SECONDS.length - 1)]!;
      const wait = (base + Math.random() * base * 0.25) * 1000;
      if (attempt > this.maxRetries || Date.now() + wait > deadline) {
        const error = failure
          ? new AppsPatrolError(failure.message)
          : new AppsPatrolError(`Apps Patrol answered ${response!.status}`, { code: response!.body?.error?.code, status: response!.status });
        this.report(error, { path, attempts: attempt, willRetryLater: true });
        return undefined;
      }
      await this.wait(wait);
    }
  }

  /** Overridable in tests. */
  protected wait(ms: number): Promise<void> {
    return sleep(ms);
  }

  private async call(method: string, path: string, body?: unknown): Promise<ApiResponse> {
    if (!this.fetchImpl) throw new Error("No fetch implementation available; pass options.fetch");

    const response = await this.fetchImpl(`${this.baseUrl}${path}`, {
      method,
      headers: {
        Authorization: `Bearer ${this.apiKey}`,
        "User-Agent": `apps-patrol-node/${VERSION}`,
        ...(body === undefined ? {} : { "Content-Type": "application/json" }),
      },
      body: body === undefined ? undefined : JSON.stringify(body),
      signal: typeof AbortSignal !== "undefined" && "timeout" in AbortSignal ? AbortSignal.timeout(this.timeout) : undefined,
    });

    let parsed: unknown = null;
    try {
      parsed = await response.json();
    } catch {
      parsed = null;
    }
    const retryAfter = Number(response.headers.get("retry-after"));
    return { status: response.status, body: parsed, retryAfter: Number.isFinite(retryAfter) ? retryAfter : undefined };
  }

  private report(error: AppsPatrolError, context: Record<string, unknown> = {}): void {
    try {
      this.logger?.warn(`[AppsPatrol] ${error.message}`, context);
      this.onError?.(error, context);
    } catch {
      // a failing handler must not break the host app
    }
  }
}

// One process-wide exit hook, however many clients exist.
const clients = new Set<WeakRef<AppsPatrol<any>>>();
let exitHookInstalled = false;

function flushAtExit(client: AppsPatrol<any>): void {
  if (typeof proc?.once !== "function" || typeof WeakRef === "undefined") return;
  clients.add(new WeakRef(client));
  if (exitHookInstalled) return;
  exitHookInstalled = true;
  proc.once("beforeExit", () => {
    for (const ref of clients) void ref.deref()?.flush().catch(() => undefined);
  });
}

function randomId(): string {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") return crypto.randomUUID();
  return `${Date.now().toString(16)}-${Math.random().toString(16).slice(2)}-${Math.random().toString(16).slice(2)}`;
}

// ---- Shopify app helpers

/**
 * An `afterAuth` hook for `@shopify/shopify-app-remix` and
 * `@shopify/shopify-app-react-router`: identifies the shop after install or sign-in.
 *
 *   hooks: { afterAuth: identifyAfterAuth(patrol, { source: "app_store" }) }
 */
export function identifyAfterAuth(patrol: AppsPatrol<any>, traits: Omit<IdentifyInput, "shop"> = {}) {
  return async ({ session }: { session: { shop: string } }): Promise<void> => {
    patrol.identify({ shop: session.shop, ...traits });
    patrol.appOpened(session.shop);
  };
}

/**
 * Express middleware: sends the daily `app_opened` for the request's shop and
 * exposes `res.locals.patrol` helpers bound to that shop.
 *
 *   app.use("/api", shopify.validateAuthenticatedSession(), patrolMiddleware(patrol));
 */
export function patrolMiddleware(
  patrol: AppsPatrol<any>,
  options: { getShop?: (req: any, res: any) => string | undefined } = {},
) {
  const getShop = options.getShop ?? ((_req: any, res: any) => res?.locals?.shopify?.session?.shop as string | undefined);
  return (req: any, res: any, next: (error?: unknown) => void): void => {
    try {
      const shop = getShop(req, res);
      if (shop) {
        patrol.appOpened(shop);
        res.locals = res.locals ?? {};
        res.locals.patrol = {
          shop,
          track: (event: string, properties: Properties = {}) => patrol.track({ shop, event, properties }),
          featureUsed: (feature: string, properties: Properties = {}) => patrol.featureUsed(shop, feature, properties),
        };
      }
    } catch {
      // never break the request
    }
    next();
  };
}
