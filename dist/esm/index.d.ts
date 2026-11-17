/**
 * Sends usage events and shop profiles from a Shopify app to Apps Patrol.
 *
 * Calls return immediately: items are queued and sent in batches in the
 * background. Nothing here throws into, or blocks, the host app (except
 * `ping`, which exists to surface setup problems).
 */
export declare const VERSION = "0.2.0";
/** Names with a meaning in Apps Patrol. Any other name is accepted too. */
export declare const STANDARD_EVENTS: readonly ["app_opened", "onboarding_started", "onboarding_completed", "feature_used", "plan_viewed", "upgrade_clicked", "limit_reached", "error_shown"];
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
    app: {
        name: string;
        public_id: string;
    };
    organization: {
        name: string;
        plan: string;
    };
    usage: {
        events_this_month: number;
        events_limit: number | null;
    };
}
export declare class AppsPatrolError extends Error {
    readonly code?: string;
    readonly status?: number;
    constructor(message: string, options?: {
        code?: string;
        status?: number;
    });
}
interface WireEvent {
    event_id: string;
    shop: string;
    name: string;
    occurred_at: string;
    properties: Properties;
}
export declare class AppsPatrol<Events extends EventMap = EventMap> {
    /** Calls recorded in test mode. */
    readonly recorded: {
        events: WireEvent[];
        shops: Record<string, unknown>[];
    };
    /** Items dropped because the queue was full or sending was paused. */
    dropped: number;
    private readonly apiKey;
    private readonly baseUrl;
    private readonly enabled;
    private readonly flushAt;
    private readonly flushInterval;
    private readonly maxQueue;
    private readonly maxRetries;
    private readonly timeout;
    private readonly onError;
    private readonly logger;
    private readonly fetchImpl;
    private readonly test;
    private queue;
    private timer;
    private sending;
    private opened;
    private halted;
    private pausedUntil;
    private warnedInactive;
    private exitHook;
    constructor(options?: AppsPatrolOptions);
    /** Queues one usage event. Returns `true` when queued, `false` when dropped. */
    track<Name extends keyof Events & string>(input: TrackInput<Name, Events[Name]>): boolean;
    /** Queues a shop profile and where its install came from. */
    identify(input: IdentifyInput): boolean;
    /**
     * Once per shop per day: the event id is derived from the shop and the date,
     * so repeats are neither queued twice here nor stored twice on the server.
     */
    appOpened(shop: string, at?: Date): boolean;
    onboardingStarted(shop: string, properties?: Properties): boolean;
    onboardingCompleted(shop: string, properties?: Properties): boolean;
    featureUsed(shop: string, feature: string, properties?: Properties): boolean;
    planViewed(shop: string, properties?: Properties): boolean;
    upgradeClicked(shop: string, properties?: Properties): boolean;
    limitReached(shop: string, limit: string, properties?: Properties): boolean;
    errorShown(shop: string, code: string, properties?: Properties): boolean;
    /** Number of items waiting to be sent. */
    get queueSize(): number;
    /**
     * Sends everything queued now. Resolves `true` when the queue is empty
     * afterwards. On serverless platforms, pass this promise to `waitUntil`.
     */
    flush(timeoutMs?: number): Promise<boolean>;
    /** Flushes and stops the background timer. */
    shutdown(timeoutMs?: number): Promise<boolean>;
    /** Checks the key against the server. Rejects with `AppsPatrolError` on failure. */
    ping(): Promise<PingResult>;
    private send;
    private ready;
    private reject;
    private enqueue;
    private schedule;
    private clearTimer;
    private registerExitHook;
    private drain;
    /** Resolves `true` when the request is finished (delivered or permanently refused). */
    private sendEvents;
    private sendShop;
    /** A response that will not improve by retrying. Always "finished". */
    private refused;
    /** Performs the request with retries. Resolves `undefined` when it could not be completed before the deadline. */
    private request;
    /** Overridable in tests. */
    protected wait(ms: number): Promise<void>;
    private call;
    private report;
}
/**
 * An `afterAuth` hook for `@shopify/shopify-app-remix` and
 * `@shopify/shopify-app-react-router`: identifies the shop after install or sign-in.
 *
 *   hooks: { afterAuth: identifyAfterAuth(patrol, { source: "app_store" }) }
 */
export declare function identifyAfterAuth(patrol: AppsPatrol<any>, traits?: Omit<IdentifyInput, "shop">): ({ session }: {
    session: {
        shop: string;
    };
}) => Promise<void>;
/**
 * Express middleware: sends the daily `app_opened` for the request's shop and
 * exposes `res.locals.patrol` helpers bound to that shop.
 *
 *   app.use("/api", shopify.validateAuthenticatedSession(), patrolMiddleware(patrol));
 */
export declare function patrolMiddleware(patrol: AppsPatrol<any>, options?: {
    getShop?: (req: any, res: any) => string | undefined;
}): (req: any, res: any, next: (error?: unknown) => void) => void;
export {};
