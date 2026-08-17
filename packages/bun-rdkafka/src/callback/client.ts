/**
 * callback/client.ts — the Callback API's base `Client` class (design §6).
 *
 * Semantics cross-checked against node-rdkafka/confluent-kafka-javascript's
 * `Client` (`lib/client.js`): connect/disconnect flow, event names, err-first
 * callbacks. Internals are free (api-compat-not-internals principle): there is
 * no N-API thread — every event is pumped by the {@link PollScheduler} through
 * `pollEvents()` and emitted on the EventEmitter here.
 *
 * ## Surface for subclasses (M2 Producer / M3 KafkaConsumer)
 *
 *  - `this.native`   — {@link NativeClient} (undefined before connect/after disconnect)
 *  - `this.scheduler`— the running {@link PollScheduler} (undefined before connect)
 *  - `this.jsOptions`— normalized `js.*` options
 *  - `this.configCallbacks` — callback properties from the config (`dr_cb`, `rebalance_cb`…)
 *  - `onEventFrame(ev)` — hook receiving event frames the base does NOT handle
 *    (DR / REBALANCE / OFFSET_COMMIT / OAUTH_REFRESH / ADMIN_RESULT).
 *    The base only wires 4 kinds: ERROR, LOG, STATS, THROTTLE.
 *  - `pollTick()`   — called on EVERY poll round after `pollEvents()`; returns
 *    the number of items handled (the consumer `consumeBatch()`s here). Base
 *    returns 0.
 *  - `isCold()`     — allows the scheduler to fall into COLD (500ms, unref).
 *    Base returns `true`; the producer MUST override (`outq > 0` ⇒ false), and
 *    a subscribed consumer MUST override to `false` (two-queue constraint — see
 *    poll-scheduler.ts).
 */

import { EventEmitter } from "node:events";
import type { BrkNative } from "../ffi/loader.ts";
import {
  BRK_CLIENT_PRODUCER,
  BRK_EVENT_ADMIN_RESULT,
  BRK_EVENT_ERROR,
  BRK_EVENT_LOG,
  BRK_EVENT_OAUTH_REFRESH,
  BRK_EVENT_STATS,
  BRK_EVENT_THROTTLE,
  type BrkAdminOp,
  type BrkClientType,
} from "../ffi/types.ts";
import type {
  BrkAdminResultEvent,
  BrkEvent,
  BrkOauthRefreshEvent,
} from "../core/batch-decoder.ts";
import {
  ConfigBuilder,
  type BuiltConfig,
  type ClientConfig,
  type ConfigCallbacks,
  type JsOptions,
} from "../core/config.ts";
import { ERROR_CODES, LibrdKafkaError } from "../core/errors.ts";
import {
  NativeClient,
  type NativeClientOptions,
  type WatermarkOffsets,
} from "../core/native-client.ts";
import { PollScheduler, type SchedulerTimers } from "../core/poll-scheduler.ts";

/* ========================================================================== */
/* Public types                                                                */
/* ========================================================================== */

/** Metadata shape of upstream's `getMetadata()` (passed through from the shim). */
export interface BrokerMetadata {
  id: number;
  host: string;
  port: number;
}

export interface PartitionMetadata {
  id: number;
  leader: number;
  replicas: number[];
  isrs: number[];
}

export interface TopicMetadata {
  name: string;
  partitions: PartitionMetadata[];
}

export interface Metadata {
  orig_broker_id: number;
  orig_broker_name: string;
  brokers: BrokerMetadata[];
  topics: TopicMetadata[];
}

export interface MetadataOptions {
  topic?: string;
  /** ms; defaults to 30000. */
  timeout?: number;
}

/** First arg of the `ready` event. */
export interface ReadyInfo {
  name: string;
}

/** Arg of the `disconnected` event + the `disconnect()` callback. */
export interface ClientMetrics {
  /** epoch ms at the time the native handle was created. */
  connectionOpened: number;
}

export interface LogEventPayload {
  severity: number;
  fac: string;
  message: string;
}

export interface StatsEventPayload {
  /** Raw JSON from librdkafka (NFR-8). */
  message: string;
}

export interface ThrottleEventPayload {
  brokerName: string;
  brokerId: number;
  throttleTime: number;
}

/** `{ lowOffset, highOffset }` — upstream's `queryWatermarkOffsets` shape. */
export interface WatermarkOffsetsResult {
  lowOffset: number;
  highOffset: number;
}

/**
 * Token returned by `oauthbearer_token_refresh_cb` — upstream's shape:
 * `{ tokenValue, lifetime, principal?, extensions? }`, where `lifetime` is the
 * **absolute epoch-ms** at which the token expires. The `value`/`token` aliases
 * are also accepted (for legacy code) — `tokenValue` wins.
 */
export interface OauthBearerToken {
  tokenValue?: string;
  value?: string;
  token?: string;
  /** Epoch-ms when the token expires; defaults to `Date.now() + 3_600_000`. */
  lifetime?: number;
  principal?: string;
  /** `{k: v}` or a flat array `[k1, v1, k2, v2, …]`. */
  extensions?: Record<string, string> | readonly string[];
}

export type OauthBearerTokenCallback = (
  err: Error | null,
  token?: OauthBearerToken,
) => void;

export type ConnectCallback = (err: LibrdKafkaError | null, metadata?: Metadata) => void;
export type MetadataCallback = (err: LibrdKafkaError | null, metadata?: Metadata) => void;
export type DisconnectCallback = (err: LibrdKafkaError | null, metrics?: ClientMetrics) => void;
export type WatermarkCallback = (
  err: LibrdKafkaError | null,
  offsets?: WatermarkOffsetsResult,
) => void;

/** Injection point for tests (fake native, fake timers) — not public API. */
export interface ClientInternalOptions {
  native?: BrkNative;
  timers?: SchedulerTimers;
  onLeak?: (label: string) => void;
}

const DEFAULT_METADATA_TIMEOUT_MS = 30_000;
const DEFAULT_WATERMARK_TIMEOUT_MS = 1_000;

/** Numbers client names by type: `producer#1`, `consumer#3`… */
const nameCounters: Record<string, number> = {};

function nextName(type: BrkClientType): string {
  const label = type === BRK_CLIENT_PRODUCER ? "producer" : "consumer";
  nameCounters[label] = (nameCounters[label] ?? 0) + 1;
  return `${label}#${nameCounters[label]}`;
}

function toLibrdKafkaError(error: unknown, context: string): LibrdKafkaError {
  if (error instanceof LibrdKafkaError) return error;
  const message = error instanceof Error ? error.message : String(error);
  return new LibrdKafkaError(message, {
    code: ERROR_CODES.ERR__FAIL,
    origin: "local",
    context,
  });
}

function notConnectedError(context: string): LibrdKafkaError {
  return new LibrdKafkaError(`bun-rdkafka: ${context}() requires a connected client`, {
    code: ERROR_CODES.ERR__STATE,
    origin: "local",
    context,
  });
}

/* ========================================================================== */
/* Client                                                                      */
/* ========================================================================== */

export class Client extends EventEmitter {
  readonly clientType: BrkClientType;

  protected native: NativeClient | undefined;
  protected scheduler: PollScheduler | undefined;

  private readonly built: BuiltConfig;
  private readonly internal: ClientInternalOptions;
  private readonly nameValue: string;
  private readyAtMs = 0;
  private connectionOpenedAt = 0;
  /** Pending ADMIN_RESULTs by correlation_id (see `_registerAdminResult`). */
  private readonly adminPending = new Map<bigint, (event: BrkAdminResultEvent) => void>();
  /** Last error seen by this client (upstream `getLastError()`); `null` until one happens. */
  private lastError: LibrdKafkaError | null = null;
  /** `event_cb` opt-in: re-emit every raw event frame as `event.event` / `event`. */
  private readonly emitRawEvents: boolean;

  constructor(
    globalConf: ClientConfig | undefined,
    topicConf: ClientConfig | undefined,
    clientType: BrkClientType,
    internal: ClientInternalOptions = {},
  ) {
    super();
    this.clientType = clientType;
    this.built = new ConfigBuilder(globalConf, topicConf).build();
    this.internal = internal;
    this.nameValue = nextName(clientType);

    // Upstream remembers the last `event.error` for getLastError() — subclasses
    // emit through the same event, so one listener covers every path.
    this.on("event.error", (err: unknown) => {
      if (err instanceof LibrdKafkaError) this.lastError = err;
    });

    const eventCb = this.built.callbacks.event_cb;
    this.emitRawEvents = eventCb !== undefined;
    if (typeof eventCb === "function") this.on("event.event", eventCb as (event: BrkEvent) => void);
  }

  /** Client name, of the form `producer#1` / `consumer#2`. */
  get name(): string {
    return this.nameValue;
  }

  /**
   * The last `LibrdKafkaError` this client saw (connect failure, `event.error`,
   * a failed native call surfaced through a callback), or `null` — upstream's
   * `Client#getLastError()`.
   */
  getLastError(): LibrdKafkaError | null {
    return this.lastError;
  }

  /** Records `err` as the last error (subclasses call this from their error paths). */
  protected recordError<E extends LibrdKafkaError>(err: E): E {
    this.lastError = err;
    return err;
  }

  protected get jsOptions(): JsOptions {
    return this.built.js;
  }

  protected get configCallbacks(): ConfigCallbacks {
    return this.built.callbacks;
  }

  /* ------------------------------------------------------------- connect */

  /**
   * Creates the native handle + starts the PollScheduler, then fetches the
   * first metadata to confirm the connection: success → emit `ready` +
   * `cb(null, metadata)`; failure → tear the handle down, emit `event.error` +
   * `cb(err)`.
   *
   * Note: the metadata call blocks up to `metadataOptions.timeout` (default
   * 30s) — the only cold path allowed to block (design §5.1).
   */
  connect(metadataOptions?: MetadataOptions | ConnectCallback, cb?: ConnectCallback): this {
    let opts: MetadataOptions = {};
    if (typeof metadataOptions === "function") cb = metadataOptions;
    else if (metadataOptions) opts = metadataOptions;

    if (this.native !== undefined) {
      const err = new LibrdKafkaError(
        `bun-rdkafka: ${this.name} is already connected (state ${this.native.state}) — disconnect() before connecting again`,
        { code: ERROR_CODES.ERR__STATE, origin: "local", context: "connect" },
      );
      queueMicrotask(() => cb?.(err));
      return this;
    }

    let nc: NativeClient;
    try {
      nc = new NativeClient(this.nativeOptions());
      nc.connect();
    } catch (error) {
      const err = this.recordError(toLibrdKafkaError(error, "connect"));
      queueMicrotask(() => {
        this.emit("event.error", err);
        cb?.(err);
      });
      return this;
    }

    this.native = nc;
    this.connectionOpenedAt = Date.now();
    this.scheduler = this.createScheduler();
    this.scheduler.start();

    const timeout = opts.timeout ?? DEFAULT_METADATA_TIMEOUT_MS;
    const topic = opts.topic ?? null;
    queueMicrotask(() => {
      // disconnect() may have slipped in between: skip silently.
      if (this.native !== nc || !nc.isOpen) return;
      let metadata: Metadata;
      try {
        metadata = JSON.parse(nc.metadata(topic, timeout)) as Metadata;
      } catch (error) {
        const err = this.recordError(toLibrdKafkaError(error, "connect"));
        const metrics: ClientMetrics = { connectionOpened: this.connectionOpenedAt };
        this.teardown(nc);
        this.emit("event.error", err);
        // Upstream: the handle came up but the first metadata fetch failed →
        // `connection.failure` (err, metrics) in addition to the connect cb.
        this.emit("connection.failure", err, metrics);
        cb?.(err);
        return;
      }
      nc.markReady();
      this.readyAtMs = Date.now();
      this.emit("ready", { name: this.name } satisfies ReadyInfo, metadata);
      cb?.(null, metadata);
    });
    return this;
  }

  /** Stops the scheduler, destroys the handle, emits `disconnected` (with metrics). */
  disconnect(cb?: DisconnectCallback): this {
    const nc = this.native;
    if (nc === undefined) {
      queueMicrotask(() => cb?.(null, undefined));
      return this;
    }
    const metrics: ClientMetrics = { connectionOpened: this.connectionOpenedAt };
    this.teardown(nc);
    queueMicrotask(() => {
      this.emit("disconnected", metrics);
      cb?.(null, metrics);
    });
    return this;
  }

  isConnected(): boolean {
    return this.native?.state === "READY";
  }

  /** Milliseconds spent connected; 0 if not (or no longer) connected. */
  connectedTime(): number {
    return this.isConnected() ? Date.now() - this.readyAtMs : 0;
  }

  /* ------------------------------------------------------- metadata & co */

  getMetadata(metadataOptions?: MetadataOptions | MetadataCallback, cb?: MetadataCallback): this {
    let opts: MetadataOptions = {};
    if (typeof metadataOptions === "function") cb = metadataOptions;
    else if (metadataOptions) opts = metadataOptions;
    const nc = this.native;
    const topic = opts.topic ?? null;
    const timeout = opts.timeout ?? DEFAULT_METADATA_TIMEOUT_MS;
    queueMicrotask(() => {
      if (nc === undefined || !nc.isOpen) {
        cb?.(this.recordError(notConnectedError("getMetadata")));
        return;
      }
      try {
        cb?.(null, JSON.parse(nc.metadata(topic, timeout)) as Metadata);
      } catch (error) {
        cb?.(this.recordError(toLibrdKafkaError(error, "getMetadata")));
      }
    });
    return this;
  }

  queryWatermarkOffsets(
    topic: string,
    partition: number,
    timeout?: number | WatermarkCallback,
    cb?: WatermarkCallback,
  ): this {
    let timeoutMs = DEFAULT_WATERMARK_TIMEOUT_MS;
    if (typeof timeout === "function") cb = timeout;
    else if (timeout !== undefined) timeoutMs = timeout;
    const nc = this.native;
    queueMicrotask(() => {
      if (nc === undefined || !nc.isOpen) {
        cb?.(this.recordError(notConnectedError("queryWatermarkOffsets")));
        return;
      }
      try {
        const wm: WatermarkOffsets = nc.queryWatermark(topic, partition, timeoutMs);
        cb?.(null, { lowOffset: wm.low, highOffset: wm.high });
      } catch (error) {
        cb?.(this.recordError(toLibrdKafkaError(error, "queryWatermarkOffsets")));
      }
    });
    return this;
  }

  /**
   * Changes the SASL PLAIN/SCRAM credentials (upstream's
   * `Client#setSaslCredentials` API). Existing connections are not torn down;
   * the new credentials apply to the next authentication. Throws a
   * LibrdKafkaError synchronously when not connected or when the mechanism is
   * not PLAIN/SCRAM.
   */
  setSaslCredentials(username: string, password: string): this {
    const nc = this.native;
    if (nc === undefined || !nc.isOpen) {
      throw notConnectedError("setSaslCredentials");
    }
    nc.saslSetCredentials(username, password);
    return this;
  }

  /* --------------------------------------------- transport for AdminClient */

  /**
   * Sends an admin request on this client's handle; the result arrives via an
   * ADMIN_RESULT event carrying the same `correlationId` (registered with
   * `_registerAdminResult`).
   *
   * @internal — used by `AdminClient` (including `createFrom(existingClient)`),
   * not public API.
   */
  _adminRequest(op: BrkAdminOp, correlationId: bigint, requestJson: string): void {
    const nc = this.native;
    if (nc === undefined || !nc.isOpen) throw notConnectedError("adminRequest");
    nc.adminRequest(op, correlationId, requestJson);
  }

  /** @internal Registers a handler for one ADMIN_RESULT. Self-removes when fired. */
  _registerAdminResult(
    correlationId: bigint,
    handler: (event: BrkAdminResultEvent) => void,
  ): void {
    this.adminPending.set(correlationId, handler);
  }

  /** @internal Removes a handler (JS-side timeout). Returns `true` if it was still registered. */
  _unregisterAdminResult(correlationId: bigint): boolean {
    return this.adminPending.delete(correlationId);
  }

  /* -------------------------------------------------- hooks for subclasses */

  /**
   * Receives the event frames the base does not handle (DR, REBALANCE,
   * OFFSET_COMMIT, OAUTH_REFRESH, ADMIN_RESULT). The base ignores them —
   * subclasses override.
   */
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  protected onEventFrame(_event: BrkEvent): void {
    /* no-op */
  }

  /**
   * Extra work on each poll round (after `pollEvents()`); returns the number of
   * items handled. KafkaConsumer `consumeBatch()`s here (two-queue constraint).
   */
  protected pollTick(): number {
    return 0;
  }

  /**
   * Allows the scheduler to fall into COLD (`js.poll.interval.ms` interval,
   * unref). Base: `true`. Producer/Consumer override — see the doc at the top
   * of this file.
   */
  protected isCold(): boolean {
    return true;
  }

  /* ----------------------------------------------------------- internals */

  private nativeOptions(): NativeClientOptions {
    const options: NativeClientOptions = {
      type: this.clientType,
      properties: this.built.properties,
      js: this.built.js,
      label: this.nameValue,
    };
    if (this.internal.native !== undefined) options.native = this.internal.native;
    if (this.internal.onLeak !== undefined) options.onLeak = this.internal.onLeak;
    return options;
  }

  private createScheduler(): PollScheduler {
    const scheduler = new PollScheduler({
      poll: () => this.pump(),
      idleMaxMs: this.built.js.pollIdleMaxMs,
      coldIntervalMs: this.built.js.pollIntervalMs,
      isCold: () => this.isCold(),
      onError: (error) => {
        this.emit("event.error", toLibrdKafkaError(error, "poll"));
      },
      ...(this.internal.timers !== undefined ? { timers: this.internal.timers } : {}),
    });
    return scheduler;
  }

  /** One poll round: drain the event queue, then let the subclass do its work. */
  private pump(): number {
    const nc = this.native;
    if (nc === undefined || !nc.isOpen) return 0;
    const events = nc.pollEvents();
    for (const event of events) {
      this.dispatchEvent(event);
      if (this.emitRawEvents) {
        // `event_cb` opt-in: the decoded frame as-is (`{ type, ...payload }`),
        // under upstream's `event.event` name and the short `event` alias.
        this.emit("event.event", event);
        this.emit("event", event);
      }
    }
    return events.length + this.pollTick();
  }

  private dispatchEvent(event: BrkEvent): void {
    switch (event.type) {
      case BRK_EVENT_ERROR:
        this.emit(
          "event.error",
          LibrdKafkaError.fromKafkaCode(event.code, event.reason || undefined, {
            isFatal: event.isFatal,
            context: "event",
          }),
        );
        return;
      case BRK_EVENT_LOG:
        this.emit("event.log", {
          severity: event.level,
          fac: event.fac,
          message: event.message,
        } satisfies LogEventPayload);
        return;
      case BRK_EVENT_STATS:
        this.emit("event.stats", { message: event.json } satisfies StatsEventPayload);
        return;
      case BRK_EVENT_THROTTLE:
        this.emit("event.throttle", {
          brokerName: event.brokerName,
          brokerId: event.brokerId,
          throttleTime: event.throttleMs,
        } satisfies ThrottleEventPayload);
        return;
      case BRK_EVENT_ADMIN_RESULT: {
        const handler = this.adminPending.get(event.correlationId);
        if (handler !== undefined) {
          this.adminPending.delete(event.correlationId);
          handler(event);
          return;
        }
        this.onEventFrame(event); // nobody waiting (JS-side timeout?) — hand to the subclass
        return;
      }
      case BRK_EVENT_OAUTH_REFRESH:
        this.handleOauthRefresh(event);
        return;
      default:
        this.onEventFrame(event);
    }
  }

  /**
   * OAUTH_REFRESH event → invokes the user's `oauthbearer_token_refresh_cb`
   * (upstream semantics: receives `oauthbearer_config`, returns the token via
   * an err-first callback OR a Promise), then pushes the token down to
   * librdkafka.
   *
   * Note: the shim always enables this event, so with
   * `sasl.mechanism=OAUTHBEARER` and NO `oauthbearer_token_refresh_cb`,
   * librdkafka's default unsecured-JWT mechanism does not run — report a clear
   * error instead of hanging silently.
   */
  private handleOauthRefresh(event: BrkOauthRefreshEvent): void {
    const nc = this.native;
    const cb = this.configCallbacks.oauthbearer_token_refresh_cb;
    if (typeof cb !== "function") {
      this.emit(
        "event.error",
        new LibrdKafkaError(
          "bun-rdkafka: received an OAUTHBEARER token refresh but the config has no " +
            "`oauthbearer_token_refresh_cb` — sasl.mechanism=OAUTHBEARER requires this callback",
          { code: ERROR_CODES.ERR__AUTHENTICATION, origin: "local", context: "oauthbearer" },
        ),
      );
      return;
    }
    const finish: OauthBearerTokenCallback = (err, token) => {
      // disconnect() may slip in while the user resolves the token.
      if (this.native !== nc || nc === undefined || !nc.isOpen) return;
      try {
        if (err !== null || token === undefined) {
          nc.setOauthBearerTokenFailure(err?.message ?? "token refresh failed");
          this.emit("event.error", toLibrdKafkaError(err ?? "token refresh failed", "oauthbearer"));
          return;
        }
        const value = token.tokenValue ?? token.value ?? token.token;
        if (typeof value !== "string" || value.length === 0) {
          throw new Error("oauthbearer_token_refresh_cb: token is missing tokenValue");
        }
        const lifetime = token.lifetime ?? Date.now() + 3_600_000;
        const extensions = Array.isArray(token.extensions)
          ? token.extensions
          : token.extensions !== undefined
            ? Object.entries(token.extensions as Record<string, string>).flat()
            : undefined;
        nc.setOauthBearerToken(value, lifetime, token.principal ?? "", extensions);
      } catch (error) {
        try {
          nc.setOauthBearerTokenFailure(error instanceof Error ? error.message : String(error));
        } catch {
          /* handle just closed — ignore */
        }
        this.emit("event.error", toLibrdKafkaError(error, "oauthbearer"));
      }
    };
    let result: unknown;
    try {
      result = (cb as (config: string, done: OauthBearerTokenCallback) => unknown)(
        event.oauthbearerConfig,
        finish,
      );
    } catch (error) {
      finish(error instanceof Error ? error : new Error(String(error)));
      return;
    }
    // Supports the cb returning a Promise<OauthBearerToken> (KafkaJS style) instead of calling done.
    if (result instanceof Promise) {
      result.then(
        (token) => finish(null, token as OauthBearerToken),
        (error: unknown) => finish(error instanceof Error ? error : new Error(String(error))),
      );
    }
  }

  /** Stops the scheduler + destroys native, returning the client to a pre-connect state. */
  private teardown(nc: NativeClient): void {
    this.scheduler?.stop();
    this.scheduler = undefined;
    nc.disconnect();
    if (this.native === nc) this.native = undefined;
    this.readyAtMs = 0;
    // No ADMIN_RESULT will ever arrive now — fail the pending requests.
    if (this.adminPending.size > 0) {
      const pending = [...this.adminPending.values()];
      this.adminPending.clear();
      const err: BrkAdminResultEvent = {
        type: BRK_EVENT_ADMIN_RESULT,
        correlationId: -1n,
        code: ERROR_CODES.ERR__DESTROY,
        json: "",
      };
      for (const handler of pending) handler(err);
    }
  }
}
