/**
 * core/config.ts — `ConfigBuilder` (design §5.5).
 *
 * Takes a node-rdkafka-style config object (`{'metadata.broker.list': …,
 * 'linger.ms': …}`) and splits it into three groups:
 *
 *  1. **librdkafka properties** → stringified with the right type, pushed down
 *     to `brk_conf_set` pair by pair (the shim routes global vs topic-level).
 *  2. **`js.*` options** → TypeScript-layer configuration (never sent to C).
 *  3. **function properties** (`rebalance_cb`, `offset_commit_cb`,
 *     `oauthbearer_token_refresh_cb`…) → handlers running on the JS thread.
 */

/** Loose type for user callbacks — avoids contravariance constraints. */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type AnyCallback = (...args: any[]) => unknown;

/**
 * Input config as consumed by the {@link ConfigBuilder}: librdkafka
 * properties + `js.*` + callbacks, one flat level. The public constructors
 * take the typed variants ({@link ProducerConfig}, {@link KafkaConsumerConfig},
 * …), which are assignable to this.
 */
export type ClientConfig = Record<string, unknown>;

/**
 * The `js.*` options (TypeScript-layer tuning; never forwarded to librdkafka).
 * Full explanations and recommended values: README, "Configuration: `js.*` options".
 */
export type JsConfig = {
  /**
   * Ceiling of the adaptive poll backoff when the client is idle (1 → 2 → 4 … ms).
   * Also the worst-case added latency for a message arriving on an idle consumer and
   * for rebalance / offset-commit / delivery-report delivery.
   *
   * @default 50
   */
  "js.poll.idle.max.ms"?: number;
  /**
   * Poll interval while the client is *cold* (no subscription/assignment, nothing in
   * flight): only log/stats/error events are picked up. The timer is `unref`'d.
   *
   * @default 500
   */
  "js.poll.interval.ms"?: number;
  /**
   * Upper bound (bytes) of the buffer one `brk_consume_batch` FFI call fills. Each
   * batch gets a fresh buffer sized to the traffic (up to this value) and message
   * key/value are views into it — no per-message copy — so a retained message
   * keeps at most one such buffer alive. Grows automatically when a single message
   * does not fit: a performance knob, not a limit. Values above ~512 KiB are slower
   * on most allocators (fresh pages per batch).
   *
   * @default 262144
   */
  "js.consume.buffer.bytes"?: number;
  /**
   * Size (bytes) of the reusable buffer for the event drain (delivery reports,
   * rebalance/commit events, stats JSON). Grows automatically.
   *
   * @default 262144
   */
  "js.event.buffer.bytes"?: number;
  /**
   * Producer backpressure threshold: messages still waiting for a delivery report.
   * Beyond it `produce()` throws `ERR__QUEUE_FULL` synchronously.
   *
   * @default the value of `queue.buffering.max.messages` (librdkafka default 100000)
   */
  "js.producer.max.pending"?: number;
  /**
   * KafkaJS API only: maximum messages handed to one `eachBatch` call.
   *
   * @default 32
   */
  "js.consumer.max.batch.size"?: number;
  /**
   * EXPERIMENTAL — serialize consume batches on a shim-owned thread so the JS thread
   * only decodes/emits (+26–33 % consumer throughput at +33–40 % CPU on ≥ 2 cores).
   * Prefetched frames are still delivered after seek/pause/revoke; see
   * docs/notes/consumer-prefetch-thread.md.
   *
   * @default false
   */
  "js.consume.prefetch"?: boolean;
  /**
   * Ring depth of the prefetch thread (frames of `js.consume.buffer.bytes`).
   *
   * @default 4
   */
  "js.consume.prefetch.frames"?: number;
  /** Reserved (zero-copy message views) — not yet effective. */
  "js.consumer.zero.copy"?: boolean;
  /** Reserved (Worker-based blocking poll) — not yet effective. */
  "js.poll.worker"?: boolean;
};

export class ConfigError extends Error {
  override readonly name = "ConfigError";
}

/* ========================================================================== */
/* Option js.*                                                                 */
/* ========================================================================== */

/** JS-layer options, normalized. */
export interface JsOptions {
  /** `js.poll.idle.max.ms` — the PollScheduler's backoff ceiling. */
  pollIdleMaxMs: number;
  /** `js.poll.interval.ms` — poll interval while COLD. */
  pollIntervalMs: number;
  /** `js.consume.buffer.bytes` — per-batch buffer cap for `brk_consume_batch`. */
  consumeBufferBytes: number;
  /** `js.event.buffer.bytes` — reusable buffer for `brk_events_poll`. */
  eventBufferBytes: number;
  /** `js.producer.max.pending` — the DeliveryLedger's backpressure threshold. */
  producerMaxPending: number;
  /** `js.consumer.max.batch.size` — max messages per `eachBatch`. */
  consumerMaxBatchSize: number;
  /** `js.consumer.zero.copy` — return Buffers as views (TODO M6). */
  consumerZeroCopy: boolean;
  /** `js.poll.worker` — blocking poll on a Worker (later phase, design §5.2). */
  pollWorker: boolean;
  /**
   * `js.consume.prefetch` — EXPERIMENT: serialize consume batches on a
   * shim-owned thread (docs/notes/consumer-prefetch-thread.md).
   */
  consumePrefetch: boolean;
  /** `js.consume.prefetch.frames` — ring depth for the prefetch thread. */
  consumePrefetchFrames: number;
}

type JsOptionType = "number" | "boolean";

interface JsOptionSpec {
  field: keyof JsOptions;
  type: JsOptionType;
  /** Default value; `producerMaxPending` is additionally derived from the librdkafka conf. */
  fallback: number | boolean;
}

/** Recognized `js.*` options. Unknown keys starting with `js.` are rejected. */
export const JS_OPTION_SPECS: Readonly<Record<string, JsOptionSpec>> = Object.freeze({
  "js.poll.idle.max.ms": { field: "pollIdleMaxMs", type: "number", fallback: 50 },
  "js.poll.interval.ms": { field: "pollIntervalMs", type: "number", fallback: 500 },
  "js.consume.buffer.bytes": {
    field: "consumeBufferBytes",
    type: "number",
    fallback: 4 * 1024 * 1024,
  },
  "js.event.buffer.bytes": {
    field: "eventBufferBytes",
    type: "number",
    fallback: 256 * 1024,
  },
  "js.producer.max.pending": {
    field: "producerMaxPending",
    type: "number",
    fallback: 100_000,
  },
  // TODO(M5): cross-check the default against confluent-kafka-javascript for the KafkaJS layer.
  "js.consumer.max.batch.size": {
    field: "consumerMaxBatchSize",
    type: "number",
    fallback: 32,
  },
  "js.consumer.zero.copy": { field: "consumerZeroCopy", type: "boolean", fallback: false },
  "js.poll.worker": { field: "pollWorker", type: "boolean", fallback: false },
  "js.consume.prefetch": { field: "consumePrefetch", type: "boolean", fallback: false },
  "js.consume.prefetch.frames": { field: "consumePrefetchFrames", type: "number", fallback: 4 },
});

export const DEFAULT_JS_OPTIONS: Readonly<JsOptions> = Object.freeze({
  pollIdleMaxMs: 50,
  pollIntervalMs: 500,
  consumeBufferBytes: 256 * 1024,
  eventBufferBytes: 256 * 1024,
  producerMaxPending: 100_000,
  consumerMaxBatchSize: 32,
  consumerZeroCopy: false,
  pollWorker: false,
  consumePrefetch: false,
  consumePrefetchFrames: 4,
});

/* ========================================================================== */
/* Callback property                                                           */
/* ========================================================================== */

/**
 * Properties accepting a function (or `true` to merely enable the matching
 * event, exactly how node-rdkafka treats `rebalance_cb`/`offset_commit_cb`).
 */
export const CALLBACK_KEYS = [
  "rebalance_cb",
  "offset_commit_cb",
  "oauthbearer_token_refresh_cb",
  "dr_cb",
  "dr_msg_cb",
  "partitioner_cb",
  "event_cb",
] as const;

export type CallbackKey = (typeof CALLBACK_KEYS)[number];

/** Keys accepting a boolean value (enable the event without a handler). */
const BOOLEAN_CALLBACK_KEYS = new Set<string>([
  "rebalance_cb",
  "offset_commit_cb",
  "dr_cb",
  "event_cb",
]);

export type ConfigCallbacks = Partial<Record<CallbackKey, AnyCallback | true>>;

/* ========================================================================== */
/* Result                                                                      */
/* ========================================================================== */

export interface BuiltConfig {
  /** Stringified (name, value) pairs, in declaration order. */
  properties: [string, string][];
  js: JsOptions;
  callbacks: ConfigCallbacks;
}

/** Stringifies a config value the way librdkafka expects. */
export function stringifyConfigValue(key: string, value: unknown): string {
  switch (typeof value) {
    case "string":
      return value;
    case "boolean":
      return value ? "true" : "false";
    case "bigint":
      return value.toString();
    case "number": {
      if (!Number.isFinite(value)) {
        throw new ConfigError(`config "${key}": non-finite number (${value})`);
      }
      // Avoid exponent notation ("1e+21"), which librdkafka cannot parse.
      return Number.isInteger(value) ? BigInt(value).toString() : String(value);
    }
    default:
      if (Array.isArray(value)) {
        return value.map((item) => stringifyConfigValue(key, item)).join(",");
      }
      throw new ConfigError(
        `config "${key}": type ${typeof value} cannot be converted into a librdkafka property`,
      );
  }
}

/**
 * Config collector. `add()` may be called multiple times (global conf + topic
 * conf); a same-named property from a later call overrides the earlier one.
 */
export class ConfigBuilder {
  private readonly props = new Map<string, string>();
  private readonly jsRaw = new Map<string, number | boolean>();
  private readonly callbacks: ConfigCallbacks = {};

  constructor(config?: ClientConfig, topicConfig?: ClientConfig) {
    if (config) this.add(config);
    if (topicConfig) this.add(topicConfig);
  }

  /** Loads one more config object. */
  add(config: ClientConfig): this {
    for (const [key, value] of Object.entries(config)) {
      if (value === undefined || value === null) continue;

      if (key.startsWith("js.")) {
        this.addJsOption(key, value);
        continue;
      }

      if (typeof value === "function") {
        this.addCallback(key, value as AnyCallback);
        continue;
      }

      if (typeof value === "boolean" && BOOLEAN_CALLBACK_KEYS.has(key)) {
        if (value) this.callbacks[key as CallbackKey] = true;
        continue;
      }

      if (isCallbackKey(key)) {
        throw new ConfigError(
          `config "${key}" must be a function${
            BOOLEAN_CALLBACK_KEYS.has(key) ? " or a boolean" : ""
          }, got ${typeof value}`,
        );
      }

      this.props.set(key, stringifyConfigValue(key, value));
    }
    return this;
  }

  /** Sets a librdkafka property directly. */
  set(key: string, value: unknown): this {
    this.props.set(key, stringifyConfigValue(key, value));
    return this;
  }

  private addJsOption(key: string, value: unknown): void {
    const spec = JS_OPTION_SPECS[key];
    if (!spec) {
      throw new ConfigError(
        `option "${key}" is not supported. Valid js.* options: ` +
          `${Object.keys(JS_OPTION_SPECS).join(", ")}`,
      );
    }
    if (spec.type === "number") {
      if (typeof value !== "number" || !Number.isFinite(value)) {
        throw new ConfigError(`option "${key}" must be a finite number, got ${String(value)}`);
      }
      if (value < 0) throw new ConfigError(`option "${key}" must not be negative (${value})`);
      this.jsRaw.set(key, value);
      return;
    }
    if (typeof value !== "boolean") {
      throw new ConfigError(`option "${key}" must be a boolean, got ${typeof value}`);
    }
    this.jsRaw.set(key, value);
  }

  private addCallback(key: string, fn: AnyCallback): void {
    if (!isCallbackKey(key)) {
      throw new ConfigError(
        `config "${key}" is a function but not a supported callback. ` +
          `Valid: ${CALLBACK_KEYS.join(", ")}`,
      );
    }
    this.callbacks[key] = fn;
  }

  build(): BuiltConfig {
    const js: JsOptions = { ...DEFAULT_JS_OPTIONS };

    // The js.producer.max.pending default follows queue.buffering.max.messages.
    const bufferingMax = this.props.get("queue.buffering.max.messages");
    if (bufferingMax !== undefined) {
      const parsed = Number.parseInt(bufferingMax, 10);
      if (Number.isFinite(parsed) && parsed > 0) js.producerMaxPending = parsed;
    }

    for (const [key, value] of this.jsRaw) {
      const spec = JS_OPTION_SPECS[key];
      if (!spec) continue;
      assignJsOption(js, spec.field, value);
    }

    return {
      properties: [...this.props.entries()],
      js,
      callbacks: { ...this.callbacks },
    };
  }
}

function assignJsOption(js: JsOptions, field: keyof JsOptions, value: number | boolean): void {
  switch (field) {
    case "consumerZeroCopy":
    case "pollWorker":
    case "consumePrefetch":
      js[field] = value as boolean;
      return;
    default:
      js[field] = value as number;
  }
}

function isCallbackKey(key: string): key is CallbackKey {
  return (CALLBACK_KEYS as readonly string[]).includes(key);
}

/** One-shot convenience: `buildConfig(conf, topicConf)`. */
export function buildConfig(config?: ClientConfig, topicConfig?: ClientConfig): BuiltConfig {
  return new ConfigBuilder(config, topicConfig).build();
}
