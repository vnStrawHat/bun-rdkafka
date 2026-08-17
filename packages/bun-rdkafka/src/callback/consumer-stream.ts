/**
 * callback/consumer-stream.ts — `ConsumerStream` (Stream API, FR-1).
 *
 * A `node:stream` Readable fed by a {@link KafkaConsumer} in non-flowing mode
 * (`consume(n, cb)`), so Readable's highWaterMark provides real backpressure:
 * the consumer is only asked for more messages when the stream's buffer has
 * room. API-compatible with confluent-kafka-javascript's
 * `lib/kafka-consumer-stream.js` (`KafkaConsumerStream`): same options,
 * events, `close()`, `consumer` field.
 *
 * Differences from upstream (all internal):
 *  - Built on the modern Readable lifecycle: `autoClose` (default `true`) maps
 *    to `autoDestroy`, and `_destroy()` unsubscribes + disconnects the consumer
 *    before `close` is emitted — so `stream.destroy()`, `pipeline()` and
 *    `for await` all release the consumer.
 *  - Non-objectMode skips tombstones (`value === null`) instead of pushing
 *    `null`, which would end the stream.
 *  - RegExp topics are converted to librdkafka's `^...` pattern strings.
 */

import { Buffer } from "node:buffer";
import { Readable, type ReadableOptions } from "node:stream";

import type { Metadata, MetadataOptions } from "./client.ts";
import type { KafkaConsumer, Message } from "./kafka-consumer.ts";

/* ========================================================================== */
/* Types                                                                       */
/* ========================================================================== */

export type StreamSubscribeTopic = string | RegExp;
export type StreamTopics =
  | StreamSubscribeTopic
  | StreamSubscribeTopic[]
  | ((metadata: Metadata | undefined) => StreamSubscribeTopic[] | StreamSubscribeTopic);

/** Options of {@link ConsumerStream} (mirrors upstream `ReadStreamOptions`). */
export interface ReadStreamOptions extends ReadableOptions {
  /**
   * Topics to subscribe to; a function receives the connect metadata. Omitted →
   * the consumer's existing subscription/assignment is read.
   */
  topics?: StreamTopics;
  /** Max delay (ms, randomized) before retrying when no messages are available (default 1000). */
  waitInterval?: number;
  /** Max messages requested per `consume()` (default 1). */
  fetchSize?: number;
  /** `true` (default) → pushes `Message` objects; `false` → pushes `message.value` buffers. */
  objectMode?: boolean;
  highWaterMark?: number;
  /** Unsubscribe + disconnect the consumer when the stream ends (default `true`). */
  autoClose?: boolean;
  /** Push each fetched batch as one array instead of message by message. */
  streamAsBatch?: boolean;
  /** Forwarded to `consumer.connect()` when the stream has to connect. */
  connectOptions?: MetadataOptions;
}

const DEFAULT_WAIT_INTERVAL_MS = 1000;
const EMPTY = Buffer.alloc(0);

/* ========================================================================== */
/* ConsumerStream                                                              */
/* ========================================================================== */

export class ConsumerStream extends Readable {
  /** The consumer being read from — usable directly (e.g. `commit()`). */
  readonly consumer: KafkaConsumer;
  readonly topics: StreamTopics | undefined;
  readonly autoClose: boolean;
  readonly waitInterval: number;
  readonly fetchSize: number;
  readonly connectOptions: MetadataOptions;
  readonly streamAsBatch: boolean;
  /** Fetched messages (or values) not yet pushed — one is pushed per `_read`. */
  readonly messages: unknown[] = [];

  readonly #objectMode: boolean;
  /** Set while the stream itself is driving `consumer.connect()`. */
  #connecting = false;
  /** Set once the stream is shutting down: ignore late `unsubscribed`/consume results. */
  #closing = false;
  /** A `consume()` request is in flight — never overlap them. */
  #reading = false;
  #retryTimer: ReturnType<typeof setTimeout> | undefined;
  /** Set once the stream has subscribed — reads before that are deferred. */
  #subscribed = false;
  #pendingReadSize: number | undefined;

  constructor(consumer: KafkaConsumer, options?: ReadStreamOptions | number) {
    let opts: ReadStreamOptions;
    if (options === undefined) opts = { waitInterval: DEFAULT_WAIT_INTERVAL_MS } as ReadStreamOptions;
    else if (typeof options === "number") opts = { waitInterval: options } as ReadStreamOptions;
    else if (options === null || typeof options !== "object") {
      throw new TypeError('"options" argument must be a number or an object');
    } else opts = options;

    // `topics` may be omitted (e.g. `createReadStream(conf, topicConf, 500)`):
    // the stream then reads whatever the consumer is already subscribed to /
    // assigned — upstream throws here, which makes its number overload unusable.
    const topics = opts.topics;
    if (topics !== undefined && typeof topics !== "function" && !Array.isArray(topics)) {
      if (typeof topics !== "string" && !(topics instanceof RegExp)) {
        throw new TypeError('"topics" argument must be a string, regex, or an array');
      }
    }

    const fetchSize = opts.fetchSize || 1;
    const objectMode = opts.objectMode === null || opts.objectMode === undefined
      ? true
      : opts.objectMode === true;
    const autoClose = opts.autoClose === undefined ? true : Boolean(opts.autoClose);
    const {
      topics: _topics,
      waitInterval: _waitInterval,
      fetchSize: _fetchSize,
      autoClose: _autoClose,
      streamAsBatch: _streamAsBatch,
      connectOptions: _connectOptions,
      ...readableOptions
    } = opts;
    // In objectMode without an explicit highWaterMark, leave room for one
    // standard fetch (upstream: fetchSize + 2).
    const highWaterMark =
      objectMode && !readableOptions.highWaterMark ? fetchSize + 2 : readableOptions.highWaterMark;
    super({
      ...readableOptions,
      objectMode,
      ...(highWaterMark !== undefined ? { highWaterMark } : {}),
      autoDestroy: readableOptions.autoDestroy ?? autoClose,
    });

    this.consumer = consumer;
    this.topics = topics;
    this.autoClose = autoClose;
    this.waitInterval = opts.waitInterval === undefined ? DEFAULT_WAIT_INTERVAL_MS : opts.waitInterval;
    this.fetchSize = fetchSize;
    this.connectOptions = opts.connectOptions ?? {};
    this.streamAsBatch = Boolean(opts.streamAsBatch);
    this.#objectMode = objectMode;

    // Unsubscribing invalidates the stream (upstream behavior).
    this.consumer.on("unsubscribed", this.#onUnsubscribed);

    this.connect(this.connectOptions);
  }

  /* ------------------------------------------------------------- connect */

  /**
   * Connects the consumer if needed, subscribes (a `topics` function receives
   * the metadata), then starts the flow of data.
   */
  connect(options?: MetadataOptions): void {
    const onConnected = (err: Error | null, metadata?: Metadata): void => {
      this.#connecting = false;
      if (this.#closing || this.destroyed) return;
      if (err) {
        this.destroy(err);
        return;
      }
      try {
        const topics = typeof this.topics === "function" ? this.topics(metadata) : this.topics;
        if (topics !== undefined) this.consumer.subscribe(normalizeTopics(topics));
      } catch (error) {
        this.destroy(error as Error);
        return;
      }
      this.#subscribed = true;
      // Start the flow of data: resume the read Readable asked for before we
      // were subscribed (Readable will not call _read again until we push),
      // or ask Readable to start one.
      const pending = this.#pendingReadSize;
      this.#pendingReadSize = undefined;
      if (pending !== undefined) this._read(pending);
      else this.read(0);
    };

    if (this.consumer.isConnected()) {
      if (typeof this.topics === "function") {
        // Already connected: a topics function still needs the metadata.
        this.consumer.getMetadata(options ?? {}, onConnected);
      } else {
        setImmediate(() => onConnected(null, undefined));
      }
      return;
    }
    this.#connecting = true;
    this.consumer.connect(options ?? {}, onConnected);
  }

  /* ---------------------------------------------------------------- read */

  override _read(size: number): void {
    if (this.messages.length > 0) {
      this.push(this.messages.shift());
      return;
    }
    if (this.#closing || this.destroyed || this.#reading) return;

    if (!this.#subscribed) {
      // Not connected/subscribed yet: connect() resumes this read afterwards.
      this.#pendingReadSize = size;
      return;
    }

    // Use the smaller of the advised size and fetchSize.
    const n = size >= this.fetchSize ? this.fetchSize : Math.max(1, size);
    this.#reading = true;
    this.consumer.consume(n, (err, messages) => {
      this.#reading = false;
      if (this.#closing || this.destroyed) return;

      // Errors are emitted but do not stop consumption (upstream behavior):
      // without an `error` listener the stream blows up, with one it moves on.
      if (err) this.emit("error", err);

      if (err || !messages || messages.length < 1) {
        this.#retry(size);
        return;
      }
      if (this.streamAsBatch) {
        // objectMode: the whole batch as one array. Byte mode cannot carry
        // arrays, so the values are concatenated into one chunk.
        if (this.#objectMode) this.push(messages);
        else this.push(Buffer.concat(messages.map((m) => m.value ?? EMPTY)));
        return;
      }
      if (this.#objectMode) {
        for (const m of messages) this.messages.push(m);
      } else {
        for (const m of messages) if (m.value !== null) this.messages.push(m.value);
      }
      if (this.messages.length > 0) this.push(this.messages.shift());
      else this.#retry(size);
    });
  }

  /** Waits up to `waitInterval` (randomized) before reading again; immediately when 0. */
  #retry(size: number): void {
    if (!this.waitInterval) {
      setImmediate(() => this._read(size));
      return;
    }
    this.#retryTimer = setTimeout(() => {
      this.#retryTimer = undefined;
      this._read(size);
    }, this.waitInterval * Math.random());
    this.#retryTimer.unref?.();
  }

  readonly #onUnsubscribed = (): void => {
    if (this.#closing || this.destroyed) return;
    this.push(null);
  };

  /* --------------------------------------------------------------- close */

  /**
   * Closes the stream: unsubscribes + disconnects the consumer, then emits
   * `close`. Idempotent; equivalent to `destroy()`.
   */
  close(cb?: () => void): void {
    if (cb) this.once("close", cb);
    this.destroy();
  }

  override _destroy(err: Error | null, cb: (error?: Error | null) => void): void {
    this.#closing = true;
    if (this.#retryTimer !== undefined) {
      clearTimeout(this.#retryTimer);
      this.#retryTimer = undefined;
    }
    this.consumer.off("unsubscribed", this.#onUnsubscribed);
    this.messages.length = 0;

    if (!this.consumer.isConnected() && !this.#connecting) {
      setImmediate(() => cb(err));
      return;
    }
    if (this.consumer.isConnected()) {
      try {
        this.consumer.unsubscribe();
      } catch {
        // Not subscribed (or already gone) — nothing to undo.
      }
    }
    // disconnect() is safe mid-connect (Client cancels the pending connect).
    this.consumer.disconnect(() => cb(err));
  }
}

/** Upstream class name — `ConsumerStream` is the `.d.ts` interface name. */
export { ConsumerStream as KafkaConsumerStream };

/* ========================================================================== */
/* Helpers                                                                     */
/* ========================================================================== */

/** Normalizes the `topics` option to librdkafka subscription strings. */
export function normalizeTopics(topics: StreamSubscribeTopic[] | StreamSubscribeTopic): string[] {
  const list = Array.isArray(topics) ? topics : [topics];
  return list.map((t) => {
    if (typeof t === "string") return t;
    if (t instanceof RegExp) {
      if (t.flags) throw new TypeError("bun-rdkafka: RegExp topics do not support flags");
      if (t.source.charAt(0) !== "^") {
        throw new TypeError('bun-rdkafka: RegExp topics must start with "^"');
      }
      return t.source;
    }
    throw new TypeError(`bun-rdkafka: invalid topic ${String(t)} — expected a string or RegExp`);
  });
}

/** Message type pushed by an objectMode {@link ConsumerStream}. */
export type { Message as ConsumerStreamMessage };
