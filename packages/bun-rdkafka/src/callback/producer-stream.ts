/**
 * callback/producer-stream.ts — `ProducerStream` (Stream API, FR-1).
 *
 * A `node:stream` Writable that produces every chunk through a
 * {@link Producer}. API-compatible with confluent-kafka-javascript's
 * `lib/producer-stream.js` (options, events, `close()`, `producer` field), but
 * built on the modern Writable lifecycle instead of manual `close` events:
 *
 *  - `autoClose` (default `true`) maps to Writable's `autoDestroy`: `finish`
 *    (or an error) destroys the stream, and `_destroy()` flushes + disconnects
 *    the producer before `close` is emitted. With `autoClose: false` the
 *    producer stays untouched until `close()` is called explicitly.
 *  - `ERR__QUEUE_FULL` from `produce()` is retried with a bounded exponential
 *    backoff (5 ms → 500 ms) after a `poll()`, instead of upstream's fixed
 *    500 ms — same backpressure, less idle time.
 *  - Non-objectMode chunks must be `Buffer`/`Uint8Array` (strings are decoded
 *    by Writable via `defaultEncoding`); anything else fails the write.
 */

import { Writable, type WritableOptions } from "node:stream";

import { ERROR_CODES } from "../core/errors.ts";
import type { MetadataOptions } from "./client.ts";
import type { ProduceHeaders, ProduceValue, Producer } from "./producer.ts";

/* ========================================================================== */
/* Types                                                                       */
/* ========================================================================== */

/** Options of {@link ProducerStream} (mirrors upstream `WriteStreamOptions`). */
export interface WriteStreamOptions extends WritableOptions {
  /** Default encoding for string chunks (non-objectMode). */
  encoding?: BufferEncoding;
  /** `true` → chunks are {@link ProducerStreamMessage} objects; `false` → buffers. */
  objectMode?: boolean;
  /** Target topic — required unless `objectMode`. */
  topic?: string;
  /** Disconnect the producer once the stream finishes/errors (default `true`). */
  autoClose?: boolean;
  /** Forwarded to `producer.setPollInterval()` (default 1000 ms). */
  pollInterval?: number;
  /** Forwarded to `producer.connect()` when the stream has to connect. */
  connectOptions?: MetadataOptions;
}

/** A chunk written to an objectMode {@link ProducerStream}. */
export interface ProducerStreamMessage {
  topic: string;
  partition?: number | null;
  value?: ProduceValue;
  key?: ProduceValue;
  timestamp?: number | null;
  opaque?: unknown;
  headers?: ProduceHeaders;
}

type WriteCallback = (error?: Error | null) => void;

/** Backoff bounds for `ERR__QUEUE_FULL` retries. */
const QUEUE_FULL_BACKOFF_MIN_MS = 5;
const QUEUE_FULL_BACKOFF_MAX_MS = 500;
/** Upper bound for the flush performed before disconnecting on close. */
const CLOSE_FLUSH_TIMEOUT_MS = 10_000;

/* ========================================================================== */
/* ProducerStream                                                              */
/* ========================================================================== */

export class ProducerStream extends Writable {
  /** The producer being written to — usable directly (e.g. `flush()`). */
  readonly producer: Producer;
  /** Topic used in non-objectMode (undefined in objectMode). */
  readonly topicName: string | undefined;
  readonly autoClose: boolean;
  readonly connectOptions: MetadataOptions;

  readonly #objectMode: boolean;
  /** Set while the stream itself is driving `producer.connect()`. */
  #connecting = false;
  /** Set once `close()` was requested — `_destroy` then always disconnects. */
  #closeRequested = false;
  #backoffMs = QUEUE_FULL_BACKOFF_MIN_MS;
  #closed = false;

  constructor(producer: Producer, options?: WriteStreamOptions | string) {
    let opts: WriteStreamOptions;
    if (options === undefined) opts = {};
    else if (typeof options === "string") opts = { encoding: options as BufferEncoding };
    else if (options === null || typeof options !== "object") {
      throw new TypeError('"streamOptions" argument must be a string or an object');
    } else opts = options;

    const objectMode = opts.objectMode === true;
    if (!objectMode && !opts.topic) {
      throw new TypeError("ProducerStreams not using objectMode must provide a topic to produce to.");
    }
    const autoClose = opts.autoClose === undefined ? true : Boolean(opts.autoClose);

    const {
      encoding: _encoding,
      topic: _topic,
      autoClose: _autoClose,
      pollInterval: _pollInterval,
      connectOptions: _connectOptions,
      ...writableOptions
    } = opts;
    super({
      ...writableOptions,
      objectMode,
      // Upstream: `finish` + autoClose → close(). Without autoClose the stream
      // stays open (and the producer connected) until close() is called.
      autoDestroy: writableOptions.autoDestroy ?? autoClose,
    });

    this.producer = producer;
    this.topicName = opts.topic;
    this.autoClose = autoClose;
    this.connectOptions = opts.connectOptions ?? {};
    this.#objectMode = objectMode;
    this.once("close", () => {
      this.#closed = true;
    });

    this.producer.setPollInterval(opts.pollInterval || 1000);
    if (opts.encoding) this.setDefaultEncoding(opts.encoding);

    if (!this.producer.isConnected()) this.connect(this.connectOptions);
  }

  /* ------------------------------------------------------------- connect */

  /** Connects the underlying producer; a connection failure destroys the stream. */
  connect(metadataOptions?: MetadataOptions): void {
    this.#connecting = true;
    this.producer.connect(metadataOptions ?? {}, (err) => {
      this.#connecting = false;
      if (!err) return;
      // ERR__STATE: the producer was already connecting on the user's side —
      // its own `ready` will release the queued writes.
      if (err.code === ERROR_CODES.ERR__STATE) return;
      this.destroy(err);
    });
  }

  /* -------------------------------------------------------------- writes */

  override _write(chunk: unknown, encoding: BufferEncoding, cb: WriteCallback): void {
    if (this.#objectMode) this.#writeMessage(chunk as ProducerStreamMessage, cb);
    else this.#writeBuffer(chunk, encoding, cb);
  }

  override _writev(
    chunks: Array<{ chunk: unknown; encoding: BufferEncoding }>,
    cb: WriteCallback,
  ): void {
    if (!this.producer.isConnected()) {
      this.#whenReady(() => this._writev(chunks, cb), cb);
      return;
    }
    this.#produceAll(chunks.map((c) => c.chunk), cb);
  }

  #writeBuffer(chunk: unknown, encoding: BufferEncoding, cb: WriteCallback): void {
    if (!(chunk instanceof Uint8Array)) {
      cb(new TypeError("Invalid data. Can only produce buffers"));
      return;
    }
    if (!this.producer.isConnected()) {
      this.#whenReady(() => this._write(chunk, encoding, cb), cb);
      return;
    }
    this.#produceAll([chunk], cb);
  }

  #writeMessage(message: ProducerStreamMessage, cb: WriteCallback): void {
    if (!this.producer.isConnected()) {
      this.#whenReady(() => this.#writeMessage(message, cb), cb);
      return;
    }
    this.#produceAll([message], cb);
  }

  /**
   * Runs `fn` on the producer's next `ready` (writes queued before connect).
   * If the stream was destroyed meanwhile, the write fails instead of producing.
   */
  #whenReady(fn: () => void, cb: WriteCallback): void {
    this.producer.once("ready", () => {
      if (this.destroyed) cb(new Error("ProducerStream destroyed before the producer was ready"));
      else fn();
    });
  }

  /**
   * Produces `chunks` in order. `ERR__QUEUE_FULL` → poll, then retry the rest
   * after a backoff; any other error → `cb(err)`.
   */
  #produceAll(chunks: unknown[], cb: WriteCallback): void {
    for (let i = 0; i < chunks.length; i++) {
      const chunk = chunks[i];
      try {
        if (chunk instanceof Uint8Array) {
          this.producer.produce(this.topicName as string, null, chunk, null);
        } else {
          const m = chunk as ProducerStreamMessage;
          this.producer.produce(
            m.topic,
            m.partition,
            m.value,
            m.key,
            m.timestamp,
            m.opaque,
            m.headers,
          );
        }
      } catch (error) {
        const err = error as Error & { code?: number };
        if (err.code === ERROR_CODES.ERR__QUEUE_FULL) {
          this.#retryAfterQueueFull(chunks.slice(i), cb);
        } else {
          setImmediate(() => cb(err));
        }
        return;
      }
    }
    this.#backoffMs = QUEUE_FULL_BACKOFF_MIN_MS;
    setImmediate(cb);
  }

  #retryAfterQueueFull(rest: unknown[], cb: WriteCallback): void {
    // Poll for good measure: pushes staged records to librdkafka and collects
    // delivery reports, freeing ledger slots.
    this.producer.poll();
    const delay = this.#backoffMs;
    this.#backoffMs = Math.min(this.#backoffMs * 2, QUEUE_FULL_BACKOFF_MAX_MS);
    setTimeout(() => {
      if (this.destroyed) {
        cb(new Error("ProducerStream destroyed while waiting for the producer queue"));
        return;
      }
      this.#produceAll(rest, cb);
    }, delay);
  }

  /* --------------------------------------------------------------- close */

  /**
   * Closes the stream: flushes + disconnects the producer (if it is connected
   * or connecting), then emits `close`. Idempotent.
   */
  close(cb?: () => void): void {
    this.#closeRequested = true;
    if (!this.destroyed) {
      if (cb) this.once("close", cb);
      this.destroy();
      return;
    }
    // Already destroyed (e.g. autoClose: false + error): the producer may still
    // be connected — release it once the destroy cycle is over.
    const finish = (): void => this.#teardown(() => cb?.());
    if (this.#closed) finish();
    else this.once("close", finish);
  }

  override _destroy(err: Error | null, cb: (error?: Error | null) => void): void {
    if (this.autoClose || this.#closeRequested) this.#teardown(() => cb(err));
    else cb(err);
  }

  /** Flushes + disconnects the producer if the stream is responsible for it. */
  #teardown(done: () => void): void {
    if (!this.producer.isConnected() && !this.#connecting) {
      setImmediate(done);
      return;
    }
    if (!this.producer.isConnected()) {
      // Still connecting: disconnect() is safe mid-connect (Client cancels).
      this.producer.disconnect(() => done());
      return;
    }
    // Do not drop what was written: wait for delivery before destroying the
    // handle (upstream's Producer.disconnect() flushes for the same reason).
    this.producer.flush(CLOSE_FLUSH_TIMEOUT_MS, () => {
      this.producer.disconnect(() => done());
    });
  }
}
