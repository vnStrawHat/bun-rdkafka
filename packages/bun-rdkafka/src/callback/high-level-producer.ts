/**
 * callback/high-level-producer.ts — `HighLevelProducer` (FR-1).
 *
 * Semantics cross-checked against node-rdkafka
 * (`lib/producer/high-level-producer.js`):
 * `produce(topic, partition, message, key, timestamp, [headers,] callback)`
 * with a per-message `(err, offset)` delivery callback, plus
 * `setKeySerializer`/`setValueSerializer` (sync, Promise, or callback-style
 * `fn(value, cb)` when the function declares 2 parameters).
 *
 * Per-message correlation reuses the {@link DeliveryLedger} directly through
 * `Producer.produceInternal` (opaque_id) — no separate mechanism needed.
 */

import type { ClientConfig } from "../core/config.ts";
import { ERROR_CODES, LibrdKafkaError } from "../core/errors.ts";
import type { ClientInternalOptions } from "./client.ts";
import {
  Producer,
  type DeliveryReportPayload,
  type ProduceHeaders,
  type ProduceValue,
} from "./producer.ts";

/** HighLevelProducer's per-message callback — upstream `(err, offset)` shape. */
export type HlpDeliveryCallback = (err: LibrdKafkaError | null, offset?: number) => void;

/**
 * Upstream's serializer: sync (returns the value), Promise, or callback-style
 * when the function takes 2 parameters (`fn(value, cb)`).
 */
export type Serializer = (
  value: unknown,
  cb?: (err: Error | null, result?: ProduceValue) => void,
) => unknown;

const identitySerializer: Serializer = (value) => value as ProduceValue;

/** Runs a serializer in any of the three styles upstream supports. */
function runSerializer(
  fn: Serializer,
  value: unknown,
  done: (err: Error | null, result?: ProduceValue) => void,
): void {
  if (fn.length === 2) {
    // Callback-style.
    try {
      fn(value, (err, result) => done(err, result));
    } catch (error) {
      done(error instanceof Error ? error : new Error(String(error)));
    }
    return;
  }
  let out: unknown;
  try {
    out = fn(value);
  } catch (error) {
    done(error instanceof Error ? error : new Error(String(error)));
    return;
  }
  if (out instanceof Promise) {
    out.then(
      (result) => done(null, result as ProduceValue),
      (error: unknown) => done(error instanceof Error ? error : new Error(String(error))),
    );
    return;
  }
  done(null, out as ProduceValue);
}

export class HighLevelProducer extends Producer {
  private keySerializer: Serializer = identitySerializer;
  private valueSerializer: Serializer = identitySerializer;

  constructor(
    globalConf?: ClientConfig,
    topicConf?: ClientConfig,
    internal: ClientInternalOptions = {},
  ) {
    super(globalConf, topicConf, internal);
  }

  setKeySerializer(serializer: Serializer): this {
    this.keySerializer = serializer;
    return this;
  }

  setValueSerializer(serializer: Serializer): this {
    this.valueSerializer = serializer;
    return this;
  }

  /**
   * `produce(topic, partition, message, key, timestamp, callback)` or
   * `produce(topic, partition, message, key, timestamp, headers, callback)`.
   *
   * Unlike `Producer.produce`: asynchronous with respect to errors —
   * serialize/produce errors go to the callback instead of throwing (matching
   * upstream).
   */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  override produce(...args: any[]): boolean {
    const [topic, partition, message, key] = args as [
      string,
      number | null | undefined,
      unknown,
      unknown,
    ];
    let timestamp: number | null | undefined;
    let headers: ProduceHeaders | undefined;
    let cb: HlpDeliveryCallback | undefined;

    // args[4..]: (timestamp, cb) | (timestamp, headers, cb) | (cb)
    const rest = args.slice(4) as unknown[];
    for (const arg of rest) {
      if (typeof arg === "function") cb = arg as HlpDeliveryCallback;
      else if (Array.isArray(arg)) headers = arg as ProduceHeaders;
      else if (typeof arg === "number") timestamp = arg;
      else if (arg === null || arg === undefined) continue;
      else {
        throw new LibrdKafkaError(
          `bun-rdkafka: HighLevelProducer.produce() does not understand an argument of type ${typeof arg}`,
          { code: ERROR_CODES.ERR__INVALID_ARG, origin: "local", context: "produce" },
        );
      }
    }
    if (cb === undefined) {
      throw new LibrdKafkaError(
        "bun-rdkafka: HighLevelProducer.produce() requires a delivery callback",
        { code: ERROR_CODES.ERR__INVALID_ARG, origin: "local", context: "produce" },
      );
    }
    const callback = cb;

    runSerializer(this.valueSerializer, message, (valueErr, serializedValue) => {
      if (valueErr) {
        callback(
          LibrdKafkaError.fromKafkaCode(ERROR_CODES.ERR__VALUE_SERIALIZATION, valueErr.message, {
            context: "produce",
          }),
        );
        return;
      }
      runSerializer(this.keySerializer, key, (keyErr, serializedKey) => {
        if (keyErr) {
          callback(
            LibrdKafkaError.fromKafkaCode(ERROR_CODES.ERR__KEY_SERIALIZATION, keyErr.message, {
              context: "produce",
            }),
          );
          return;
        }
        try {
          this.produceInternal(
            topic,
            partition,
            serializedValue ?? null,
            serializedKey ?? null,
            timestamp,
            undefined,
            headers,
            (err: LibrdKafkaError | null, report?: DeliveryReportPayload) =>
              err ? callback(err) : callback(null, report?.offset),
          );
        } catch (error) {
          callback(
            error instanceof LibrdKafkaError
              ? error
              : new LibrdKafkaError(String(error), { context: "produce" }),
          );
        }
      });
    });
    return true;
  }
}
