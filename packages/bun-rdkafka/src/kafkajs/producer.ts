/**
 * kafkajs/producer.ts — the promisified `KafkaJS.Producer` (FR-2), built on
 * the Callback API `Producer` (../callback/producer).
 *
 * Semantics cross-checked against confluent-kafka-javascript's
 * `lib/kafkajs/_producer.js`:
 *  - `send()` takes NO per-call `acks`/`timeout`/`compression` (throws
 *    ERR__INVALID_ARG — configured at producer creation, upstream's settled
 *    decision).
 *  - The `send()` result is RecordMetadata MERGED PER topic-partition (keeping
 *    the smallest baseOffset), not per-message — matching upstream.
 *  - `transaction()` resolves the producer itself (one transaction at a time);
 *    `commit`/`abort`/`sendOffsets`/`isActive` live on the producer.
 *  - `transactional.id` in the config → `initTransactions` inside `connect()`.
 *  - Promise correlation via `opaque` + the `delivery-report` event (our
 *    internals: the opaque travels through the DeliveryLedger —
 *    api-compat-not-internals).
 */

import type { ClientConfig } from "../core/config.ts";
import { ERROR_CODES, LibrdKafkaError } from "../core/errors.ts";
import {
  Producer as CallbackProducer,
  type DeliveryReportPayload,
  type ProduceHeaders,
  type TopicPartitionOffset,
} from "../callback/producer.ts";
import type { Client } from "../callback/client.ts";
import {
  createBindingMessageMetadata,
  extractStatsCb,
  loggerTrampoline,
  mapCommonConfig,
  mapProducerConfig,
  resolveLogger,
  type CommonRawConfig,
  type Logger,
  type LogMessage,
} from "./config-mapper.ts";
import { KafkaJSError, KafkaJSTimeout, fromLibrdKafkaError, toKafkaJSError } from "./errors.ts";
import { Admin } from "./admin.ts";

/* ========================================================================== */
/* Public types (upstream shapes)                                              */
/* ========================================================================== */

/**
 * Compression codec — configured at `kafkaJS.compression` when creating the
 * producer.
 *
 * `Snappy` is upstream's spelling (`types/kafkajs.d.ts`); `SNAPPY` is kept for
 * code written in the uppercase style of the other keys. Both keys share one
 * value.
 */
export const CompressionTypes = Object.freeze({
  None: "none",
  GZIP: "gzip",
  Snappy: "snappy",
  SNAPPY: "snappy",
  LZ4: "lz4",
  ZSTD: "zstd",
});

/** KafkaJS-style headers: a key → value (or value array) map. */
export interface IHeaders {
  [key: string]: Buffer | string | (Buffer | string)[] | undefined;
}

export interface Message {
  key?: Buffer | string | null;
  value: Buffer | string | null;
  partition?: number | null;
  timestamp?: string | number;
  headers?: IHeaders;
}

export interface ProducerRecord {
  topic: string;
  messages: Message[];
}

export interface TopicMessages {
  topic: string;
  messages: Message[];
}

export interface ProducerBatch {
  topicMessages?: TopicMessages[];
}

export interface RecordMetadata {
  topicName: string;
  partition: number;
  errorCode: number;
  baseOffset?: string;
  logAppendTime?: string;
  logStartOffset?: string;
}

export interface SendOffsetsArg {
  consumer: ConsumerSource;
  topics: { topic: string; partitions: { partition: number; offset: string }[] }[];
  /** NOT supported (upstream: a consumer object must be passed) — presence → throw. */
  consumerGroupId?: string;
}

/** The consumer providing group metadata for `sendOffsets` (M5b's KafkaJS Consumer). */
export interface ConsumerSource {
  _getInternalClient?: () => unknown;
}

const enum ProducerState {
  INIT = 0,
  CONNECTING = 1,
  INITIALIZING_TRANSACTIONS = 2,
  INITIALIZED_TRANSACTIONS = 3,
  CONNECTED = 4,
  DISCONNECTING = 5,
  DISCONNECTED = 6,
}

interface DeliveryOpaque {
  resolve: (metadata: RecordMetadata) => void;
  reject: (err: KafkaJSError) => void;
}

/** KafkaJS headers → the Callback API shape (array of one-key objects). */
export function convertToRdKafkaHeaders(headers: IHeaders | undefined): ProduceHeaders | null {
  if (!headers) return null;
  const out: Record<string, Buffer | string | null>[] = [];
  for (const [key, value] of Object.entries(headers)) {
    if (value !== undefined && Array.isArray(value)) {
      for (const v of value) out.push({ [key]: v });
    } else {
      out.push({ [key]: value ?? null });
    }
  }
  return out;
}

function stateError(message: string): KafkaJSError {
  return new KafkaJSError(message, { code: ERROR_CODES.ERR__STATE });
}

function invalidArg(message: string): KafkaJSError {
  return new KafkaJSError(message, { code: ERROR_CODES.ERR__INVALID_ARG });
}

const PER_SEND_KEY_MESSAGE = (key: string, fn: string): string =>
  `'${key}' is not supported as a property to '${fn}', but must be passed to the producer during creation: ` +
  `kafka.producer({ kafkaJS: { ${key}: <value>, ... } })`;

/* ========================================================================== */
/* Producer                                                                    */
/* ========================================================================== */

export class Producer {
  #userConfig: CommonRawConfig;
  #internalClient: CallbackProducer | null = null;
  #state: ProducerState = ProducerState.INIT;
  #ongoingTransaction = false;
  #logger: Logger;
  #statsCb: ((payload: unknown) => void) | undefined;
  #connectionError: KafkaJSError | null = null;
  #clientName: string | undefined;
  #isTransactional = false;

  /** Not used directly — created via `kafka.producer()`. */
  constructor(rawMergedConfig: CommonRawConfig | undefined) {
    this.#userConfig = rawMergedConfig ?? {};
    // The logger must be ready before connect (as in upstream) — trial-map the
    // common config for the log level; config errors surface fully in connect().
    let logger: Logger;
    try {
      logger = resolveLogger(this.#userConfig, mapCommonConfig(this.#userConfig));
    } catch {
      logger = resolveLogger({}, { globalConf: {}, topicConf: {} });
    }
    this.#logger = logger;
  }

  /** @internal The underlying Callback-API client (undefined before connect). */
  _getInternalClient(): CallbackProducer | null {
    return this.#internalClient;
  }

  /** An admin client riding this producer's connection. */
  dependentAdmin(): Admin {
    return new Admin(null, this as unknown as { _getInternalClient(): Client | null; logger(): Logger });
  }

  #metadata(): object {
    return createBindingMessageMetadata(this.#clientName);
  }

  async connect(): Promise<void> {
    if (this.#state !== ProducerState.INIT) {
      throw stateError("Connect has already been called elsewhere.");
    }
    this.#state = ProducerState.CONNECTING;

    let globalConf: ClientConfig;
    try {
      const mapped = mapProducerConfig(this.#userConfig, mapCommonConfig(this.#userConfig));
      globalConf = mapped.globalConf as ClientConfig;
      this.#logger = resolveLogger(this.#userConfig, mapped);
      this.#statsCb = extractStatsCb(this.#userConfig);
      /* Delivery results flow through await send() — always enable delivery reports. */
      globalConf["dr_cb"] = true;
      this.#isTransactional = Object.hasOwn(globalConf, "transactional.id");
    } catch (e) {
      this.#state = ProducerState.DISCONNECTED;
      throw toKafkaJSError(e);
    }

    const client = new CallbackProducer(globalConf);
    this.#internalClient = client;

    client.on("event.error", (err: LibrdKafkaError) => {
      if (this.#state < ProducerState.CONNECTED && this.#connectionError === null) {
        this.#connectionError = fromLibrdKafkaError(err);
      }
      this.#logger.error(`Error: ${err.message}`, this.#metadata());
    });
    client.on("event.log", (msg: LogMessage) => loggerTrampoline(msg, this.#logger));
    if (this.#statsCb !== undefined) {
      const statsCb = this.#statsCb;
      client.on("event.stats", (payload: unknown) => statsCb(payload));
    }
    client.on("delivery-report", (err, report) => this.#deliveryCallback(err, report));

    await new Promise<void>((resolve, reject) => {
      const fail = (err: LibrdKafkaError | KafkaJSError): void => {
        this.#state = ProducerState.DISCONNECTED;
        reject(
          this.#connectionError ??
            (err instanceof KafkaJSError ? err : fromLibrdKafkaError(err)),
        );
      };
      client.connect({}, (err) => {
        if (err !== null) {
          fail(err);
          return;
        }
        this.#clientName = client.name;
        if (this.#isTransactional) {
          this.#state = ProducerState.INITIALIZING_TRANSACTIONS;
          this.#logger.debug("Attempting to initialize transactions", this.#metadata());
          client.initTransactions(5000, (txnErr) => {
            if (txnErr !== null) {
              fail(txnErr);
              return;
            }
            this.#state = ProducerState.INITIALIZED_TRANSACTIONS;
            this.#finishConnect(resolve);
          });
          return;
        }
        this.#finishConnect(resolve);
      });
    });
  }

  #finishConnect(resolve: () => void): void {
    this.#state = ProducerState.CONNECTED;
    this.#logger.info("Producer connected", this.#metadata());
    resolve();
  }

  async disconnect(): Promise<void> {
    if (this.#state === ProducerState.INIT) return;
    if (this.#state >= ProducerState.DISCONNECTING) return;
    this.#state = ProducerState.DISCONNECTING;
    await new Promise<void>((resolve, reject) => {
      this.#internalClient?.disconnect((err) => {
        if (err !== null) {
          reject(fromLibrdKafkaError(err));
          return;
        }
        this.#state = ProducerState.DISCONNECTED;
        this.#logger.info("Producer disconnected", this.#metadata());
        resolve();
      });
    });
  }

  /* ------------------------------------------------------------------ send */

  #deliveryCallback(err: LibrdKafkaError | null, report: DeliveryReportPayload): void {
    const opaque = report.opaque as DeliveryOpaque | undefined;
    if (
      opaque === undefined ||
      (typeof opaque.resolve !== "function" && typeof opaque.reject !== "function")
    ) {
      throw stateError("Internal error: deliveryCallback called without opaque set properly");
    }
    if (err !== null) {
      opaque.reject(fromLibrdKafkaError(err));
      return;
    }
    opaque.resolve({
      topicName: report.topic,
      partition: report.partition,
      errorCode: 0,
      baseOffset: String(report.offset),
      logAppendTime: "-1",
      logStartOffset: "0",
    });
  }

  async send(sendOptions: ProducerRecord): Promise<RecordMetadata[]> {
    if (this.#state !== ProducerState.CONNECTED) {
      throw stateError("Cannot send without awaiting connect()");
    }
    if (sendOptions === null || typeof sendOptions !== "object") {
      throw invalidArg(
        "The argument passed to send must be an object, and must contain the 'topic' and 'messages' properties: {topic: string, messages: Message[]}",
      );
    }
    for (const key of ["acks", "timeout", "compression"] as const) {
      if (Object.hasOwn(sendOptions, key)) {
        throw invalidArg(PER_SEND_KEY_MESSAGE(key, "send"));
      }
    }

    const client = this.#internalClient;
    if (client === null) throw stateError("Cannot send without awaiting connect()");

    const msgPromises: Promise<RecordMetadata>[] = [];
    for (const msg of sendOptions.messages) {
      const partition = msg.partition ?? -1;
      const value = typeof msg.value === "string" ? Buffer.from(msg.value) : msg.value;
      const timestamp = msg.timestamp ? Number(msg.timestamp) : 0;
      const headers = convertToRdKafkaHeaders(msg.headers);

      msgPromises.push(
        new Promise<RecordMetadata>((resolve, reject) => {
          const opaque: DeliveryOpaque = {
            resolve,
            reject: (e) => reject(e),
          };
          try {
            client.produce(
              sendOptions.topic,
              partition,
              value ?? null,
              msg.key ?? null,
              timestamp,
              opaque,
              headers ?? undefined,
            );
          } catch (e) {
            reject(toKafkaJSError(e));
          }
        }),
      );
    }

    const recordMetadataArr = await Promise.all(msgPromises);

    /* Merge per topic-partition, keeping the smallest baseOffset (matching upstream). */
    const byTp = new Map<string, RecordMetadata>();
    for (const metadata of recordMetadataArr) {
      const key = `${metadata.topicName},${metadata.partition}`;
      const current = byTp.get(key);
      if (metadata.baseOffset === undefined || current === undefined) {
        byTp.set(key, metadata);
        continue;
      }
      if (current.baseOffset === undefined) continue;
      if (Number(current.baseOffset) > Number(metadata.baseOffset)) byTp.set(key, metadata);
    }
    return [...byTp.values()];
  }

  async sendBatch(sendOptions: ProducerBatch): Promise<RecordMetadata[]> {
    if (this.#state !== ProducerState.CONNECTED) {
      throw stateError("Cannot sendBatch without awaiting connect()");
    }
    if (sendOptions === null || typeof sendOptions !== "object") {
      throw invalidArg(
        "The argument passed to sendBatch must be an object, and must contain the 'topicMessages' property: { topicMessages: {topic: string, messages: Message[]}[] }",
      );
    }
    for (const key of ["acks", "timeout", "compression"] as const) {
      if (Object.hasOwn(sendOptions, key)) {
        throw invalidArg(PER_SEND_KEY_MESSAGE(key, "sendBatch"));
      }
    }
    const topicMessages = sendOptions.topicMessages;
    if (topicMessages !== null && topicMessages !== undefined && !Array.isArray(topicMessages)) {
      throw invalidArg("The 'topicMessages' property must be an array.");
    }
    if (!topicMessages || topicMessages.length === 0) return [];
    const records = await Promise.all(topicMessages.map((tm) => this.send(tm)));
    return records.flat();
  }

  /* ---------------------------------------------------------- transactions */

  /**
   * Starts a transaction — only for a producer with a `transactionalId`.
   * Resolves the producer itself (transaction context ≡ producer, matching
   * upstream).
   */
  async transaction(): Promise<this> {
    if (this.#state !== ProducerState.CONNECTED) {
      throw stateError("Cannot start transaction without awaiting connect()");
    }
    if (this.#ongoingTransaction) {
      throw stateError("Can only start one transaction at a time.");
    }
    this.#logger.debug("Attempting to begin transaction", this.#metadata());
    return new Promise((resolve, reject) => {
      this.#internalClient?.beginTransaction((err) => {
        if (err !== null) {
          reject(fromLibrdKafkaError(err));
          return;
        }
        this.#ongoingTransaction = true;
        resolve(this);
      });
    });
  }

  async commit(): Promise<void> {
    if (this.#state !== ProducerState.CONNECTED) {
      throw stateError("Cannot commit without awaiting connect()");
    }
    if (!this.#ongoingTransaction) {
      throw stateError("Cannot commit, no transaction ongoing.");
    }
    this.#logger.debug("Attempting to commit transaction", this.#metadata());
    return new Promise((resolve, reject) => {
      this.#internalClient?.commitTransaction(5000, (err) => {
        if (err !== null) {
          reject(fromLibrdKafkaError(err));
          return;
        }
        this.#ongoingTransaction = false;
        resolve();
      });
    });
  }

  async abort(): Promise<void> {
    if (this.#state !== ProducerState.CONNECTED) {
      throw stateError("Cannot abort without awaiting connect()");
    }
    if (!this.#ongoingTransaction) {
      throw stateError("Cannot abort, no transaction ongoing.");
    }
    this.#logger.debug("Attempting to abort transaction", this.#metadata());
    return new Promise((resolve, reject) => {
      this.#internalClient?.abortTransaction(5000, (err) => {
        if (err !== null) {
          reject(fromLibrdKafkaError(err));
          return;
        }
        this.#ongoingTransaction = false;
        resolve();
      });
    });
  }

  async sendOffsets(arg: SendOffsetsArg): Promise<void> {
    const { consumerGroupId, topics, consumer } = arg ?? {};
    if (consumerGroupId !== undefined || !consumer) {
      throw invalidArg(
        "The sendOffsets method must be called with a connected consumer instance and without a consumerGroupId parameter.",
      );
    }
    if (!Array.isArray(topics) || topics.length === 0) {
      throw invalidArg("sendOffsets arguments are invalid");
    }
    if (this.#state !== ProducerState.CONNECTED) {
      throw stateError("Cannot sendOffsets without awaiting connect()");
    }
    if (!this.#ongoingTransaction) {
      throw stateError("Cannot sendOffsets, no transaction ongoing.");
    }

    const offsets: TopicPartitionOffset[] = topics.flatMap((topic) =>
      topic.partitions.map((p) => ({
        topic: String(topic.topic),
        partition: Number(p.partition),
        offset: Number(p.offset),
      })),
    );
    /* The KafkaJS Consumer exposes _getInternalClient() (as does upstream);
     * the callback sendOffsetsToTransaction then duck-types `.native`/`.client`. */
    const internal =
      typeof consumer._getInternalClient === "function"
        ? consumer._getInternalClient()
        : consumer;

    return new Promise((resolve, reject) => {
      this.#internalClient?.sendOffsetsToTransaction(
        offsets,
        internal as Parameters<CallbackProducer["sendOffsetsToTransaction"]>[1],
        (err) => {
          if (err !== null) reject(fromLibrdKafkaError(err));
          else resolve();
        },
      );
    });
  }

  /** Is a transaction currently open? */
  isActive(): boolean {
    return this.#ongoingTransaction;
  }

  /* ----------------------------------------------------------------- misc */

  /**
   * Flushes pending messages. Defaults to `{timeout: 500}`; on timeout →
   * rejects with KafkaJSTimeout (after one event-loop turn so pending await
   * send()s can run — matching upstream).
   */
  async flush(args: { timeout?: number } = { timeout: 500 }): Promise<void> {
    if (this.#state !== ProducerState.CONNECTED) {
      throw stateError("Cannot flush without awaiting connect()");
    }
    if (!Object.hasOwn(args, "timeout")) {
      throw invalidArg("timeout must be set for flushing");
    }
    this.#logger.debug(`Attempting to flush messages for ${args.timeout}ms`, this.#metadata());
    return new Promise((resolve, reject) => {
      this.#internalClient?.flush(args.timeout, (err) => {
        if (err !== null) {
          const kjsErr = fromLibrdKafkaError(err);
          if (err.code === ERROR_CODES.ERR__TIMED_OUT) {
            setTimeout(
              () => reject(new KafkaJSTimeout(kjsErr, { code: kjsErr.code })),
              0,
            );
          } else {
            reject(kjsErr);
          }
          return;
        }
        setTimeout(resolve, 0);
      });
    });
  }

  logger(): Logger {
    return this.#logger;
  }

  /**
   * Changes the SASL PLAIN/SCRAM credentials — before connect it edits the
   * config, after connect it goes through `rd_kafka_sasl_set_credentials`
   * (M6): existing connections are not torn down, the new credentials apply to
   * the next authentication (matching upstream).
   */
  setSaslCredentials(args: { username?: string; password?: string } = {}): void {
    if (!Object.hasOwn(args, "username")) {
      throw invalidArg("username must be set for setSaslCredentials");
    }
    if (!Object.hasOwn(args, "password")) {
      throw invalidArg("password must be set for setSaslCredentials");
    }
    if (this.#state < ProducerState.CONNECTING) {
      this.#userConfig["sasl.username"] = args.username as string;
      this.#userConfig["sasl.password"] = args.password as string;
      const block = this.#userConfig.kafkaJS;
      if (block !== undefined && block.sasl !== undefined) {
        // hasOwn was checked above — TS cannot narrow through Object.hasOwn.
        block.sasl.username = args.username as string;
        block.sasl.password = args.password as string;
      }
      return;
    }
    const client = this.#internalClient;
    if (client === null) {
      throw new KafkaJSError("setSaslCredentials: producer has no active client", {
        code: ERROR_CODES.ERR__STATE,
      });
    }
    try {
      client.setSaslCredentials(args.username as string, args.password as string);
    } catch (error) {
      throw error instanceof LibrdKafkaError ? fromLibrdKafkaError(error) : error;
    }
  }
}
