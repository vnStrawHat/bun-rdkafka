/**
 * kafkajs/errors.ts — the KafkaJS namespace's error system (FR-2).
 *
 * Semantics cross-checked against confluent-kafka-javascript's
 * `lib/kafkajs/_error.js`: errors distinguished by `error.code` (upstream's
 * settled decision — see MIGRATION.md), `type` is the error-code NAME (e.g.
 * `"ERR__TIMED_OUT"`), and the subclass tree keeps exactly the list upstream
 * exposes. `fromLibrdKafkaError()` is our version of
 * `createKafkaJsErrorFromLibRdKafkaError()`: picks the subclass by code.
 */

import { ERROR_CODES, ERROR_NAMES, LibrdKafkaError } from "../core/errors.ts";

/** `KafkaJS.ErrorCodes` — the same table as the Callback API's `CODES.ERRORS`. */
export const ErrorCodes = ERROR_CODES;

export interface KafkaJSErrorProperties {
  /** Retriable (transactional producer). */
  retriable?: boolean;
  /** Fatal (transactional producer). */
  fatal?: boolean;
  /** Abortable (transactional producer). */
  abortable?: boolean;
  stack?: string;
  code?: number;
}

/**
 * The promisified API's root error. Distinguish error kinds by
 * {@link KafkaJSError#code} (against `ErrorCodes`), not by `error.name`.
 */
export class KafkaJSError extends Error {
  override name = "KafkaJSError";
  /** The librdkafka error code — the PRIMARY field for distinguishing errors. */
  code: number;
  retriable: boolean;
  fatal: boolean;
  abortable: boolean;
  /** The error-code name (e.g. `"ERR__TIMED_OUT"`); `"ERR_UNKNOWN"` for unknown codes. */
  type: string;

  constructor(e: Error | string, properties: KafkaJSErrorProperties = {}) {
    super(typeof e === "string" ? e : e.message);
    const {
      retriable = false,
      fatal = false,
      abortable = false,
      stack,
      code = ERROR_CODES.ERR_UNKNOWN,
    } = properties;
    this.retriable = retriable;
    this.fatal = fatal;
    this.abortable = abortable;
    this.code = code;
    this.type = ERROR_NAMES[code] ?? "ERR_UNKNOWN";
    if (stack !== undefined) this.stack = stack;
  }
}

/** An error carrying a Kafka protocol error code from a response (code > 0). */
export class KafkaJSProtocolError extends KafkaJSError {
  override name = "KafkaJSProtocolError";
}

export class KafkaJSOffsetOutOfRange extends KafkaJSProtocolError {
  override name = "KafkaJSOffsetOutOfRange";
}

export class KafkaJSConnectionError extends KafkaJSError {
  override name = "KafkaJSConnectionError";
}

export class KafkaJSRequestTimeoutError extends KafkaJSError {
  override name = "KafkaJSRequestTimeoutError";
}

export class KafkaJSPartialMessageError extends KafkaJSError {
  override name = "KafkaJSPartialMessageError";
}

export class KafkaJSSASLAuthenticationError extends KafkaJSError {
  override name = "KafkaJSSASLAuthenticationError";
}

export class KafkaJSGroupCoordinatorNotFound extends KafkaJSError {
  override name = "KafkaJSGroupCoordinatorNotFound";
}

export class KafkaJSNotImplemented extends KafkaJSError {
  override name = "KafkaJSNotImplemented";
}

export class KafkaJSTimeout extends KafkaJSError {
  override name = "KafkaJSTimeout";
}

export class KafkaJSNoBrokerAvailableError extends KafkaJSError {
  override name = "KafkaJSNoBrokerAvailableError";
}

/** Per-topic error of `admin.createTopics()`. */
export class KafkaJSCreateTopicError extends KafkaJSProtocolError {
  topic: string;
  constructor(e: Error | string, topicName: string, properties?: KafkaJSErrorProperties) {
    super(e, properties);
    this.topic = topicName;
    this.name = "KafkaJSCreateTopicError";
  }
}

/** Aggregate error of `admin.deleteGroups()` — `groups` holds per-group reports. */
export class KafkaJSDeleteGroupsError extends KafkaJSError {
  groups: unknown[];
  constructor(e: Error | string, groups?: unknown[]) {
    super(e);
    this.groups = groups ?? [];
    this.name = "KafkaJSDeleteGroupsError";
  }
}

export interface DeleteTopicRecordsErrorPartition {
  partition: number;
  offset: string;
  error: KafkaJSError;
}

/** Aggregate error of `admin.deleteTopicRecords()` — retriable when EVERY error is. */
export class KafkaJSDeleteTopicRecordsError extends KafkaJSError {
  partitions: DeleteTopicRecordsErrorPartition[];
  constructor({ partitions }: { partitions: DeleteTopicRecordsErrorPartition[] }) {
    const retriable = partitions
      .filter(({ error }) => error !== null)
      .every(({ error }) => error.retriable === true);
    super("Error while deleting records", { retriable });
    this.name = "KafkaJSDeleteTopicRecordsError";
    this.partitions = partitions;
  }
}

/** Multiple simultaneous errors (e.g. `createTopics` with several failing topics). */
export class KafkaJSAggregateError extends Error {
  errors: unknown[];
  constructor(message: string, errors: unknown[]) {
    super(message);
    this.errors = errors;
    this.name = "KafkaJSAggregateError";
  }
}

/** Is the error part of the rebalance-in-progress family? (upstream helper) */
export const isRebalancing = (e: KafkaJSError): boolean =>
  e.type === "REBALANCE_IN_PROGRESS" ||
  e.type === "NOT_COORDINATOR_FOR_GROUP" ||
  e.type === "ILLEGAL_GENERATION";

export const isKafkaJSError = (e: unknown): e is KafkaJSError => e instanceof KafkaJSError;

/**
 * `LibrdKafkaError` (Callback API) → `KafkaJSError` — picks the subclass by
 * code, keeping upstream's selection order. `abortable` comes from
 * `isTxnRequiresAbort` (how that flag travels up from the shim — see
 * core/errors.ts).
 */
export function fromLibrdKafkaError(e: LibrdKafkaError): KafkaJSError {
  const properties: KafkaJSErrorProperties = {
    retriable: e.isRetriable,
    fatal: e.isFatal,
    abortable: e.isTxnRequiresAbort,
    code: e.code,
  };
  if (e.stack !== undefined) properties.stack = e.stack;

  switch (e.code) {
    case ERROR_CODES.ERR_OFFSET_OUT_OF_RANGE:
      return new KafkaJSOffsetOutOfRange(e, properties);
    case ERROR_CODES.ERR_REQUEST_TIMED_OUT:
      return new KafkaJSRequestTimeoutError(e, properties);
    case ERROR_CODES.ERR__PARTIAL:
      return new KafkaJSPartialMessageError(e, properties);
    case ERROR_CODES.ERR__AUTHENTICATION:
      return new KafkaJSSASLAuthenticationError(e, properties);
    case ERROR_CODES.ERR_COORDINATOR_NOT_AVAILABLE:
      return new KafkaJSGroupCoordinatorNotFound(e, properties);
    case ERROR_CODES.ERR__NOT_IMPLEMENTED:
      return new KafkaJSNotImplemented(e, properties);
    case ERROR_CODES.ERR__TIMED_OUT:
      return new KafkaJSTimeout(e, properties);
    case ERROR_CODES.ERR__ALL_BROKERS_DOWN:
      return new KafkaJSNoBrokerAvailableError(e, properties);
    case ERROR_CODES.ERR__TRANSPORT:
      return new KafkaJSConnectionError(e, properties);
    default:
      if (e.code > 0) return new KafkaJSProtocolError(e, properties);
      return new KafkaJSError(e, properties);
  }
}

/** Normalizes unknown → KafkaJSError (kept as-is when already a KafkaJSError). */
export function toKafkaJSError(e: unknown): KafkaJSError {
  if (e instanceof KafkaJSError) return e;
  if (e instanceof LibrdKafkaError) return fromLibrdKafkaError(e);
  if (e instanceof Error) return new KafkaJSError(e);
  return new KafkaJSError(String(e));
}
