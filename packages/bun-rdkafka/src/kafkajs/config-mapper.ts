/**
 * kafkajs/config-mapper.ts — the `kafkaJS` block → librdkafka property
 * translation table (FR-2, design §7).
 *
 * The table is a 1:1 cross-check against confluent-kafka-javascript's
 * `kafkaJSToRdKafkaConfig` / `#kafkaJSToProducerConfig` /
 * `#kafkaJSToConsumerConfig` (lib/kafkajs/_common.js, _producer.js,
 * _consumer.js) — including the DEFAULTS applied only when the kafkaJS block
 * exists and is NON-empty ("compatibility mode"), and the deliberately
 * rejected keys (`retry.factor`, `socketFactory`, `autoCommitThreshold`…).
 *
 * ## Composition (the contract with kafka.ts / producer.ts / consumer.ts):
 *
 * ```ts
 * const common = mapCommonConfig(raw);              // ONLY the kafkaJS-common translation
 * const mapped = mapProducerConfig(raw, common);    // + producer part + PASSTHROUGH
 * // mapped.globalConf is the final config handed to the Callback API.
 * ```
 *
 * Passthrough (every key outside `kafkaJS`) is assigned LAST, so it overrides
 * the translation — matching upstream's
 * `Object.assign(compatibleConfig, userConfig)` order. `topicConf` stays empty:
 * the shim routes topic-level properties itself (librdkafka's fallthrough
 * default topic conf).
 */

import { ERROR_CODES } from "../core/errors.ts";
import { KafkaJSError } from "./errors.ts";

/* ========================================================================== */
/* Types                                                                       */
/* ========================================================================== */

export interface MappedConfig {
  globalConf: Record<string, unknown>;
  topicConf: Record<string, unknown>;
}

export interface RetryConfig {
  maxRetryTime?: number;
  initialRetryTime?: number;
  retries?: number;
  factor?: number;
  multiplier?: number;
  restartOnFailure?: unknown;
}

export interface SaslConfig {
  mechanism: string;
  username?: string;
  password?: string;
  /** OAUTHBEARER: `(config: string) => Promise<{value, principal, lifetime, extensions?}>`. */
  oauthBearerProvider?: (oauthbearerConfig: string) => Promise<OauthProviderToken>;
}

export interface OauthProviderToken {
  value: string;
  principal: string;
  /** Epoch-ms when the token expires. */
  lifetime: number;
  extensions?: Record<string, string>;
}

export interface CommonKafkaJSBlock {
  brokers?: string[];
  clientId?: string;
  sasl?: SaslConfig;
  ssl?: boolean;
  requestTimeout?: number;
  enforceRequestTimeout?: boolean;
  connectionTimeout?: number;
  authenticationTimeout?: number;
  retry?: RetryConfig;
  logLevel?: number;
  logger?: Logger;
  [key: string]: unknown;
}

/** Raw config: `{ kafkaJS?: {...} }` + pass-through librdkafka properties. */
export interface CommonRawConfig {
  kafkaJS?: CommonKafkaJSBlock;
  [key: string]: unknown;
}

/* ========================================================================== */
/* Logger (logLevel, DefaultLogger, trampoline — as in upstream _common.js)    */
/* ========================================================================== */

/** The KafkaJS API's log levels. */
export const logLevel = Object.freeze({
  NOTHING: 0,
  ERROR: 1,
  WARN: 2,
  INFO: 3,
  DEBUG: 4,
});

export interface Logger {
  info(message: string, extra?: object): void;
  error(message: string, extra?: object): void;
  warn(message: string, extra?: object): void;
  debug(message: string, extra?: object): void;
  setLogLevel(level: number): void;
  namespace?(): Logger;
}

/** syslog severity (librdkafka) → logLevel KafkaJS. */
export const severityToLogLevel: Readonly<Record<number, number>> = Object.freeze({
  0: logLevel.NOTHING,
  1: logLevel.ERROR,
  2: logLevel.ERROR,
  3: logLevel.ERROR,
  4: logLevel.WARN,
  5: logLevel.WARN,
  6: logLevel.INFO,
  7: logLevel.DEBUG,
});

export class DefaultLogger implements Logger {
  private level: number = logLevel.INFO;

  setLogLevel(level: number): void {
    this.level = level;
  }

  info(message: string, extra?: object): void {
    if (this.level >= logLevel.INFO) console.info({ message, ...extra });
  }

  error(message: string, extra?: object): void {
    if (this.level >= logLevel.ERROR) console.error({ message, ...extra });
  }

  warn(message: string, extra?: object): void {
    if (this.level >= logLevel.WARN) console.warn({ message, ...extra });
  }

  debug(message: string, extra?: object): void {
    if (this.level >= logLevel.DEBUG) console.log({ message, ...extra });
  }

  namespace(): Logger {
    return this;
  }
}

/** The Callback API's `event.log` payload. */
export interface LogMessage {
  severity: number;
  fac: string;
  message: string;
  name?: string;
}

/** Forwards a librdkafka event.log to the user's logger. */
export function loggerTrampoline(msg: LogMessage, logger: Logger | undefined): void {
  if (!logger) return;
  const level = severityToLogLevel[msg.severity];
  const extra = { fac: msg.fac, timestamp: Date.now(), name: msg.name };
  switch (level) {
    case logLevel.NOTHING:
      break;
    case logLevel.ERROR:
      logger.error(msg.message, extra);
      break;
    case logLevel.WARN:
      logger.warn(msg.message, extra);
      break;
    case logLevel.INFO:
      logger.info(msg.message, extra);
      break;
    case logLevel.DEBUG:
      logger.debug(msg.message, extra);
      break;
    default:
      throw new KafkaJSError("Invalid logLevel", { code: ERROR_CODES.ERR__INVALID_ARG });
  }
}

/** Metadata for the binding's log messages (upstream shape). */
export function createBindingMessageMetadata(clientName?: string): object {
  return { name: clientName ?? "", fac: "BINDING", timestamp: Date.now() };
}

/* ========================================================================== */
/* Partition assigners                                                         */
/* ========================================================================== */

export const PartitionAssigners = Object.freeze({
  roundRobin: "roundrobin",
  range: "range",
  cooperativeSticky: "cooperative-sticky",
});

/* ========================================================================== */
/* Recognized kafkaJS keys (exact upstream table)                              */
/* ========================================================================== */

const KAFKAJS_PROPERTIES: Readonly<Record<string, readonly string[]>> = Object.freeze({
  common: [
    "brokers",
    "clientId",
    "sasl",
    "ssl",
    "requestTimeout",
    "enforceRequestTimeout",
    "connectionTimeout",
    "authenticationTimeout",
    "retry",
    "socketFactory",
    "reauthenticationThreshold",
    "logLevel",
    "logger",
  ],
  producer: [
    "createPartitioner",
    "metadataMaxAge",
    "allowAutoTopicCreation",
    "transactionTimeout",
    "idempotent",
    "maxInFlightRequests",
    "transactionalId",
    "compression",
    "acks",
    "timeout",
  ],
  consumer: [
    "groupId",
    "partitionAssigners",
    "partitionAssignors",
    "sessionTimeout",
    "rebalanceTimeout",
    "heartbeatInterval",
    "metadataMaxAge",
    "allowAutoTopicCreation",
    "maxBytesPerPartition",
    "maxWaitTimeInMs",
    "minBytes",
    "maxBytes",
    "readUncommitted",
    "maxInFlightRequests",
    "rackId",
    "fromBeginning",
    "autoCommit",
    "autoCommitInterval",
    "autoCommitThreshold",
  ],
  admin: [],
});

export type KafkaJsClientType = "producer" | "consumer" | "admin";
export type KafkaJsPropertyType = KafkaJsClientType | "common";

/** First key in the block NOT in common+clientType, or null. */
export function checkAllowedKeys(
  clientType: KafkaJsClientType,
  config: Record<string, unknown>,
): string | null {
  const allowedCommon = KAFKAJS_PROPERTIES["common"] ?? [];
  const allowedSpecific = KAFKAJS_PROPERTIES[clientType] ?? [];
  for (const key of Object.keys(config)) {
    if (!allowedCommon.includes(key) && !allowedSpecific.includes(key)) return key;
  }
  return null;
}

/**
 * First kafkaJS key appearing OUTSIDE the `kafkaJS` block (misplaced), or
 * null. `acks` is excluded because it shares its name with a librdkafka
 * property.
 */
export function checkIfKafkaJsKeysPresent(
  propertyType: KafkaJsPropertyType,
  config: Record<string, unknown>,
): string | null {
  const kjsKeys = KAFKAJS_PROPERTIES[propertyType] ?? [];
  for (const key of Object.keys(config)) {
    if (kjsKeys.includes(key) && key !== "acks") return key;
  }
  return null;
}

/* ========================================================================== */
/* Compatibility error messages (condensed from upstream, meaning preserved)   */
/* ========================================================================== */

function invalidArg(message: string): KafkaJSError {
  return new KafkaJSError(message, { code: ERROR_CODES.ERR__INVALID_ARG });
}

function notImplementedErr(message: string): KafkaJSError {
  return new KafkaJSError(message, { code: ERROR_CODES.ERR__NOT_IMPLEMENTED });
}

export const CompatibilityErrorMessages = Object.freeze({
  brokerString: () =>
    "The 'brokers' property must be an array of strings.\n" +
    "For example: ['kafka:9092', 'kafka2:9093']\n",
  saslUnsupportedMechanism: (mechanism: string) =>
    `SASL mechanism ${mechanism} is not supported.`,
  saslUsernamePasswordString: (mechanism: string) =>
    `The 'sasl.username' and 'sasl.password' properties must be strings and must be present for the mechanism ${mechanism}.`,
  saslOauthBearerProvider: () => `The 'oauthBearerProvider' property must be a function.`,
  sslObject: () =>
    "The 'ssl' property must be a boolean. Any additional configuration must be provided outside the kafkaJS block " +
    '(e.g. "ssl.ca.location", "enable.ssl.certificate.verification").',
  retryFactorMultiplier: () =>
    "The 'retry.factor' and 'retry.multiplier' are not supported. They are always set to the default of 0.2 and 2 respectively.",
  retryRestartOnFailure: () =>
    "The restartOnFailure property is ignored. The client always retries on failure.",
  socketFactory: () => "The socketFactory property is not supported.",
  reauthenticationThreshold: () =>
    "Reauthentication threshold cannot be set, and reauthentication is automated when 80% of connections.max.reauth.ms is reached.",
  logLevelName: (setLevel: unknown) =>
    "The log level must be one of: " +
    Object.keys(logLevel).join(", ") +
    ", was " +
    String(setLevel),
  unsupportedKey: (key: string) => `The '${key}' property is not supported.`,
  kafkaJSCommonKey: (key: string) =>
    `The '${key}' property seems to be a KafkaJS property in the main config block. ` +
    `It must be moved to the kafkaJS block: new Kafka({ kafkaJS: { ${key}: <value>, ... }, ... })`,
  kafkaJSClientKey: (key: string, cOrP: string) =>
    `The '${key}' property seems to be a KafkaJS property in the main config block. ` +
    `It must be moved to the kafkaJS block: kafka.${cOrP}({ kafkaJS: { ${key}: <value>, ... }, ... })`,
  createPartitioner: () =>
    "The 'createPartitioner' property is not supported yet. The default partitioner is set to murmur2_random, " +
    "compatible with the DefaultPartitioner and the Java partitioner. Alternatives can be set with the " +
    "'partitioner' librdkafka property outside the kafkaJS block.",
  partitionAssignors: () =>
    "partitionAssignors must be a list of strings from within `PartitionAssignors`.\n",
  autoCommitThreshold: () => "The property 'autoCommitThreshold' is not supported.",
});

/* ========================================================================== */
/* mapCommonConfig                                                             */
/* ========================================================================== */

/**
 * Translates the COMMON part of the kafkaJS block. Does NOT merge passthrough
 * (that is mapProducerConfig/mapConsumerConfig/mapAdminConfig's job — see the
 * doc at the top of this file). Empty/absent block → empty mapping (no
 * defaults applied — "compat mode" only kicks in when the block has content,
 * matching upstream).
 */
export function mapCommonConfig(raw: CommonRawConfig): MappedConfig {
  const globalConf: Record<string, unknown> = {};
  const block = raw.kafkaJS;
  if (!block || Object.keys(block).length === 0) return { globalConf, topicConf: {} };

  if (Object.hasOwn(block, "brokers")) {
    if (!Array.isArray(block.brokers)) {
      throw invalidArg(CompatibilityErrorMessages.brokerString());
    }
    globalConf["bootstrap.servers"] = block.brokers.join(",");
  }

  if (Object.hasOwn(block, "clientId")) globalConf["client.id"] = block.clientId;

  let withSasl = false;
  if (Object.hasOwn(block, "sasl") && block.sasl) {
    const sasl = block.sasl;
    const mechanism = sasl.mechanism.toUpperCase();
    if (mechanism === "OAUTHBEARER") {
      globalConf["sasl.mechanism"] = mechanism;
      if (Object.hasOwn(sasl, "oauthBearerProvider")) {
        const provider = sasl.oauthBearerProvider;
        if (typeof provider !== "function") {
          throw invalidArg(CompatibilityErrorMessages.saslOauthBearerProvider());
        }
        // Wraps a KafkaJS-style provider ({value,...}) → a Callback-API-style
        // token ({tokenValue,...}); Client.handleOauthRefresh supports a
        // Promise-returning cb.
        globalConf["oauthbearer_token_refresh_cb"] = (oauthbearerConfig: string) =>
          Promise.resolve(provider(oauthbearerConfig)).then((token) => {
            for (const field of ["value", "principal", "lifetime"] as const) {
              if (!Object.hasOwn(token, field)) {
                throw invalidArg(`Token must have a ${field} property.`);
              }
            }
            return {
              tokenValue: token.value,
              extensions: token.extensions,
              principal: token.principal,
              lifetime: token.lifetime,
            };
          });
      }
      /* An absent oauthBearerProvider is valid (librdkafka's unsecured token).
       * bun-rdkafka NOTE: the shim always enables the OAUTH_REFRESH event, so
       * the default unsecured path does NOT run — see callback/client.ts. */
    } else if (mechanism === "PLAIN" || mechanism.startsWith("SCRAM")) {
      if (typeof sasl.username !== "string" || typeof sasl.password !== "string") {
        throw invalidArg(CompatibilityErrorMessages.saslUsernamePasswordString(mechanism));
      }
      globalConf["sasl.mechanism"] = mechanism;
      globalConf["sasl.username"] = sasl.username;
      globalConf["sasl.password"] = sasl.password;
    } else {
      throw invalidArg(CompatibilityErrorMessages.saslUnsupportedMechanism(mechanism));
    }
    withSasl = true;
  }

  if (Object.hasOwn(block, "ssl") && typeof block.ssl !== "boolean") {
    throw invalidArg(CompatibilityErrorMessages.sslObject());
  }
  if (block.ssl && withSasl) globalConf["security.protocol"] = "sasl_ssl";
  else if (withSasl) globalConf["security.protocol"] = "sasl_plaintext";
  else if (block.ssl) globalConf["security.protocol"] = "ssl";

  globalConf["socket.timeout.ms"] = Object.hasOwn(block, "requestTimeout")
    ? block.requestTimeout
    : 30000; /* default KafkaJS */
  if (Object.hasOwn(block, "enforceRequestTimeout") && !block.enforceRequestTimeout) {
    globalConf["socket.timeout.ms"] = 300000;
  }

  const connectionTimeout = block.connectionTimeout ?? 1000;
  const authenticationTimeout = block.authenticationTimeout ?? 10000;
  globalConf["socket.connection.setup.timeout.ms"] = Math.max(
    Number(connectionTimeout) + Number(authenticationTimeout),
    1000,
  );

  const retry = block.retry ?? {};
  globalConf["retry.backoff.max.ms"] = retry.maxRetryTime ?? 30000;
  globalConf["retry.backoff.ms"] = retry.initialRetryTime ?? 300;
  if (typeof retry.factor === "number" || typeof retry.multiplier === "number") {
    throw invalidArg(CompatibilityErrorMessages.retryFactorMultiplier());
  }
  if (retry.restartOnFailure) {
    throw invalidArg(CompatibilityErrorMessages.retryRestartOnFailure());
  }

  if (Object.hasOwn(block, "socketFactory")) {
    throw invalidArg(CompatibilityErrorMessages.socketFactory());
  }
  if (Object.hasOwn(block, "reauthenticationThreshold")) {
    throw invalidArg(CompatibilityErrorMessages.reauthenticationThreshold());
  }

  globalConf["log_level"] = 6; /* LOG_INFO — default ở compat mode */
  if (Object.hasOwn(block, "logLevel")) {
    let setLevel = block.logLevel;
    const envLevel = process.env["KAFKAJS_LOG_LEVEL"];
    if (envLevel) {
      setLevel = (logLevel as Record<string, number>)[envLevel.toUpperCase()];
    }
    switch (setLevel) {
      case logLevel.NOTHING:
        globalConf["log_level"] = 0; /* LOG_EMERG */
        break;
      case logLevel.ERROR:
        globalConf["log_level"] = 3; /* LOG_ERR */
        break;
      case logLevel.WARN:
        globalConf["log_level"] = 4; /* LOG_WARNING */
        break;
      case logLevel.INFO:
        globalConf["log_level"] = 6; /* LOG_INFO */
        break;
      case logLevel.DEBUG:
        globalConf["log_level"] = 7; /* LOG_DEBUG */
        break;
      default:
        throw invalidArg(CompatibilityErrorMessages.logLevelName(setLevel));
    }
  }

  return { globalConf, topicConf: {} };
}

/* ========================================================================== */
/* Passthrough helper                                                          */
/* ========================================================================== */

/**
 * Every key outside `kafkaJS` — assigned LAST (overriding the translation,
 * like upstream). `stats_cb` is excluded (the caller extracts it itself;
 * ConfigBuilder does not accept it); `log_level`/`debug` go down to librdkafka
 * as usual.
 */
function passthroughOf(raw: CommonRawConfig): Record<string, unknown> {
  const passthrough: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(raw)) {
    if (key === "kafkaJS" || key === "stats_cb") continue;
    passthrough[key] = value;
  }
  return passthrough;
}

function blockOf(
  raw: CommonRawConfig,
  clientType: KafkaJsClientType,
): Record<string, unknown> | null {
  const block = raw.kafkaJS;
  if (!block || Object.keys(block).length === 0) return null;
  const disallowed = checkAllowedKeys(clientType, block);
  if (disallowed !== null) {
    throw invalidArg(CompatibilityErrorMessages.unsupportedKey(disallowed));
  }
  return block;
}

/* ========================================================================== */
/* mapProducerConfig                                                           */
/* ========================================================================== */

/**
 * The producer translation (the `_producer.js#kafkaJSToProducerConfig` table)
 * + merged common + passthrough. `common` MUST be the result of
 * `mapCommonConfig(raw)`.
 */
export function mapProducerConfig(raw: CommonRawConfig, common: MappedConfig): MappedConfig {
  const globalConf: Record<string, unknown> = { ...common.globalConf };
  const block = blockOf(raw, "producer");

  if (block !== null) {
    if (Object.hasOwn(block, "createPartitioner")) {
      throw notImplementedErr(CompatibilityErrorMessages.createPartitioner());
    }
    globalConf["partitioner"] = "murmur2_random";

    if (Object.hasOwn(block, "metadataMaxAge")) {
      globalConf["topic.metadata.refresh.interval.ms"] = block["metadataMaxAge"];
    }
    if (Object.hasOwn(block, "allowAutoTopicCreation")) {
      globalConf["allow.auto.create.topics"] = block["allowAutoTopicCreation"];
    }

    globalConf["transaction.timeout.ms"] = Object.hasOwn(block, "transactionTimeout")
      ? block["transactionTimeout"]
      : 60000;
    // librdkafka invariant: socket.timeout.ms <= transaction.timeout.ms + 100.
    const socketTimeout = Number(globalConf["socket.timeout.ms"]);
    const txnTimeout = Number(globalConf["transaction.timeout.ms"]);
    if (Number.isFinite(socketTimeout) && socketTimeout > txnTimeout + 100) {
      globalConf["socket.timeout.ms"] = txnTimeout + 100;
    }

    if (Object.hasOwn(block, "idempotent")) {
      globalConf["enable.idempotence"] = block["idempotent"];
    }
    if (Object.hasOwn(block, "maxInFlightRequests")) {
      globalConf["max.in.flight"] = block["maxInFlightRequests"];
    }
    if (Object.hasOwn(block, "transactionalId")) {
      globalConf["transactional.id"] = block["transactionalId"];
    }
    if (Object.hasOwn(block, "compression")) {
      globalConf["compression.codec"] = block["compression"];
    }
    if (Object.hasOwn(block, "acks")) globalConf["acks"] = block["acks"];
    if (Object.hasOwn(block, "timeout")) globalConf["request.timeout.ms"] = block["timeout"];

    const retry = (block["retry"] as RetryConfig | undefined) ?? {};
    globalConf["retries"] = retry.retries ?? 5;
  }

  Object.assign(globalConf, passthroughOf(raw));
  return { globalConf, topicConf: { ...common.topicConf } };
}

/* ========================================================================== */
/* mapConsumerConfig                                                           */
/* ========================================================================== */

/**
 * The consumer translation (the `_consumer.js#kafkaJSToConsumerConfig` table)
 * + merged common + passthrough. Runtime-layer overrides (rebalance_cb,
 * `enable.auto.offset.store=false`…) are consumer.ts's job, NOT the mapper's.
 */
export function mapConsumerConfig(raw: CommonRawConfig, common: MappedConfig): MappedConfig {
  const globalConf: Record<string, unknown> = { ...common.globalConf };
  const block = blockOf(raw, "consumer");

  const protocol = raw["group.protocol"];
  const isClassicProtocol =
    protocol === undefined ||
    (typeof protocol === "string" && protocol.toLowerCase() === "classic");

  if (block !== null) {
    if (Object.hasOwn(block, "groupId")) globalConf["group.id"] = block["groupId"];

    let assignors = block["partitionAssignors"];
    if (Object.hasOwn(block, "partitionAssigners")) assignors = block["partitionAssigners"];
    if (assignors !== undefined) {
      if (!isClassicProtocol) {
        throw invalidArg(
          "partitionAssignors is not supported when group.protocol is not 'classic'.",
        );
      }
      if (!Array.isArray(assignors) || assignors.some((a) => typeof a !== "string")) {
        throw invalidArg(CompatibilityErrorMessages.partitionAssignors());
      }
      globalConf["partition.assignment.strategy"] = assignors.join(",");
    } else if (isClassicProtocol) {
      globalConf["partition.assignment.strategy"] = PartitionAssigners.roundRobin;
    }

    if (Object.hasOwn(block, "sessionTimeout")) {
      if (!isClassicProtocol) {
        throw invalidArg("sessionTimeout is not supported when group.protocol is not 'classic'.");
      }
      globalConf["session.timeout.ms"] = block["sessionTimeout"];
    } else if (isClassicProtocol) {
      globalConf["session.timeout.ms"] = 30000;
    }

    if (Object.hasOwn(block, "heartbeatInterval")) {
      if (!isClassicProtocol) {
        throw invalidArg(
          "heartbeatInterval is not supported when group.protocol is not 'classic'.",
        );
      }
      globalConf["heartbeat.interval.ms"] = block["heartbeatInterval"];
    }

    if (Object.hasOwn(block, "rebalanceTimeout")) {
      /* librdkafka uses the max poll interval as the rebalance timeout. */
      globalConf["max.poll.interval.ms"] = Number(block["rebalanceTimeout"]);
    } else if (!globalConf["max.poll.interval.ms"]) {
      globalConf["max.poll.interval.ms"] = 300000; /* librdkafka default */
    }

    if (Object.hasOwn(block, "metadataMaxAge")) {
      globalConf["topic.metadata.refresh.interval.ms"] = block["metadataMaxAge"];
    }

    globalConf["allow.auto.create.topics"] = Object.hasOwn(block, "allowAutoTopicCreation")
      ? block["allowAutoTopicCreation"]
      : true;

    globalConf["max.partition.fetch.bytes"] = Object.hasOwn(block, "maxBytesPerPartition")
      ? block["maxBytesPerPartition"]
      : 1048576;

    if (Object.hasOwn(block, "maxWaitTimeInMs")) {
      globalConf["fetch.wait.max.ms"] = block["maxWaitTimeInMs"];
    }
    if (Object.hasOwn(block, "minBytes")) globalConf["fetch.min.bytes"] = block["minBytes"];

    globalConf["fetch.message.max.bytes"] = Object.hasOwn(block, "maxBytes")
      ? block["maxBytes"]
      : 10485760;

    if (Object.hasOwn(block, "readUncommitted")) {
      globalConf["isolation.level"] = block["readUncommitted"]
        ? "read_uncommitted"
        : "read_committed";
    }
    if (Object.hasOwn(block, "maxInFlightRequests")) {
      globalConf["max.in.flight"] = block["maxInFlightRequests"];
    }
    if (Object.hasOwn(block, "rackId")) globalConf["client.rack"] = block["rackId"];

    if (Object.hasOwn(block, "fromBeginning")) {
      globalConf["auto.offset.reset"] = block["fromBeginning"] ? "earliest" : "latest";
    }

    globalConf["enable.auto.commit"] = Object.hasOwn(block, "autoCommit")
      ? block["autoCommit"]
      : true;
    if (Object.hasOwn(block, "autoCommitInterval")) {
      globalConf["auto.commit.interval.ms"] = block["autoCommitInterval"];
    }
    if (Object.hasOwn(block, "autoCommitThreshold")) {
      throw notImplementedErr(CompatibilityErrorMessages.autoCommitThreshold());
    }
  }

  Object.assign(globalConf, passthroughOf(raw));
  return { globalConf, topicConf: { ...common.topicConf } };
}

/* ========================================================================== */
/* mapAdminConfig                                                              */
/* ========================================================================== */

/** Admin has no dedicated kafkaJS keys — just common + passthrough. */
export function mapAdminConfig(raw: CommonRawConfig, common: MappedConfig): MappedConfig {
  const globalConf: Record<string, unknown> = { ...common.globalConf };
  blockOf(raw, "admin"); // validation: producer/consumer keys in an admin block → throw
  Object.assign(globalConf, passthroughOf(raw));
  return { globalConf, topicConf: { ...common.topicConf } };
}

/* ========================================================================== */
/* Raw config merge (Kafka.producer()/consumer()/admin())                      */
/* ========================================================================== */

/**
 * Merges `Kafka`'s common config with the client-specific config: the
 * `kafkaJS` block shallow-merges one level (specific over common), the rest
 * shallow-merges too (specific over common) — matching upstream's
 * `#mergeConfiguration`.
 */
export function mergeRawConfigs(
  common: CommonRawConfig,
  specific: CommonRawConfig | undefined,
): CommonRawConfig {
  const spec = { ...(specific ?? {}) };
  const merged: CommonRawConfig = { ...common };
  merged.kafkaJS = { ...(common.kafkaJS ?? {}) };
  if (typeof spec.kafkaJS === "object" && spec.kafkaJS !== null) {
    Object.assign(merged.kafkaJS, spec.kafkaJS);
    delete spec.kafkaJS;
  }
  Object.assign(merged, spec);
  if (Object.keys(merged.kafkaJS ?? {}).length === 0) delete merged.kafkaJS;
  return merged;
}

/**
 * Extracts the logger + log level with upstream's exact precedence:
 * kafkaJS.logLevel (already translated into the mapped `log_level`) <
 * passthrough `log_level` < passthrough `debug`. Returns the logger with its
 * level set.
 */
export function resolveLogger(raw: CommonRawConfig, mapped: MappedConfig): Logger {
  const logger: Logger = raw.kafkaJS?.logger ?? new DefaultLogger();
  const mappedLevel = mapped.globalConf["log_level"];
  if (typeof mappedLevel === "number") {
    logger.setLogLevel(severityToLogLevel[mappedLevel] ?? logLevel.INFO);
  }
  if (Object.hasOwn(raw, "debug")) logger.setLogLevel(logLevel.DEBUG);
  return logger;
}

/** The `stats_cb` passthrough (ConfigBuilder does not accept this key). */
export function extractStatsCb(raw: CommonRawConfig): ((payload: unknown) => void) | undefined {
  const cb = raw["stats_cb"];
  return typeof cb === "function" ? (cb as (payload: unknown) => void) : undefined;
}
