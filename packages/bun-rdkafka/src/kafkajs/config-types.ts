/**
 * kafkajs/config-types.ts — the PUBLIC config types of the KafkaJS namespace,
 * mirroring upstream's `types/kafkajs.d.ts` (`KafkaConfig`, `ProducerConfig`,
 * `ConsumerConfig`, `AdminConfig` and the `*ConstructorConfig` wrappers) so
 * editors complete both the `kafkaJS` block and the librdkafka pass-through
 * keys.
 *
 * Deviations from upstream, on purpose:
 *  - `brokers` / `groupId` are optional: the same value may be given as the
 *    librdkafka property (`bootstrap.servers` / `group.id`) outside the block.
 *  - `logLevel` is a number (our `logLevel` is a frozen object, not an enum).
 *  - The constructor configs also accept the `js.*` options ({@link JsConfig}).
 *
 * The mapper (`config-mapper.ts`) still validates keys at runtime; these types
 * are for completion, not enforcement.
 */

import type { JsConfig } from "../core/config.ts";
import type {
  ConsumerGlobalConfig,
  ConsumerTopicConfig,
  GlobalConfig,
  ProducerGlobalConfig,
  ProducerTopicConfig,
} from "../core/librdkafka-config.ts";
import type { Logger, OauthProviderToken } from "./config-mapper.ts";
import type { RebalanceCallback } from "./consumer.ts";

/* ---- common ---------------------------------------------------------------- */

/** `sasl.mechanism` values accepted in the `kafkaJS` block (case-insensitive at runtime). */
export type SASLMechanism =
  | "plain"
  | "scram-sha-256"
  | "scram-sha-512"
  | "oauthbearer"
  | "PLAIN"
  | "SCRAM-SHA-256"
  | "SCRAM-SHA-512"
  | "OAUTHBEARER";

export type SASLOptions =
  | {
      mechanism: "plain" | "scram-sha-256" | "scram-sha-512" | "PLAIN" | "SCRAM-SHA-256" | "SCRAM-SHA-512";
      username: string;
      password: string;
    }
  | {
      mechanism: "oauthbearer" | "OAUTHBEARER";
      /** Called on every token refresh with librdkafka's `sasl.oauthbearer.config`. */
      oauthBearerProvider: (oauthbearerConfig: string) => Promise<OauthProviderToken>;
    };

/** `kafkaJS.retry` — only `retries` is honoured (as upstream); the rest is validated/ignored. */
export type RetryOptions = {
  maxRetryTime?: number;
  initialRetryTime?: number;
  retries?: number;
};

/** The `kafkaJS` block accepted by `new Kafka({ kafkaJS })` and merged into every client. */
export type KafkaConfig = {
  /** Broker list — alternatively `bootstrap.servers` outside the block. */
  brokers?: string[];
  /** → `security.protocol` (`true` = SSL, combined with `sasl` = SASL_SSL). Extra TLS options go outside the block (`ssl.ca.location`, …). */
  ssl?: boolean;
  sasl?: SASLOptions;
  /** → `client.id` */
  clientId?: string;
  /** → `socket.connection.setup.timeout.ms` */
  connectionTimeout?: number;
  /** → `socket.connection.setup.timeout.ms` (added to `connectionTimeout`) */
  authenticationTimeout?: number;
  /** → `request.timeout.ms` (only with `enforceRequestTimeout`) */
  requestTimeout?: number;
  enforceRequestTimeout?: boolean;
  retry?: RetryOptions;
  /** One of `logLevel.NOTHING|ERROR|WARN|INFO|DEBUG` */
  logLevel?: number;
  logger?: Logger;
};

/* ---- producer -------------------------------------------------------------- */

/**
 * `kafkaJS` block of `kafka.producer({ kafkaJS })`. Also accepts the common
 * keys of {@link KafkaConfig} (the runtime merges them), a superset of upstream's type.
 */
export type ProducerConfig = KafkaConfig & {
  /** → `topic.metadata.refresh.interval.ms` */
  metadataMaxAge?: number;
  /** → `allow.auto.create.topics` */
  allowAutoTopicCreation?: boolean;
  /** → `enable.idempotence` */
  idempotent?: boolean;
  /** → `transactional.id` */
  transactionalId?: string;
  /** → `transaction.timeout.ms` (default 60000 when the block is present) */
  transactionTimeout?: number;
  /** → `max.in.flight` */
  maxInFlightRequests?: number;
  /** → `acks` (`-1`/`all` = all in-sync replicas) */
  acks?: number;
  /** → `compression.codec` — a {@link CompressionTypes} value */
  compression?: "none" | "gzip" | "snappy" | "lz4" | "zstd";
  /** → `request.timeout.ms` */
  timeout?: number;
};

/* ---- consumer -------------------------------------------------------------- */

/** `partitionAssigners` values — see `PartitionAssigners`. */
export type PartitionAssignerName = "roundrobin" | "range" | "cooperative-sticky";

/**
 * `kafkaJS` block of `kafka.consumer({ kafkaJS })`. Also accepts the common
 * keys of {@link KafkaConfig} (the runtime merges them), a superset of upstream's type.
 */
export type ConsumerConfig = KafkaConfig & {
  /** → `group.id` — alternatively `group.id` outside the block. */
  groupId?: string;
  /** → `topic.metadata.refresh.interval.ms` */
  metadataMaxAge?: number;
  /** → `session.timeout.ms` */
  sessionTimeout?: number;
  /** → `max.poll.interval.ms` */
  rebalanceTimeout?: number;
  /** → `heartbeat.interval.ms` */
  heartbeatInterval?: number;
  /** → `max.partition.fetch.bytes` */
  maxBytesPerPartition?: number;
  /** → `fetch.min.bytes` */
  minBytes?: number;
  /** → `fetch.message.max.bytes` */
  maxBytes?: number;
  /** → `fetch.wait.max.ms` */
  maxWaitTimeInMs?: number;
  /** → `allow.auto.create.topics` */
  allowAutoTopicCreation?: boolean;
  /** → `max.in.flight` */
  maxInFlightRequests?: number;
  /** → `isolation.level` */
  readUncommitted?: boolean;
  /** → `client.rack` */
  rackId?: string;
  /** → `auto.offset.reset` (`earliest` / `latest`) */
  fromBeginning?: boolean;
  /** → `enable.auto.commit` (default `true`) */
  autoCommit?: boolean;
  /** → `auto.commit.interval.ms` */
  autoCommitInterval?: number;
  /** → `partition.assignment.strategy` (classic group protocol only) */
  partitionAssigners?: PartitionAssignerName[];
  /** Alias of `partitionAssigners`. */
  partitionAssignors?: PartitionAssignerName[];
};

/* ---- admin ----------------------------------------------------------------- */

/** `kafkaJS` block of `kafka.admin({ kafkaJS })` — the common keys only. */
export type AdminConfig = KafkaConfig;

/* ---- constructor configs ---------------------------------------------------- */

/** `new Kafka(config)`: librdkafka global properties + `js.*` + the common `kafkaJS` block. */
export type CommonConstructorConfig = GlobalConfig & JsConfig & { kafkaJS?: KafkaConfig };

/** `kafka.producer(config)`: producer/topic properties + `js.*` + the producer `kafkaJS` block. */
export type ProducerConstructorConfig = ProducerGlobalConfig &
  ProducerTopicConfig &
  JsConfig & { kafkaJS?: ProducerConfig };

/** `kafka.consumer(config)`: consumer/topic properties + `js.*` + the consumer `kafkaJS` block. */
export type ConsumerConstructorConfig = ConsumerGlobalConfig &
  ConsumerTopicConfig &
  JsConfig & {
    kafkaJS?: ConsumerConfig;
    /** Rebalance hook `(err, assignment, { assign, unassign, … })` — see {@link RebalanceCallback}. */
    rebalance_cb?: RebalanceCallback;
  };

/** `kafka.admin(config)`: global properties + `js.*` + the admin `kafkaJS` block. */
export type AdminConstructorConfig = GlobalConfig & JsConfig & { kafkaJS?: AdminConfig };
