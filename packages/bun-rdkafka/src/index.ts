/**
 * bun-rdkafka — public entrypoint.
 *
 * Exports both API layers (see docs/02-detail-design.md §1):
 *  - Callback API (node-rdkafka style): Client (+ Producer, HighLevelProducer,
 *    KafkaConsumer, AdminClient from M2/M3), CODES, librdkafkaVersion
 *  - KafkaJS namespace (promisified, from M5)
 */

export {
  Client,
  type BrokerMetadata,
  type ClientInternalOptions,
  type ClientMetrics,
  type ConnectCallback,
  type DisconnectCallback,
  type LogEventPayload,
  type Metadata,
  type MetadataCallback,
  type MetadataOptions,
  type PartitionMetadata,
  type ReadyInfo,
  type StatsEventPayload,
  type ThrottleEventPayload,
  type TopicMetadata,
  type WatermarkCallback,
  type WatermarkOffsetsResult,
} from "./callback/client.ts";

export { CODES, ERROR_CODES, LibrdKafkaError } from "./core/errors.ts";
export type { ClientConfig } from "./core/config.ts";

/**
 * Version of the statically linked librdkafka. This is a FUNCTION (not a
 * string like upstream) so importing the package does not force a dlopen —
 * the native library loads on the first call. See the M2 report if a string
 * alias is ever needed.
 */
export { librdkafkaVersion } from "./ffi/loader.ts";

/**
 * librdkafka's `builtin.features` (e.g. `["gzip", "ssl", "sasl_scram", …]`).
 * Also a FUNCTION for the same lazy-load reason as `librdkafkaVersion`; the
 * default export below exposes both as upstream-style getters.
 */
export { features } from "./ffi/loader.ts";

/* M2/M3 append here:
 *   (M2: Producer/HighLevelProducer — exported below)
 *   (M3: KafkaConsumer — exported below)
 *   (M4: AdminClient — exported below)
 *   (M5: the KafkaJS namespace — exported below)
 */

// M5 — namespace KafkaJS (promisified API, FR-2)
export * as KafkaJS from "./kafkajs/index.ts";

/**
 * Admin API enums. Upstream exports them at BOTH layers (`rdkafka.d.ts` for the
 * Callback API and the `KafkaJS` namespace) — the same constant objects, with
 * no behavior attached.
 */
export {
  AclOperationTypes,
  ConsumerGroupStates,
  ConsumerGroupTypes,
  IsolationLevel,
} from "./kafkajs/admin.ts";

// M4 — AdminClient + OAUTHBEARER (Callback API)
export {
  AdminClient,
  type AdminCallback,
  type AdminEventHandlers,
  type AdminNode,
  type AdminOperationOptions,
  type DeleteGroupsReport,
  type DeleteRecordsEntry,
  type DeleteRecordsReport,
  type GroupDescription,
  type GroupOffsetsPartition,
  type GroupOffsetsRequest,
  type GroupOffsetsResult,
  type GroupOverview,
  type ListGroupsResult,
  type MemberDescription,
  OffsetSpec,
  type NewTopic,
  type TopicDescription,
  type TopicPartitionDescription,
} from "./callback/admin.ts";
export type { OauthBearerToken, OauthBearerTokenCallback } from "./callback/client.ts";

// M2 — Producer (Callback API)
// Note: the TopicPartitionOffset type uses kafka-consumer.ts's export (M3);
// producer.ts has a local version with the same shape.
export {
  Producer,
  STAGING_MAX_RECORDS,
  type DeliveryReportListener,
  type DeliveryReportPayload,
  type FlushCallback,
  type ProduceHeaders,
  type ProduceValue,
  type TransactionCallback,
} from "./callback/producer.ts";
export {
  HighLevelProducer,
  type HlpDeliveryCallback,
  type Serializer,
} from "./callback/high-level-producer.ts";

// M3 — KafkaConsumer (Callback API)
export {
  KafkaConsumer,
  type Assignment,
  type CommittedCallback,
  type ConsumeCallback,
  type EofEvent,
  type Message,
  type MessageCallback,
  type MessageHeader,
  type MessageKey,
  type MessageValue,
  type OffsetsForTimesCallback,
  type RebalanceProtocolName,
  type SeekCallback,
  type SubscribeTopicList,
  type TopicPartition,
  type TopicPartitionOffset,
  type TopicPartitionOffsetAndMetadata,
} from "./callback/kafka-consumer.ts";

/* -------------------------------------------------------------------------- */
/* Default export                                                              */
/* -------------------------------------------------------------------------- */

import { features as featuresFn, librdkafkaVersion as librdkafkaVersionFn } from "./ffi/loader.ts";
import * as callbackClient from "./callback/client.ts";
import * as coreErrors from "./core/errors.ts";
import * as kafkaJsNamespace from "./kafkajs/index.ts";
import * as kafkaJsAdmin from "./kafkajs/admin.ts";
import * as callbackAdmin from "./callback/admin.ts";
import * as callbackProducer from "./callback/producer.ts";
import * as callbackHlp from "./callback/high-level-producer.ts";
import * as callbackConsumer from "./callback/kafka-consumer.ts";

/**
 * Upstream-style default export (`import Kafka from "@vnstrawhat/bun-rdkafka"`
 * ≙ `require('@confluentinc/kafka-javascript')`): the named exports plus
 * `Kafka.features` / `Kafka.librdkafkaVersion` as **lazy getters** — reading
 * them (not importing) triggers the native load, so the values behave exactly
 * like upstream's constants without breaking the lazy-dlopen policy.
 */
const defaultExport = {
  Client: callbackClient.Client,
  Producer: callbackProducer.Producer,
  HighLevelProducer: callbackHlp.HighLevelProducer,
  KafkaConsumer: callbackConsumer.KafkaConsumer,
  AdminClient: callbackAdmin.AdminClient,
  OffsetSpec: callbackAdmin.OffsetSpec,
  STAGING_MAX_RECORDS: callbackProducer.STAGING_MAX_RECORDS,
  CODES: coreErrors.CODES,
  ERROR_CODES: coreErrors.ERROR_CODES,
  LibrdKafkaError: coreErrors.LibrdKafkaError,
  KafkaJS: kafkaJsNamespace,
  AclOperationTypes: kafkaJsAdmin.AclOperationTypes,
  ConsumerGroupStates: kafkaJsAdmin.ConsumerGroupStates,
  ConsumerGroupTypes: kafkaJsAdmin.ConsumerGroupTypes,
  IsolationLevel: kafkaJsAdmin.IsolationLevel,
  /** Lazy: loads the native library on first read. */
  get features(): string[] {
    return featuresFn();
  },
  /** Lazy: loads the native library on first read. */
  get librdkafkaVersion(): string {
    return librdkafkaVersionFn();
  },
};

export default defaultExport;
