/**
 * kafkajs/index.ts — the `KafkaJS` namespace surface, mirroring upstream's
 * `lib/kafkajs/_kafka.js` `module.exports` exactly: Kafka, the full error
 * class tree + ErrorCodes, logLevel, PartitionAssigners (alias
 * PartitionAssignors),
 * CompressionTypes, ConsumerGroupStates/Types, AclOperationTypes,
 * IsolationLevel. The Producer/Consumer/Admin classes are not exported
 * directly by upstream (only created via Kafka) — we additionally export their
 * TYPES (type-only) for use in user function signatures.
 */

export { Kafka } from "./kafka.ts";

export {
  ErrorCodes,
  KafkaJSAggregateError,
  KafkaJSConnectionError,
  KafkaJSCreateTopicError,
  KafkaJSDeleteGroupsError,
  KafkaJSDeleteTopicRecordsError,
  KafkaJSError,
  KafkaJSGroupCoordinatorNotFound,
  KafkaJSNoBrokerAvailableError,
  KafkaJSNotImplemented,
  KafkaJSOffsetOutOfRange,
  KafkaJSPartialMessageError,
  KafkaJSProtocolError,
  KafkaJSRequestTimeoutError,
  KafkaJSSASLAuthenticationError,
  KafkaJSTimeout,
  isKafkaJSError,
  isRebalancing,
  type KafkaJSErrorProperties,
} from "./errors.ts";

export {
  DefaultLogger,
  PartitionAssigners,
  PartitionAssigners as PartitionAssignors,
  logLevel,
  type CommonRawConfig,
  type Logger,
} from "./config-mapper.ts";

export { CompressionTypes } from "./producer.ts";
export type {
  IHeaders,
  Message,
  Producer,
  ProducerBatch,
  ProducerRecord,
  RecordMetadata,
  TopicMessages,
} from "./producer.ts";

export {
  AclOperationTypes,
  ConsumerGroupStates,
  ConsumerGroupTypes,
  IsolationLevel,
} from "./admin.ts";
export type { Admin, ITopicConfig } from "./admin.ts";

export type {
  Batch,
  Consumer,
  ConsumerRunConfig,
  ConsumerSubscribeTopics,
  EachBatchHandler,
  EachBatchPayload,
  EachMessageHandler,
  EachMessagePayload,
  KafkaJSMessage,
  TopicPartition,
  TopicPartitionOffset,
  TopicPartitionOffsetAndMetadata,
  CommittedOffset,
  AssignmentFns,
  RebalanceAssignment,
  RebalanceCallback,
} from "./consumer.ts";
