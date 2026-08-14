/**
 * kafkajs/admin.ts — the promisified `KafkaJS.Admin` (FR-2), built on the
 * Callback API `AdminClient` (../callback/admin).
 *
 * Semantics cross-checked against confluent-kafka-javascript's
 * `lib/kafkajs/_admin.js`. Internal difference (api-compat-not-internals):
 * upstream waits for 'ready' via node-rdkafka's eventHandlers; we confirm the
 * connection with one `listTopics` call after `AdminClient.create()` — every
 * AdminClient method waits for ready itself, so this call resolves when ready
 * / rejects with the exact connect error.
 *
 * UPSTREAM DEVIATIONS (recorded in the M5a report, updated in M6):
 *  - `fetchTopicOffsets`: READ_UNCOMMITTED takes the watermark path (fast,
 *    broker-locally cached); READ_COMMITTED goes through admin ListOffsets (M6).
 *  - `listGroups` has a per-group `type` (M6); `protocolType` is ABSENT —
 *    librdkafka does not expose it on ConsumerGroupListing.
 *    `matchConsumerGroupStates`/`Types` filter on the JS side.
 */

import type { ClientConfig } from "../core/config.ts";
import { ERROR_CODES, LibrdKafkaError } from "../core/errors.ts";
import {
  AdminClient,
  OffsetSpec,
  type AdminOperationOptions,
  type DeleteRecordsReport,
  type GroupDescription,
  type GroupOffsetsResult,
  type ListGroupsResult,
  type ListOffsetsReport,
  type NewTopic,
  type TopicDescription,
} from "../callback/admin.ts";
import type { Client } from "../callback/client.ts";
import {
  createBindingMessageMetadata,
  extractStatsCb,
  loggerTrampoline,
  mapAdminConfig,
  mapCommonConfig,
  resolveLogger,
  type CommonRawConfig,
  type Logger,
  type LogMessage,
} from "./config-mapper.ts";
import {
  KafkaJSAggregateError,
  KafkaJSCreateTopicError,
  KafkaJSDeleteGroupsError,
  KafkaJSDeleteTopicRecordsError,
  KafkaJSError,
  KafkaJSNotImplemented,
  fromLibrdKafkaError,
  toKafkaJSError,
} from "./errors.ts";

/* ========================================================================== */
/* Enums (values per librdkafka)                                               */
/* ========================================================================== */

/** `rd_kafka_consumer_group_state_t`. */
export const ConsumerGroupStates = Object.freeze({
  UNKNOWN: 0,
  PREPARING_REBALANCE: 1,
  COMPLETING_REBALANCE: 2,
  STABLE: 3,
  DEAD: 4,
  EMPTY: 5,
});

/** `rd_kafka_consumer_group_type_t`. */
export const ConsumerGroupTypes = Object.freeze({
  UNKNOWN: 0,
  CONSUMER: 1,
  CLASSIC: 2,
});

/** `rd_kafka_AclOperation_t`. */
export const AclOperationTypes = Object.freeze({
  UNKNOWN: 0,
  ANY: 1,
  ALL: 2,
  READ: 3,
  WRITE: 4,
  CREATE: 5,
  DELETE: 6,
  ALTER: 7,
  DESCRIBE: 8,
  CLUSTER_ACTION: 9,
  DESCRIBE_CONFIGS: 10,
  ALTER_CONFIGS: 11,
  IDEMPOTENT_WRITE: 12,
});

/** `rd_kafka_IsolationLevel_t`. */
export const IsolationLevel = Object.freeze({
  READ_UNCOMMITTED: 0,
  READ_COMMITTED: 1,
});

/** The shim's type names (rd_kafka_consumer_group_type_name) → enum. */
const GROUP_TYPE_BY_NAME: Readonly<Record<string, number>> = Object.freeze({
  Unknown: ConsumerGroupTypes.UNKNOWN,
  Consumer: ConsumerGroupTypes.CONSUMER,
  Classic: ConsumerGroupTypes.CLASSIC,
});

/** The shim's state names (rd_kafka_consumer_group_state_name) → enum. */
const GROUP_STATE_BY_NAME: Readonly<Record<string, number>> = Object.freeze({
  Unknown: ConsumerGroupStates.UNKNOWN,
  PreparingRebalance: ConsumerGroupStates.PREPARING_REBALANCE,
  CompletingRebalance: ConsumerGroupStates.COMPLETING_REBALANCE,
  Stable: ConsumerGroupStates.STABLE,
  Dead: ConsumerGroupStates.DEAD,
  Empty: ConsumerGroupStates.EMPTY,
});

/* ========================================================================== */
/* Public types (upstream shapes)                                              */
/* ========================================================================== */

export interface ITopicConfig {
  topic: string;
  numPartitions?: number | null;
  replicationFactor?: number | null;
  configEntries?: { name: string; value: string }[] | null;
  replicaAssignment?: unknown;
}

export interface FetchOffsetsPartition {
  partition: number;
  offset: string;
  metadata: string | null;
  leaderEpoch: number | null;
  error: LibrdKafkaError | null;
}

export interface FetchOffsetsTopic {
  topic: string;
  partitions: FetchOffsetsPartition[];
}

export interface TopicOffset {
  partition: number;
  offset: string;
  high: string;
  low: string;
}

const enum AdminState {
  INIT = 0,
  CONNECTING = 1,
  CONNECTED = 4,
  DISCONNECTING = 5,
  DISCONNECTED = 6,
}

/** An existing client (KafkaJS Producer/Consumer) backing a dependent admin. */
export interface DependentClientSource {
  _getInternalClient(): Client | null;
  logger(): Logger;
}

function stateError(message: string): KafkaJSError {
  return new KafkaJSError(message, { code: ERROR_CODES.ERR__STATE });
}

function toErr(e: unknown): KafkaJSError {
  return toKafkaJSError(e);
}

/* ========================================================================== */
/* Admin                                                                       */
/* ========================================================================== */

export class Admin {
  #userConfig: CommonRawConfig | null;
  #existingClient: DependentClientSource | null;
  #internalClient: AdminClient | null = null;
  /** The client used for watermarks/metadata (the AdminClient itself, or the host). */
  #watermarkClient: Client | null = null;
  #state: AdminState = AdminState.INIT;
  #logger: Logger;
  #statsCb: ((payload: unknown) => void) | undefined;

  /** Not used directly — created via `kafka.admin()` or `dependentAdmin()`. */
  constructor(
    rawMergedConfig: CommonRawConfig | null,
    existingClient: DependentClientSource | null = null,
  ) {
    this.#userConfig = rawMergedConfig;
    this.#existingClient = existingClient;
    let logger: Logger;
    try {
      const raw = rawMergedConfig ?? {};
      logger = resolveLogger(raw, mapCommonConfig(raw));
    } catch {
      logger = resolveLogger({}, { globalConf: {}, topicConf: {} });
    }
    this.#logger = logger;
  }

  #metadata(): object {
    return createBindingMessageMetadata(this.#internalClient?.name);
  }

  #requireConnected(): AdminClient {
    if (this.#state !== AdminState.CONNECTED || this.#internalClient === null) {
      throw stateError("Admin client is not connected.");
    }
    return this.#internalClient;
  }

  async connect(): Promise<void> {
    if (this.#state !== AdminState.INIT) {
      throw stateError("Connect has already been called elsewhere.");
    }
    this.#state = AdminState.CONNECTING;

    try {
      if (this.#existingClient !== null) {
        const underlying = this.#existingClient._getInternalClient();
        if (underlying === null || !underlying.isConnected()) {
          throw stateError("Underlying client is not connected.");
        }
        this.#logger = this.#existingClient.logger();
        this.#internalClient = AdminClient.createFrom(underlying);
        this.#watermarkClient = underlying;
        this.#state = AdminState.CONNECTED;
        this.#logger.info("Admin client connected", this.#metadata());
        return;
      }

      const raw = this.#userConfig ?? {};
      const mapped = mapAdminConfig(raw, mapCommonConfig(raw));
      this.#logger = resolveLogger(raw, mapped);
      this.#statsCb = extractStatsCb(raw);

      const admin = AdminClient.create(mapped.globalConf as ClientConfig, {
        "event.log": ((msg: LogMessage) => loggerTrampoline(msg, this.#logger)) as (
          ...args: unknown[]
        ) => void,
        ...(this.#statsCb !== undefined
          ? {
              "event.stats": ((payload: unknown) => this.#statsCb?.(payload)) as (
                ...args: unknown[]
              ) => void,
            }
          : {}),
      });
      this.#internalClient = admin;
      this.#watermarkClient = admin;

      /* Connection confirmation: the first method waits for ready itself (see the doc at the top). */
      await new Promise<void>((resolve, reject) => {
        admin.listTopics({ timeout: 30000 }, (err) => {
          if (err !== null) reject(fromLibrdKafkaError(err));
          else resolve();
        });
      });
      this.#state = AdminState.CONNECTED;
      this.#logger.info("Admin client connected", this.#metadata());
    } catch (e) {
      this.#state = AdminState.DISCONNECTED;
      this.#internalClient?.disconnect();
      throw toErr(e);
    }
  }

  async disconnect(): Promise<void> {
    if (this.#state === AdminState.INIT) return;
    if (this.#state >= AdminState.DISCONNECTING) return;
    this.#state = AdminState.DISCONNECTING;
    await new Promise<void>((resolve, reject) => {
      if (this.#internalClient === null) {
        resolve();
        return;
      }
      this.#internalClient.disconnect((err) => {
        if (err !== null) {
          reject(fromLibrdKafkaError(err));
          return;
        }
        this.#state = AdminState.DISCONNECTED;
        this.#logger.info("Admin client disconnected", this.#metadata());
        resolve();
      });
    });
  }

  logger(): Logger {
    return this.#logger;
  }

  /* ----------------------------------------------------------------- topics */

  #topicConfigToRdKafka(topic: ITopicConfig): NewTopic {
    if (Object.hasOwn(topic, "replicaAssignment")) {
      throw new KafkaJSNotImplemented("replicaAssignment is not yet implemented.", {
        code: ERROR_CODES.ERR__NOT_IMPLEMENTED,
      });
    }
    const config: Record<string, string> = {};
    for (const entry of topic.configEntries ?? []) config[entry.name] = entry.value;
    return {
      topic: topic.topic,
      num_partitions: topic.numPartitions ?? -1,
      replication_factor: topic.replicationFactor ?? -1,
      config,
    };
  }

  /**
   * Creates topics. Returns `true` when ALL were created; an already-existing
   * topic → `false` (not an error); other errors aggregate into a
   * KafkaJSAggregateError.
   */
  async createTopics(options: {
    topics: ITopicConfig[];
    timeout?: number;
    validateOnly?: boolean;
    waitForLeaders?: boolean;
  }): Promise<boolean> {
    const client = this.#requireConnected();
    if (Object.hasOwn(options, "validateOnly")) {
      throw new KafkaJSNotImplemented("validateOnly is not yet implemented.", {
        code: ERROR_CODES.ERR__NOT_IMPLEMENTED,
      });
    }
    if (Object.hasOwn(options, "waitForLeaders")) {
      throw new KafkaJSNotImplemented("waitForLeaders is not yet implemented.", {
        code: ERROR_CODES.ERR__NOT_IMPLEMENTED,
      });
    }

    let allTopicsCreated = true;
    const errors: KafkaJSCreateTopicError[] = [];
    await Promise.allSettled(
      options.topics
        .map((t) => this.#topicConfigToRdKafka(t))
        .map(
          (topicConfig) =>
            new Promise<void>((resolve) => {
              client.createTopic(topicConfig, options.timeout ?? 5000, (err) => {
                if (err !== null) {
                  if (err.code === ERROR_CODES.ERR_TOPIC_ALREADY_EXISTS) {
                    allTopicsCreated = false;
                    resolve();
                    return;
                  }
                  const e = fromLibrdKafkaError(err);
                  errors.push(new KafkaJSCreateTopicError(e, topicConfig.topic, e));
                }
                resolve();
              });
            }),
        ),
    );
    if (errors.length > 0) throw new KafkaJSAggregateError("Topic creation errors", errors);
    return allTopicsCreated;
  }

  async deleteTopics(options: { topics: string[]; timeout?: number }): Promise<void> {
    const client = this.#requireConnected();
    await Promise.all(
      options.topics.map(
        (topic) =>
          new Promise<void>((resolve, reject) => {
            client.deleteTopic(topic, options.timeout ?? 5000, (err) => {
              if (err !== null) reject(fromLibrdKafkaError(err));
              else resolve();
            });
          }),
      ),
    );
  }

  async listTopics(options: { timeout?: number } = {}): Promise<string[]> {
    const client = this.#requireConnected();
    return new Promise((resolve, reject) => {
      client.listTopics({ timeout: options.timeout ?? 5000 }, (err, topics) => {
        if (err !== null || topics === undefined) reject(toErr(err));
        else resolve(topics);
      });
    });
  }

  /* ----------------------------------------------------------------- groups */

  async listGroups(
    options: {
      timeout?: number;
      matchConsumerGroupStates?: number[];
      matchConsumerGroupTypes?: number[];
    } = {},
  ): Promise<ListGroupsResult> {
    const client = this.#requireConnected();
    const result = await new Promise<ListGroupsResult>((resolve, reject) => {
      client.listGroups({ timeout: options.timeout ?? 5000 }, (err, groups) => {
        if (err !== null || groups === undefined) reject(toErr(err));
        else resolve(groups);
      });
    });
    let groups = result.groups;
    const states = options.matchConsumerGroupStates;
    if (states !== undefined && states.length > 0) {
      groups = groups.filter((g) =>
        states.includes(GROUP_STATE_BY_NAME[g.state] ?? ConsumerGroupStates.UNKNOWN),
      );
    }
    const types = options.matchConsumerGroupTypes;
    if (types !== undefined && types.length > 0) {
      groups = groups.filter((g) =>
        types.includes(GROUP_TYPE_BY_NAME[g.type] ?? ConsumerGroupTypes.UNKNOWN),
      );
    }
    return { ...result, groups };
  }

  async describeGroups(
    groups: string[],
    options: { timeout?: number; includeAuthorizedOperations?: boolean } = {},
  ): Promise<{ groups: GroupDescription[] }> {
    const client = this.#requireConnected();
    const opts: AdminOperationOptions = { timeout: options.timeout ?? 5000 };
    if (options.includeAuthorizedOperations !== undefined) {
      opts.includeAuthorizedOperations = options.includeAuthorizedOperations;
    }
    const descriptions = await new Promise<GroupDescription[]>((resolve, reject) => {
      client.describeGroups(groups, opts, (err, result) => {
        if (err !== null || result === undefined) reject(toErr(err));
        else resolve(result);
      });
    });
    return { groups: descriptions };
  }

  async deleteGroups(
    groups: string[],
    options: { timeout?: number } = {},
  ): Promise<{ groupId: string; errorCode: number; error: KafkaJSError | null }[]> {
    const client = this.#requireConnected();
    return new Promise((resolve, reject) => {
      client.deleteGroups(groups, { timeout: options.timeout ?? 5000 }, (err, reports) => {
        if (err !== null || reports === undefined) {
          reject(toErr(err));
          return;
        }
        let errorsPresent = false;
        const converted = reports.map((report) => {
          const error = report.error !== null ? fromLibrdKafkaError(report.error) : null;
          if (error !== null) errorsPresent = true;
          return { groupId: report.groupId, errorCode: report.errorCode, error };
        });
        if (errorsPresent) {
          reject(new KafkaJSDeleteGroupsError("Error in DeleteGroups", converted));
          return;
        }
        resolve(converted);
      });
    });
  }

  /* ---------------------------------------------------------------- offsets */

  /**
   * A group's committed offsets (KafkaJS's `fetchOffsets`). `topics` is either
   * an array of names (filtered after fetching everything) or an array of
   * `{topic, partitions}`.
   */
  async fetchOffsets(
    options: {
      groupId?: string;
      topics?: string[] | { topic: string; partitions: number[] }[];
      timeout?: number;
      requireStableOffsets?: boolean;
      resolveOffsets?: boolean;
    } = {},
  ): Promise<FetchOffsetsTopic[]> {
    const client = this.#requireConnected();
    if (Object.hasOwn(options, "resolveOffsets")) {
      throw new KafkaJSNotImplemented("resolveOffsets is not yet implemented.", {
        code: ERROR_CODES.ERR__NOT_IMPLEMENTED,
      });
    }
    const { groupId, topics } = options;
    if (!groupId) {
      throw new KafkaJSError("groupId is required.", { code: ERROR_CODES.ERR__INVALID_ARG });
    }

    let partitions: { topic: string; partition: number }[] | null = null;
    let originalTopics: string[] | null = null;
    if (topics !== undefined && Array.isArray(topics) && topics.length > 0) {
      const first = topics[0];
      if (typeof first === "string") {
        originalTopics = topics as string[];
      } else if (
        typeof first === "object" &&
        first !== null &&
        Array.isArray((first as { partitions: unknown }).partitions)
      ) {
        partitions = (topics as { topic: string; partitions: number[] }[]).flatMap((t) =>
          t.partitions.map((partition) => ({ topic: t.topic, partition })),
        );
      } else {
        throw new KafkaJSError("Invalid topics format.", {
          code: ERROR_CODES.ERR__INVALID_ARG,
        });
      }
    }

    const opts: AdminOperationOptions = { timeout: options.timeout ?? 5000 };
    if (options.requireStableOffsets !== undefined) {
      opts.requireStableOffsets = options.requireStableOffsets;
    }

    const results = await new Promise<GroupOffsetsResult[]>((resolve, reject) => {
      client.listConsumerGroupOffsets([{ groupId, partitions }], opts, (err, result) => {
        if (err !== null || result === undefined) reject(toErr(err));
        else resolve(result);
      });
    });

    if (results.length !== 1) throw new KafkaJSError("Unexpected number of group results.");
    const groupResult = results[0];
    if (groupResult === undefined) throw new KafkaJSError("Unexpected number of group results.");
    if (groupResult.error !== null) throw fromLibrdKafkaError(groupResult.error);

    const byTopic = new Map<string, FetchOffsetsPartition[]>();
    for (const tp of groupResult.partitions) {
      const entry: FetchOffsetsPartition = {
        partition: tp.partition,
        offset: String(tp.offset),
        metadata: tp.metadata ?? null,
        leaderEpoch: tp.leaderEpoch ?? null,
        error: tp.error,
      };
      const list = byTopic.get(tp.topic);
      if (list === undefined) byTopic.set(tp.topic, [entry]);
      else list.push(entry);
    }
    let converted = [...byTopic.entries()].map(([topic, parts]) => ({
      topic,
      partitions: parts,
    }));
    if (originalTopics !== null) {
      converted = converted.filter((c) => originalTopics.includes(c.topic));
    }
    return converted;
  }

  /** Deletes records older than the given offsets in one topic (offset -1 = everything). */
  async deleteTopicRecords(options: {
    topic: string;
    partitions: { partition: number; offset: string }[];
    timeout?: number;
    operationTimeout?: number;
  }): Promise<DeleteRecordsReport[]> {
    const client = this.#requireConnected();
    if (
      !Object.hasOwn(options, "topic") ||
      !Object.hasOwn(options, "partitions") ||
      !Array.isArray(options.partitions)
    ) {
      throw new KafkaJSError(
        "Options must include 'topic' and 'partitions', and 'partitions' must be an array.",
        { code: ERROR_CODES.ERR__INVALID_ARG },
      );
    }
    const entries = options.partitions.map((p) => {
      if (p.offset === null || p.offset === undefined) {
        throw new KafkaJSError("Each partition must have a valid offset.", {
          code: ERROR_CODES.ERR__INVALID_ARG,
        });
      }
      const offset = +p.offset;
      if (Number.isNaN(offset)) {
        throw new KafkaJSError("Offset must be a valid number.", {
          code: ERROR_CODES.ERR__INVALID_ARG,
        });
      }
      return { topic: options.topic, partition: p.partition, offset };
    });

    const opts: AdminOperationOptions = { timeout: options.timeout ?? 5000 };
    if (options.operationTimeout !== undefined) opts.operationTimeout = options.operationTimeout;

    const results = await new Promise<DeleteRecordsReport[]>((resolve, reject) => {
      client.deleteRecords(entries, opts, (err, reports) => {
        if (err !== null || reports === undefined) reject(toErr(err));
        else resolve(reports);
      });
    });

    const withErrors = results.filter((r) => r.error !== null);
    if (withErrors.length > 0) {
      throw new KafkaJSDeleteTopicRecordsError({
        partitions: withErrors.map((r) => ({
          partition: r.partition,
          offset: String(r.lowWatermark),
          error: fromLibrdKafkaError(r.error as LibrdKafkaError),
        })),
      });
    }
    return results;
  }

  /* --------------------------------------------------------------- metadata */

  /** Topic metadata (describe topics; omitted `topics` → every topic). */
  async fetchTopicMetadata(
    options: {
      topics?: string[];
      timeout?: number;
      includeAuthorizedOperations?: boolean;
    } = {},
  ): Promise<
    {
      name: string;
      topicId?: string;
      isInternal: boolean;
      partitions: {
        partitionErrorCode: number;
        partitionId: number;
        leader: number;
        leaderNode: { id: number; host: string; port: number } | null;
        replicas: number[];
        replicaNodes: { id: number; host: string; port: number }[];
        isr: number[];
        isrNodes: { id: number; host: string; port: number }[];
      }[];
      authorizedOperations?: unknown;
    }[]
  > {
    const client = this.#requireConnected();
    const topics = Object.hasOwn(options, "topics")
      ? (options.topics ?? [])
      : await this.listTopics({ ...(options.timeout !== undefined ? { timeout: options.timeout } : {}) });

    const opts: AdminOperationOptions = { timeout: options.timeout ?? 5000 };
    if (options.includeAuthorizedOperations !== undefined) {
      opts.includeAuthorizedOperations = options.includeAuthorizedOperations;
    }

    const metadata = await new Promise<TopicDescription[]>((resolve, reject) => {
      client.describeTopics(topics, opts, (err, result) => {
        if (err !== null || result === undefined) reject(toErr(err));
        else resolve(result);
      });
    });

    const failed = metadata.find((t) => t.error !== null);
    if (failed !== undefined) throw fromLibrdKafkaError(failed.error as LibrdKafkaError);

    return metadata.map((topic) => ({
      name: topic.name,
      isInternal: topic.isInternal,
      partitions: topic.partitions.map((p) => ({
        partitionErrorCode: ERROR_CODES.ERR_NO_ERROR,
        partitionId: p.partition,
        leader: p.leader !== null ? p.leader.id : -1,
        leaderNode: p.leader,
        replicas: p.replicas.map((r) => r.id),
        replicaNodes: p.replicas,
        isr: p.isr.map((n) => n.id),
        isrNodes: p.isr,
      })),
    }));
  }

  /**
   * Earliest/latest offsets of every partition of a topic.
   * Internals: READ_UNCOMMITTED (default) goes through a watermark query;
   * READ_COMMITTED goes through admin ListOffsets with the isolation level
   * (M6).
   */
  async fetchTopicOffsets(
    topic: string,
    options: { timeout?: number; isolationLevel?: number } = {},
  ): Promise<TopicOffset[]> {
    this.#requireConnected();
    if (
      options.isolationLevel !== undefined &&
      options.isolationLevel !== IsolationLevel.READ_UNCOMMITTED
    ) {
      const timeout = options.timeout ?? 5000;
      const topicData = await this.fetchTopicMetadata({ topics: [topic], timeout });
      const partitionIds = topicData.flatMap((t) => t.partitions.map((p) => p.partitionId));
      // Two requests (each partition may only appear once per request):
      // all EARLIEST (low) + all LATEST (high, = the LSO under READ_COMMITTED).
      const [lows, highs] = await Promise.all([
        this.#listOffsets(partitionIds, topic, OffsetSpec.EARLIEST, options.isolationLevel, timeout),
        this.#listOffsets(partitionIds, topic, OffsetSpec.LATEST, options.isolationLevel, timeout),
      ]);
      return partitionIds.map((partitionId) => {
        const low = lows.get(partitionId);
        const high = highs.get(partitionId);
        if (low === undefined || high === undefined) {
          throw stateError(`listOffsets: partition ${partitionId} missing in result`);
        }
        const itemErr = low.error ?? high.error;
        if (itemErr !== null) throw fromLibrdKafkaError(itemErr);
        return {
          partition: partitionId,
          offset: String(high.offset),
          high: String(high.offset),
          low: String(low.offset),
        };
      });
    }
    const timeout = options.timeout ?? 5000;
    const watermarkClient = this.#watermarkClient;
    if (watermarkClient === null) throw stateError("Admin client is not connected.");

    const topicData = await this.fetchTopicMetadata({ topics: [topic], timeout });
    const partitionIds = topicData.flatMap((t) => t.partitions.map((p) => p.partitionId));

    return Promise.all(
      partitionIds.map(
        (partitionId) =>
          new Promise<TopicOffset>((resolve, reject) => {
            watermarkClient.queryWatermarkOffsets(topic, partitionId, timeout, (err, offsets) => {
              if (err !== null || offsets === undefined) {
                reject(toErr(err));
                return;
              }
              resolve({
                partition: partitionId,
                offset: String(offsets.highOffset),
                high: String(offsets.highOffset),
                low: String(offsets.lowOffset),
              });
            });
          }),
      ),
    );
  }

  /**
   * The first offset whose timestamp is >= `timestamp` for every partition of
   * the topic (admin ListOffsets, M6). Omitted `timestamp` → LATEST (matching
   * upstream). A partition with no messages after the timestamp → offset -1
   * (the broker's result passed through, as upstream does).
   */
  async fetchTopicOffsetsByTimestamp(
    topic: string,
    timestamp?: number,
    options: { timeout?: number; isolationLevel?: number } = {},
  ): Promise<{ partition: number; offset: string }[]> {
    this.#requireConnected();
    const timeout = options.timeout ?? 5000;
    const spec = timestamp === undefined ? OffsetSpec.LATEST : timestamp;
    const topicData = await this.fetchTopicMetadata({ topics: [topic], timeout });
    const partitionIds = topicData.flatMap((t) => t.partitions.map((p) => p.partitionId));
    const results = await this.#listOffsets(
      partitionIds,
      topic,
      spec,
      options.isolationLevel,
      timeout,
    );
    return partitionIds.map((partitionId) => {
      const row = results.get(partitionId);
      if (row === undefined) {
        throw stateError(`listOffsets: partition ${partitionId} missing in result`);
      }
      if (row.error !== null) throw fromLibrdKafkaError(row.error);
      return { partition: partitionId, offset: String(row.offset) };
    });
  }

  /** One `listOffsets` call with the same spec across partitions → a per-partition map. */
  #listOffsets(
    partitionIds: readonly number[],
    topic: string,
    offsetSpec: number,
    isolationLevel: number | undefined,
    timeout: number,
  ): Promise<Map<number, ListOffsetsReport>> {
    const client = this.#requireConnected();
    const opts: AdminOperationOptions = { timeout };
    if (isolationLevel !== undefined) opts.isolationLevel = isolationLevel;
    return new Promise((resolve, reject) => {
      client.listOffsets(
        partitionIds.map((partition) => ({ topic, partition, offsetSpec })),
        opts,
        (err, result) => {
          if (err !== null || result === undefined) reject(toErr(err));
          else resolve(new Map(result.map((r) => [r.partition, r])));
        },
      );
    });
  }
}
