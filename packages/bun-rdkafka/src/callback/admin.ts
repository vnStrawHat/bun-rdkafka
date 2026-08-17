/**
 * callback/admin.ts — AdminClient (Callback API, node-rdkafka style).
 *
 * API + semantics cross-checked against confluent-kafka-javascript's
 * `lib/admin.js`: `AdminClient.create(conf, eventHandlers?)` /
 * `createFrom(existingClient, eventHandlers?)`, err-first callback methods,
 * timeout as an optional middle parameter. Internals are free
 * (api-compat-not-internals principle): each command =
 * `brk_admin_request(op, correlation_id, req_json)` → wait for the
 * ADMIN_RESULT event carrying the same `correlation_id` (dispatched by
 * `Client._registerAdminResult`).
 *
 * Source of truth for the req/res JSON schemas: the comment at the top of
 * `native/src/shim_admin.c`.
 *
 * Two modes:
 *  - **standalone** (`create`): its own producer-type client, connects
 *    immediately; methods called before ready wait automatically (matching
 *    upstream behavior: `create()` returns a usable client).
 *  - **createFrom(existingClient)**: rides the handle of an ALREADY-connected
 *    Producer/KafkaConsumer; events (`ready`, `event.error`…) surface on the
 *    host client; the lifecycle belongs to the host — the admin's
 *    `disconnect()` does NOT touch the host client.
 *
 * librdkafka limitation (noted in the schema comment):
 * ListConsumerGroupOffsets / DeleteRecords take one element per native request
 * ⇒ multi-element calls are fanned out into multiple requests (separate
 * correlation_ids) and the results merged, preserving input order.
 */

import {
  BRK_ADMIN_CREATE_PARTITIONS,
  BRK_ADMIN_CREATE_TOPICS,
  BRK_ADMIN_DELETE_GROUPS,
  BRK_ADMIN_DELETE_RECORDS,
  BRK_ADMIN_DELETE_TOPICS,
  BRK_ADMIN_DESCRIBE_GROUPS,
  BRK_ADMIN_DESCRIBE_TOPICS,
  BRK_ADMIN_LIST_CONSUMER_GROUP_OFFSETS,
  BRK_ADMIN_LIST_GROUPS,
  BRK_ADMIN_LIST_OFFSETS,
  BRK_CLIENT_PRODUCER,
  RD_KAFKA_OFFSET_SPEC_EARLIEST,
  RD_KAFKA_OFFSET_SPEC_LATEST,
  RD_KAFKA_OFFSET_SPEC_MAX_TIMESTAMP,
  type BrkAdminOp,
} from "../ffi/types.ts";
import type { BrkAdminResultEvent } from "../core/batch-decoder.ts";
import type { ClientConfig } from "../core/config.ts";
import { ERROR_CODES, LibrdKafkaError } from "../core/errors.ts";
import {
  Client,
  type ClientEventMap,
  type ClientEvents,
  type ClientGlobalConfig,
  type ClientInternalOptions,
} from "./client.ts";

/* ========================================================================== */
/* Public types (shapes cross-checked against upstream)                        */
/* ========================================================================== */

/** Topic spec for `createTopic` — exact upstream field names. */
export interface NewTopic {
  topic: string;
  num_partitions?: number;
  replication_factor?: number;
  config?: Record<string, string | number | boolean>;
}

export interface AdminNode {
  id: number;
  host: string;
  port: number;
}

export interface GroupOverview {
  groupId: string;
  isSimpleConsumerGroup: boolean;
  state: string;
  /** Group type: "Unknown" | "Consumer" | "Classic" (M6). */
  type: string;
}

export interface ListGroupsResult {
  groups: GroupOverview[];
  errors: LibrdKafkaError[];
}

export interface MemberDescription {
  memberId: string;
  clientId: string;
  clientHost: string;
  groupInstanceId: string | null;
  assignment: { topicPartitions: { topic: string; partition: number }[] };
}

export interface GroupDescription {
  groupId: string;
  error: LibrdKafkaError | null;
  isSimpleConsumerGroup: boolean;
  partitionAssignor: string;
  state: string;
  coordinator: AdminNode | null;
  members: MemberDescription[];
}

export interface DeleteGroupsReport {
  groupId: string;
  errorCode: number;
  error: LibrdKafkaError | null;
}

export interface GroupOffsetsRequest {
  groupId: string;
  /** Omitted/`null` = every partition the group has committed. */
  partitions?: { topic: string; partition: number }[] | null;
}

export interface GroupOffsetsPartition {
  topic: string;
  partition: number;
  offset: number;
  leaderEpoch: number | null;
  metadata: string | null;
  error: LibrdKafkaError | null;
}

export interface GroupOffsetsResult {
  groupId: string;
  error: LibrdKafkaError | null;
  partitions: GroupOffsetsPartition[];
}

export interface DeleteRecordsEntry {
  topic: string;
  partition: number;
  /** Deletes every record BEFORE this offset (`-1` = up to the high watermark). */
  offset: number;
}

export interface DeleteRecordsReport {
  topic: string;
  partition: number;
  /** Low watermark after deletion. */
  lowWatermark: number;
  error: LibrdKafkaError | null;
}

/**
 * OffsetSpec for `listOffsets` — an `rd_kafka_OffsetSpec_t` value, or a
 * timestamp (ms, >= 0) asking for the first offset whose timestamp is >= it.
 */
export const OffsetSpec = Object.freeze({
  EARLIEST: RD_KAFKA_OFFSET_SPEC_EARLIEST,
  LATEST: RD_KAFKA_OFFSET_SPEC_LATEST,
  MAX_TIMESTAMP: RD_KAFKA_OFFSET_SPEC_MAX_TIMESTAMP,
});

export interface ListOffsetsEntry {
  topic: string;
  partition: number;
  /** `OffsetSpec.*` or a timestamp in ms (>= 0). */
  offsetSpec: number;
}

export interface ListOffsetsReport {
  topic: string;
  partition: number;
  /** Resulting offset; -1 when not found (e.g. a timestamp past the last message). */
  offset: number;
  /** Timestamp of the offset (ms); -1 when absent. */
  timestamp: number;
  leaderEpoch: number | null;
  error: LibrdKafkaError | null;
}

export interface TopicPartitionDescription {
  partition: number;
  leader: AdminNode | null;
  isr: AdminNode[];
  replicas: AdminNode[];
}

export interface TopicDescription {
  name: string;
  error: LibrdKafkaError | null;
  isInternal: boolean;
  partitions: TopicPartitionDescription[];
}

export interface AdminOperationOptions {
  timeout?: number;
  /** Only deleteRecords/createTopic/deleteTopic/createPartitions. */
  operationTimeout?: number;
  /** describeGroups/describeTopics — best-effort (see the schema comment). */
  includeAuthorizedOperations?: boolean;
  /** listConsumerGroupOffsets — best-effort. */
  requireStableOffsets?: boolean;
  /** listOffsets — 0 = READ_UNCOMMITTED (default), 1 = READ_COMMITTED. */
  isolationLevel?: number;
}

export type AdminCallback<T = void> = [T] extends [void]
  ? (err: LibrdKafkaError | null) => void
  : (err: LibrdKafkaError | null, result?: T) => void;

export type AdminEventHandlers = Record<string, (...args: unknown[]) => void>;

const DEFAULT_TIMEOUT_MS = 5_000;
const DEFAULT_DELETE_RECORDS_OPERATION_TIMEOUT_MS = 60_000;

/** Process-global correlation_id — never reused within a process's lifetime. */
let correlationCounter = 0n;
function nextCorrelationId(): bigint {
  correlationCounter += 1n;
  return correlationCounter;
}

/* ========================================================================== */
/* Helpers                                                                     */
/* ========================================================================== */

interface RawItemError {
  error_code?: number;
  error_string?: string;
}

function itemError(item: RawItemError, context: string): LibrdKafkaError | null {
  const code = item.error_code ?? 0;
  if (code === 0) return null;
  return LibrdKafkaError.fromKafkaCode(code, item.error_string || undefined, { context });
}

/** `(arg, timeoutOrOptions?, cb)` → `{options, cb}` (upstream pattern). */
function splitOptions<T>(
  timeoutOrOptions: number | AdminOperationOptions | T | undefined,
  cb: T | undefined,
): { options: AdminOperationOptions; cb: T } {
  if (typeof timeoutOrOptions === "function") {
    return { options: {}, cb: timeoutOrOptions as T };
  }
  const options: AdminOperationOptions =
    typeof timeoutOrOptions === "number"
      ? { timeout: timeoutOrOptions }
      : ((timeoutOrOptions as AdminOperationOptions | undefined) ?? {});
  if (typeof cb !== "function") {
    throw new TypeError("bun-rdkafka: AdminClient requires an err-first callback as the last argument");
  }
  return { options, cb };
}

/* ========================================================================== */
/* AdminClient                                                                 */
/* ========================================================================== */

/** `AdminClient.create(conf)` — librdkafka's global properties + `js.*` + the common callbacks. */
export type AdminClientConfig = ClientGlobalConfig;

/** Events of {@link AdminClient} — the client events only. */
export type AdminClientEventMap = ClientEventMap;

export class AdminClient extends Client<AdminClientEventMap> {
  /** Host client when built via `createFrom` — transport rides its handle. */
  private readonly host: Client | undefined;
  /** Waits for the standalone connect (create() is immediately usable, like upstream). */
  private readyPromise: Promise<void>;
  private disconnected = false;

  private constructor(
    globalConf: ClientConfig | undefined,
    host: Client | undefined,
    internal: ClientInternalOptions = {},
  ) {
    super(globalConf, undefined, BRK_CLIENT_PRODUCER, internal);
    this.host = host;
    this.readyPromise = Promise.resolve();
  }

  /**
   * Creates a standalone AdminClient from a librdkafka config and connects
   * immediately. `eventHandlers`: an `event name → listener` map attached
   * before connecting.
   */
  static create(
    conf: AdminClientConfig,
    eventHandlers?: AdminEventHandlers,
    internal?: ClientInternalOptions,
  ): AdminClient {
    const admin = new AdminClient(conf, undefined, internal ?? {});
    if (eventHandlers) {
      for (const [name, handler] of Object.entries(eventHandlers)) {
        admin.on(name as ClientEvents, handler as ClientEventMap[ClientEvents]);
      }
    }
    admin.readyPromise = new Promise<void>((resolve, reject) => {
      admin.connect({}, (err) => (err ? reject(err) : resolve()));
    });
    // Connect errors are surfaced by the first method call — must not go unhandled.
    admin.readyPromise.catch(() => {});
    return admin;
  }

  /**
   * An AdminClient riding an ALREADY-connected Producer/KafkaConsumer's
   * connection. Events surface on the host client; the admin's `disconnect()`
   * does not touch the host.
   */
  static createFrom(existingClient: Client, eventHandlers?: AdminEventHandlers): AdminClient {
    const admin = new AdminClient(undefined, existingClient);
    if (eventHandlers) {
      for (const [name, handler] of Object.entries(eventHandlers)) {
        existingClient.on(name as ClientEvents, handler as ClientEventMap[ClientEvents]);
      }
    }
    admin.readyPromise = existingClient.isConnected()
      ? Promise.resolve()
      : Promise.reject(
          new LibrdKafkaError(
            "bun-rdkafka: AdminClient.createFrom() requires an already-connected host client",
            { code: ERROR_CODES.ERR__STATE, origin: "local", context: "createFrom" },
          ),
        );
    admin.readyPromise.catch(() => {});
    return admin;
  }

  /** The client holding the actual handle (itself, or the host for `createFrom`). */
  private get transport(): Client {
    return this.host ?? this;
  }

  override disconnect(cb?: Parameters<Client["disconnect"]>[0]): this {
    this.disconnected = true;
    if (this.host !== undefined) {
      // the lifecycle belongs to the host client — just mark ourselves closed.
      queueMicrotask(() => cb?.(null, undefined));
      return this;
    }
    return super.disconnect(cb) as this;
  }

  /* ------------------------------------------------------------ dispatch */

  /**
   * Sends the request + waits for the matching ADMIN_RESULT. `timeoutMs` is the
   * request timeout handed to librdkafka; JS keeps an extra `2×timeout + 2s`
   * safety guard in case the event goes missing (not expected to happen).
   */
  private request(
    op: BrkAdminOp,
    req: Record<string, unknown>,
    timeoutMs: number,
  ): Promise<{ code: number; json: string }> {
    return this.readyPromise.then(
      () =>
        new Promise((resolve, reject) => {
          if (this.disconnected) {
            reject(
              new LibrdKafkaError("bun-rdkafka: AdminClient is disconnected", {
                code: ERROR_CODES.ERR__STATE,
                origin: "local",
                context: "adminRequest",
              }),
            );
            return;
          }
          const transport = this.transport;
          const correlationId = nextCorrelationId();
          const guard = setTimeout(
            () => {
              if (transport._unregisterAdminResult(correlationId)) {
                reject(
                  LibrdKafkaError.fromKafkaCode(
                    ERROR_CODES.ERR__TIMED_OUT,
                    "admin request received no result (JS-side guard)",
                    { context: "adminRequest" },
                  ),
                );
              }
            },
            2 * timeoutMs + 2_000,
          );
          guard.unref?.();
          transport._registerAdminResult(correlationId, (event: BrkAdminResultEvent) => {
            clearTimeout(guard);
            resolve({ code: event.code, json: event.json });
          });
          try {
            transport._adminRequest(op, correlationId, JSON.stringify(req));
          } catch (error) {
            clearTimeout(guard);
            transport._unregisterAdminResult(correlationId);
            reject(error);
          }
        }),
    );
  }

  /** `request()` + JSON parse + turning the top-level kafka_err into a LibrdKafkaError. */
  private async requestJson<T>(
    op: BrkAdminOp,
    req: Record<string, unknown>,
    timeoutMs: number,
    context: string,
  ): Promise<T> {
    const { code, json } = await this.request(op, req, timeoutMs);
    if (code !== 0) {
      throw LibrdKafkaError.fromKafkaCode(code, undefined, { context });
    }
    return JSON.parse(json) as T;
  }

  /** Delivers a promise's result to an err-first callback on a microtask. */
  private deliver<T>(promise: Promise<T>, cb: (err: LibrdKafkaError | null, result?: T) => void): void {
    promise.then(
      (result) => cb(null, result),
      (error: unknown) =>
        cb(
          error instanceof LibrdKafkaError
            ? error
            : new LibrdKafkaError(error instanceof Error ? error.message : String(error), {
                code: ERROR_CODES.ERR__FAIL,
                origin: "local",
              }),
        ),
    );
  }

  /* ------------------------------------------------------------- methods */

  /** `createTopic({topic, num_partitions, replication_factor, config?})`. */
  createTopic(topic: NewTopic, timeout?: number | AdminCallback, cb?: AdminCallback): void {
    const { options, cb: done } = splitOptions(timeout, cb);
    const timeoutMs = options.timeout ?? DEFAULT_TIMEOUT_MS;
    const promise = this.requestJson<{ topics: (RawItemError & { name: string })[] }>(
      BRK_ADMIN_CREATE_TOPICS,
      {
        topics: [
          {
            name: topic.topic,
            num_partitions: topic.num_partitions ?? -1,
            replication_factor: topic.replication_factor ?? -1,
            ...(topic.config !== undefined
              ? {
                  config: Object.fromEntries(
                    Object.entries(topic.config).map(([k, v]) => [k, String(v)]),
                  ),
                }
              : {}),
          },
        ],
        timeout_ms: timeoutMs,
        operation_timeout_ms: options.operationTimeout ?? timeoutMs,
      },
      timeoutMs,
      "createTopic",
    ).then((res) => {
      const err = itemError(res.topics[0] ?? {}, "createTopic");
      if (err) throw err;
    });
    this.deliver(promise, done);
  }

  deleteTopic(topic: string, timeout?: number | AdminCallback, cb?: AdminCallback): void {
    const { options, cb: done } = splitOptions(timeout, cb);
    const timeoutMs = options.timeout ?? DEFAULT_TIMEOUT_MS;
    const promise = this.requestJson<{ topics: (RawItemError & { name: string })[] }>(
      BRK_ADMIN_DELETE_TOPICS,
      {
        topics: [topic],
        timeout_ms: timeoutMs,
        operation_timeout_ms: options.operationTimeout ?? timeoutMs,
      },
      timeoutMs,
      "deleteTopic",
    ).then((res) => {
      const err = itemError(res.topics[0] ?? {}, "deleteTopic");
      if (err) throw err;
    });
    this.deliver(promise, done);
  }

  createPartitions(
    topic: string,
    totalPartitions: number,
    timeout?: number | AdminCallback,
    cb?: AdminCallback,
  ): void {
    const { options, cb: done } = splitOptions(timeout, cb);
    const timeoutMs = options.timeout ?? DEFAULT_TIMEOUT_MS;
    const promise = this.requestJson<{ topics: (RawItemError & { name: string })[] }>(
      BRK_ADMIN_CREATE_PARTITIONS,
      {
        topics: [{ name: topic, total_count: totalPartitions }],
        timeout_ms: timeoutMs,
        operation_timeout_ms: options.operationTimeout ?? timeoutMs,
      },
      timeoutMs,
      "createPartitions",
    ).then((res) => {
      const err = itemError(res.topics[0] ?? {}, "createPartitions");
      if (err) throw err;
    });
    this.deliver(promise, done);
  }

  /** The list of topic NAMES (via metadata — there is no dedicated admin op). */
  listTopics(options?: AdminOperationOptions | AdminCallback<string[]>, cb?: AdminCallback<string[]>): void {
    const { options: opts, cb: done } = splitOptions(options, cb);
    const timeoutMs = opts.timeout ?? DEFAULT_TIMEOUT_MS;
    const promise = this.readyPromise.then(
      () =>
        new Promise<string[]>((resolve, reject) => {
          this.transport.getMetadata({ timeout: timeoutMs }, (err, metadata) => {
            if (err || metadata === undefined) reject(err ?? new Error("empty metadata"));
            else resolve(metadata.topics.map((t) => t.name));
          });
        }),
    );
    this.deliver(promise, done);
  }

  listGroups(
    options?: AdminOperationOptions | AdminCallback<ListGroupsResult>,
    cb?: AdminCallback<ListGroupsResult>,
  ): void {
    const { options: opts, cb: done } = splitOptions(options, cb);
    const timeoutMs = opts.timeout ?? DEFAULT_TIMEOUT_MS;
    interface Raw {
      groups: { group_id: string; is_simple: boolean; state: string; type?: string }[];
      errors: RawItemError[];
    }
    const promise = this.requestJson<Raw>(
      BRK_ADMIN_LIST_GROUPS,
      { timeout_ms: timeoutMs },
      timeoutMs,
      "listGroups",
    ).then((res) => ({
      groups: res.groups.map((g) => ({
        groupId: g.group_id,
        isSimpleConsumerGroup: g.is_simple,
        state: g.state,
        type: g.type ?? "Unknown",
      })),
      errors: res.errors
        .map((e) => itemError(e, "listGroups"))
        .filter((e): e is LibrdKafkaError => e !== null),
    }));
    this.deliver(promise, done);
  }

  describeGroups(
    groups: readonly string[],
    options?: AdminOperationOptions | AdminCallback<GroupDescription[]>,
    cb?: AdminCallback<GroupDescription[]>,
  ): void {
    const { options: opts, cb: done } = splitOptions(options, cb);
    const timeoutMs = opts.timeout ?? DEFAULT_TIMEOUT_MS;
    interface RawMember {
      member_id: string;
      client_id: string;
      group_instance_id: string | null;
      host: string;
      assignment: { topic: string; partition: number }[];
    }
    interface RawGroup extends RawItemError {
      group_id: string;
      is_simple: boolean;
      partition_assignor: string;
      state: string;
      coordinator: AdminNode | null;
      members: RawMember[];
    }
    const promise = this.requestJson<{ groups: RawGroup[] }>(
      BRK_ADMIN_DESCRIBE_GROUPS,
      {
        groups: [...groups],
        timeout_ms: timeoutMs,
        ...(opts.includeAuthorizedOperations !== undefined
          ? { include_authorized_operations: opts.includeAuthorizedOperations }
          : {}),
      },
      timeoutMs,
      "describeGroups",
    ).then((res) =>
      res.groups.map((g) => ({
        groupId: g.group_id,
        error: itemError(g, "describeGroups"),
        isSimpleConsumerGroup: g.is_simple,
        partitionAssignor: g.partition_assignor,
        state: g.state,
        coordinator: g.coordinator,
        members: (g.members ?? []).map((m) => ({
          memberId: m.member_id,
          clientId: m.client_id,
          clientHost: m.host,
          groupInstanceId: m.group_instance_id,
          assignment: {
            topicPartitions: (m.assignment ?? []).map((tp) => ({
              topic: tp.topic,
              partition: tp.partition,
            })),
          },
        })),
      })),
    );
    this.deliver(promise, done);
  }

  deleteGroups(
    groups: readonly string[],
    options?: AdminOperationOptions | AdminCallback<DeleteGroupsReport[]>,
    cb?: AdminCallback<DeleteGroupsReport[]>,
  ): void {
    const { options: opts, cb: done } = splitOptions(options, cb);
    const timeoutMs = opts.timeout ?? DEFAULT_TIMEOUT_MS;
    const promise = this.requestJson<{ groups: (RawItemError & { group_id: string })[] }>(
      BRK_ADMIN_DELETE_GROUPS,
      { groups: [...groups], timeout_ms: timeoutMs },
      timeoutMs,
      "deleteGroups",
    ).then((res) =>
      res.groups.map((g) => ({
        groupId: g.group_id,
        errorCode: g.error_code ?? 0,
        error: itemError(g, "deleteGroups"),
      })),
    );
    this.deliver(promise, done);
  }

  /**
   * The groups' committed offsets (KafkaJS name: `fetchOffsets` — aliased in
   * M5). Fan-out: one native request per group (librdkafka limitation).
   */
  listConsumerGroupOffsets(
    requests: readonly GroupOffsetsRequest[],
    options?: AdminOperationOptions | AdminCallback<GroupOffsetsResult[]>,
    cb?: AdminCallback<GroupOffsetsResult[]>,
  ): void {
    const { options: opts, cb: done } = splitOptions(options, cb);
    const timeoutMs = opts.timeout ?? DEFAULT_TIMEOUT_MS;
    interface RawTp extends RawItemError {
      topic: string;
      partition: number;
      offset: number;
      leader_epoch: number;
      metadata: string | null;
    }
    interface RawGroup extends RawItemError {
      group_id: string;
      partitions: RawTp[];
    }
    const one = (req: GroupOffsetsRequest): Promise<GroupOffsetsResult> =>
      this.requestJson<{ groups: RawGroup[] }>(
        BRK_ADMIN_LIST_CONSUMER_GROUP_OFFSETS,
        {
          group_id: req.groupId,
          partitions:
            req.partitions === undefined || req.partitions === null
              ? null
              : req.partitions.map((p) => ({ topic: p.topic, partition: p.partition })),
          timeout_ms: timeoutMs,
          ...(opts.requireStableOffsets !== undefined
            ? { require_stable: opts.requireStableOffsets }
            : {}),
        },
        timeoutMs,
        "listConsumerGroupOffsets",
      ).then((res) => {
        const g = res.groups[0];
        if (g === undefined) {
          return { groupId: req.groupId, error: null, partitions: [] };
        }
        return {
          groupId: g.group_id,
          error: itemError(g, "listConsumerGroupOffsets"),
          partitions: (g.partitions ?? []).map((tp) => ({
            topic: tp.topic,
            partition: tp.partition,
            offset: tp.offset,
            leaderEpoch: tp.leader_epoch < 0 ? null : tp.leader_epoch,
            metadata: tp.metadata,
            error: itemError(tp, "listConsumerGroupOffsets"),
          })),
        };
      });
    this.deliver(Promise.all(requests.map(one)), done);
  }

  /**
   * Alias of {@link listConsumerGroupOffsets} under the KafkaJS name
   * `fetchOffsets` — spec §4 FR-1 lists both names for `AdminClient`. Upstream
   * only has this name on `KafkaJS.Admin`; the alias lets code written for
   * either API style run unchanged.
   */
  fetchOffsets(
    requests: readonly GroupOffsetsRequest[],
    options?: AdminOperationOptions | AdminCallback<GroupOffsetsResult[]>,
    cb?: AdminCallback<GroupOffsetsResult[]>,
  ): void {
    this.listConsumerGroupOffsets(requests, options, cb);
  }

  /**
   * Deletes records before the given offsets. Fan-out: one native request per
   * partition (librdkafka limitation); merged results preserve input order.
   */
  deleteRecords(
    entries: readonly DeleteRecordsEntry[],
    options?: AdminOperationOptions | AdminCallback<DeleteRecordsReport[]>,
    cb?: AdminCallback<DeleteRecordsReport[]>,
  ): void {
    const { options: opts, cb: done } = splitOptions(options, cb);
    const timeoutMs = opts.timeout ?? DEFAULT_TIMEOUT_MS;
    const operationTimeoutMs =
      opts.operationTimeout ?? DEFAULT_DELETE_RECORDS_OPERATION_TIMEOUT_MS;
    interface RawTp extends RawItemError {
      topic: string;
      partition: number;
      offset: number;
    }
    const one = (entry: DeleteRecordsEntry): Promise<DeleteRecordsReport> =>
      this.requestJson<{ partitions: RawTp[] }>(
        BRK_ADMIN_DELETE_RECORDS,
        {
          partitions: [{ topic: entry.topic, partition: entry.partition, offset: entry.offset }],
          timeout_ms: timeoutMs,
          operation_timeout_ms: operationTimeoutMs,
        },
        timeoutMs + operationTimeoutMs,
        "deleteRecords",
      ).then((res) => {
        const tp = res.partitions.find(
          (p) => p.topic === entry.topic && p.partition === entry.partition,
        );
        return {
          topic: entry.topic,
          partition: entry.partition,
          lowWatermark: tp?.offset ?? -1,
          error: tp ? itemError(tp, "deleteRecords") : null,
        };
      });
    this.deliver(Promise.all(entries.map(one)), done);
  }

  /**
   * Offsets per OffsetSpec (EARLIEST/LATEST/MAX_TIMESTAMP/timestamp) for each
   * partition — `rd_kafka_ListOffsets`. Each partition may appear at most ONCE
   * per call (a Kafka request limitation); for multiple specs on one partition,
   * call multiple times. Results keep the broker's ordering.
   */
  listOffsets(
    entries: readonly ListOffsetsEntry[],
    options?: AdminOperationOptions | AdminCallback<ListOffsetsReport[]>,
    cb?: AdminCallback<ListOffsetsReport[]>,
  ): void {
    const { options: opts, cb: done } = splitOptions(options, cb);
    const timeoutMs = opts.timeout ?? DEFAULT_TIMEOUT_MS;
    interface RawTp extends RawItemError {
      topic: string;
      partition: number;
      offset: number;
      timestamp: number;
      leader_epoch: number;
    }
    const promise = this.requestJson<{ partitions: RawTp[] }>(
      BRK_ADMIN_LIST_OFFSETS,
      {
        partitions: entries.map((e) => ({
          topic: e.topic,
          partition: e.partition,
          offset_spec: e.offsetSpec,
        })),
        timeout_ms: timeoutMs,
        ...(opts.isolationLevel !== undefined
          ? { isolation_level: opts.isolationLevel }
          : {}),
      },
      timeoutMs,
      "listOffsets",
    ).then((res) =>
      (res.partitions ?? []).map((tp) => ({
        topic: tp.topic,
        partition: tp.partition,
        offset: tp.offset,
        timestamp: tp.timestamp,
        leaderEpoch: tp.leader_epoch < 0 ? null : tp.leader_epoch,
        error: itemError(tp, "listOffsets"),
      })),
    );
    this.deliver(promise, done);
  }

  describeTopics(
    topics: readonly string[],
    options?: AdminOperationOptions | AdminCallback<TopicDescription[]>,
    cb?: AdminCallback<TopicDescription[]>,
  ): void {
    const { options: opts, cb: done } = splitOptions(options, cb);
    const timeoutMs = opts.timeout ?? DEFAULT_TIMEOUT_MS;
    interface RawPartition {
      partition: number;
      leader: AdminNode | null;
      isr: AdminNode[];
      replicas: AdminNode[];
    }
    interface RawTopic extends RawItemError {
      name: string;
      is_internal: boolean;
      partitions: RawPartition[];
    }
    const promise = this.requestJson<{ topics: RawTopic[] }>(
      BRK_ADMIN_DESCRIBE_TOPICS,
      {
        topics: [...topics],
        timeout_ms: timeoutMs,
        ...(opts.includeAuthorizedOperations !== undefined
          ? { include_authorized_operations: opts.includeAuthorizedOperations }
          : {}),
      },
      timeoutMs,
      "describeTopics",
    ).then((res) =>
      res.topics.map((t) => ({
        name: t.name,
        error: itemError(t, "describeTopics"),
        isInternal: t.is_internal,
        partitions: (t.partitions ?? []).map((p) => ({
          partition: p.partition,
          leader: p.leader,
          isr: p.isr ?? [],
          replicas: p.replicas ?? [],
        })),
      })),
    );
    this.deliver(promise, done);
  }
}
