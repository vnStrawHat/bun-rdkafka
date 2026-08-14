/**
 * Callback API — AdminClient + OAUTHBEARER wiring (M4). Everything runs on a
 * FAKE native function table: the fake `brk_admin_request` auto-"answers" by
 * enqueuing an ADMIN_RESULT frame for the next `brk_events_poll` — simulating
 * the real path (request → event on main_q → dispatch by correlation_id).
 */

import { describe, expect, test } from "bun:test";
import { ptr } from "bun:ffi";
import type { BrkNative } from "../../packages/bun-rdkafka/src/ffi/loader.ts";
import {
  BRK_ADMIN_CREATE_TOPICS,
  BRK_ADMIN_DELETE_RECORDS,
  BRK_ADMIN_DESCRIBE_GROUPS,
  BRK_ADMIN_LIST_CONSUMER_GROUP_OFFSETS,
  BRK_ADMIN_LIST_GROUPS,
  BRK_CLIENT_PRODUCER,
  BRK_EVENT_ADMIN_RESULT,
  BRK_EVENT_OAUTH_REFRESH,
} from "../../packages/bun-rdkafka/src/ffi/types.ts";
import { AdminClient } from "../../packages/bun-rdkafka/src/callback/admin.ts";
import { Client } from "../../packages/bun-rdkafka/src/callback/client.ts";
import { ERROR_CODES, LibrdKafkaError } from "../../packages/bun-rdkafka/src/core/errors.ts";
import { adminResultPayload, encodeEventFrames, oauthRefreshPayload } from "./helpers/c-side-encoders.ts";

type AnyFn = (...args: any[]) => any;

const DECODER = new TextDecoder();

/** Reads a NUL-terminated string from a Uint8Array (the fake's cstring args). */
function cstr(buf: unknown): string {
  const bytes = buf as Uint8Array;
  const end = bytes.indexOf(0);
  return DECODER.decode(end < 0 ? bytes : bytes.subarray(0, end));
}

const META_JSON = JSON.stringify({
  orig_broker_id: 1,
  orig_broker_name: "localhost:9092/1",
  brokers: [{ id: 1, host: "localhost", port: 9092 }],
  topics: [
    { name: "m4-a", partitions: [] },
    { name: "m4-b", partitions: [] },
  ],
});

const keepAlive: Uint8Array[] = [];

interface AdminRequestRecord {
  op: number;
  correlationId: bigint;
  req: any;
}

/**
 * A fake native with a self-answering "admin broker": `respond(op, corr, req)`
 * returns `{code?, json}` (or raw frames for ordering tests); frames are
 * enqueued and drained on the next `brk_events_poll`.
 */
function adminFake(
  respond: (op: number, corr: bigint, req: any) => { code?: number; json: string } | null,
) {
  const calls: { name: string; args: unknown[] }[] = [];
  const requests: AdminRequestRecord[] = [];
  const frameQueue: Uint8Array[] = [];

  const enqueueAdminResult = (corr: bigint, code: number, json: string) => {
    frameQueue.push(
      encodeEventFrames([
        { type: BRK_EVENT_ADMIN_RESULT, payload: adminResultPayload(corr, code, json) },
      ]),
    );
  };

  const overrides: Record<string, AnyFn> = {
    brk_conf_new: () => 1,
    brk_client_new: () => 2,
    brk_metadata: (_h: unknown, _t: unknown, _ms: unknown, out: BigUint64Array) => {
      const buf = new TextEncoder().encode(`${META_JSON}\0`);
      keepAlive.push(buf);
      out[0] = BigInt(ptr(buf));
      return buf.length - 1;
    },
    brk_admin_request: (_h: unknown, op: number, corr: bigint, reqBuf: unknown) => {
      const req = JSON.parse(cstr(reqBuf));
      requests.push({ op, correlationId: corr, req });
      const answer = respond(op, corr, req);
      if (answer !== null) enqueueAdminResult(corr, answer.code ?? 0, answer.json);
      return 0;
    },
    brk_events_poll: (_h: unknown, buf: Uint8Array) => {
      if (frameQueue.length === 0) return 0;
      let offset = 0;
      let count = 0;
      while (frameQueue.length > 0 && offset + frameQueue[0]!.length <= buf.length) {
        const frame = frameQueue.shift()!;
        buf.set(frame, offset);
        offset += frame.length;
        count++;
      }
      return count;
    },
  };

  const proxy = new Proxy(
    {},
    {
      get(_target, prop: string) {
        return (...args: unknown[]) => {
          calls.push({ name: prop, args });
          const fn = overrides[prop];
          return fn ? fn(...args) : 0;
        };
      },
    },
  );

  return {
    native: proxy as unknown as BrkNative,
    calls,
    requests,
    enqueueAdminResult,
    enqueueFrame: (frame: Uint8Array) => frameQueue.push(frame),
    names: () => calls.map((c) => c.name),
  };
}

function promisify<T>(run: (cb: (err: LibrdKafkaError | null, result?: T) => void) => void): Promise<T> {
  return new Promise((resolve, reject) => {
    run((err, result) => (err ? reject(err) : resolve(result as T)));
  });
}

/* ========================================================================== */
/* AdminClient                                                                 */
/* ========================================================================== */

describe("AdminClient (standalone)", () => {
  test("createTopic: the right op, the right request shape, cb(null) on success", async () => {
    const fake = adminFake((op, _corr, req) => {
      expect(op).toBe(BRK_ADMIN_CREATE_TOPICS);
      expect(req.topics).toEqual([
        {
          name: "m4-new",
          num_partitions: 3,
          replication_factor: 1,
          config: { "cleanup.policy": "compact" },
        },
      ]);
      expect(req.timeout_ms).toBe(1234);
      expect(req.operation_timeout_ms).toBe(1234);
      return { json: JSON.stringify({ topics: [{ name: "m4-new", error_code: 0 }] }) };
    });
    const admin = AdminClient.create({ "bootstrap.servers": "x" }, undefined, {
      native: fake.native,
      onLeak: () => {},
    });
    await promisify<void>((cb) =>
      admin.createTopic(
        {
          topic: "m4-new",
          num_partitions: 3,
          replication_factor: 1,
          config: { "cleanup.policy": "compact" },
        },
        1234,
        cb as (err: LibrdKafkaError | null) => void,
      ),
    );
    expect(fake.requests).toHaveLength(1);
    admin.disconnect();
  });

  test("a per-item error → cb(LibrdKafkaError with code + message)", async () => {
    const fake = adminFake(() => ({
      json: JSON.stringify({
        topics: [{ name: "m4-dup", error_code: 36, error_string: "Topic already exists" }],
      }),
    }));
    const admin = AdminClient.create({ "bootstrap.servers": "x" }, undefined, {
      native: fake.native,
      onLeak: () => {},
    });
    const err = await promisify<void>((cb) =>
      admin.createTopic({ topic: "m4-dup" }, cb as (e: LibrdKafkaError | null) => void),
    ).catch((e: LibrdKafkaError) => e);
    expect(err).toBeInstanceOf(LibrdKafkaError);
    expect((err as LibrdKafkaError).code).toBe(36); // TOPIC_ALREADY_EXISTS
    expect((err as LibrdKafkaError).message).toContain("already exists");
    admin.disconnect();
  });

  test("ADMIN_RESULT's top-level kafka_err → cb(err) with the right code", async () => {
    const fake = adminFake(() => ({ code: ERROR_CODES.ERR__TIMED_OUT, json: "{}" }));
    const admin = AdminClient.create({ "bootstrap.servers": "x" }, undefined, {
      native: fake.native,
      onLeak: () => {},
    });
    const err = await promisify<void>((cb) =>
      admin.deleteTopic("m4-x", cb as (e: LibrdKafkaError | null) => void),
    ).catch((e: LibrdKafkaError) => e);
    expect((err as LibrdKafkaError).code).toBe(ERROR_CODES.ERR__TIMED_OUT);
    admin.disconnect();
  });

  test("listTopics goes through metadata; listGroups maps the upstream shape", async () => {
    const fake = adminFake((op) => {
      expect(op).toBe(BRK_ADMIN_LIST_GROUPS);
      return {
        json: JSON.stringify({
          groups: [{ group_id: "g1", is_simple: false, state: "Stable", type: "Classic" }],
          errors: [{ error_code: ERROR_CODES.ERR__TIMED_OUT, error_string: "a slow broker" }],
        }),
      };
    });
    const admin = AdminClient.create({ "bootstrap.servers": "x" }, undefined, {
      native: fake.native,
      onLeak: () => {},
    });
    const topics = await promisify<string[]>((cb) => admin.listTopics(cb));
    expect(topics).toEqual(["m4-a", "m4-b"]);

    const groups = await promisify<any>((cb) => admin.listGroups(cb));
    expect(groups.groups).toEqual([
      { groupId: "g1", isSimpleConsumerGroup: false, state: "Stable", type: "Classic" },
    ]);
    expect(groups.errors).toHaveLength(1);
    expect(groups.errors[0]).toBeInstanceOf(LibrdKafkaError);
    admin.disconnect();
  });

  test("describeGroups: map members/coordinator/assignment", async () => {
    const fake = adminFake((op) => {
      expect(op).toBe(BRK_ADMIN_DESCRIBE_GROUPS);
      return {
        json: JSON.stringify({
          groups: [
            {
              group_id: "g1",
              error_code: 0,
              is_simple: false,
              partition_assignor: "range",
              state: "Stable",
              coordinator: { id: 1, host: "localhost", port: 9092 },
              members: [
                {
                  member_id: "m-1",
                  client_id: "c-1",
                  group_instance_id: null,
                  host: "/10.0.0.5",
                  assignment: [{ topic: "t", partition: 0 }],
                },
              ],
            },
          ],
        }),
      };
    });
    const admin = AdminClient.create({ "bootstrap.servers": "x" }, undefined, {
      native: fake.native,
      onLeak: () => {},
    });
    const groups = await promisify<any>((cb) => admin.describeGroups(["g1"], cb));
    expect(groups).toEqual([
      {
        groupId: "g1",
        error: null,
        isSimpleConsumerGroup: false,
        partitionAssignor: "range",
        state: "Stable",
        coordinator: { id: 1, host: "localhost", port: 9092 },
        members: [
          {
            memberId: "m-1",
            clientId: "c-1",
            clientHost: "/10.0.0.5",
            groupInstanceId: null,
            assignment: { topicPartitions: [{ topic: "t", partition: 0 }] },
          },
        ],
      },
    ]);
    admin.disconnect();
  });

  test("fan-out: listConsumerGroupOffsets with 2 groups = 2 requests, merged in order", async () => {
    const fake = adminFake((op, _corr, req) => {
      expect(op).toBe(BRK_ADMIN_LIST_CONSUMER_GROUP_OFFSETS);
      const gid = req.group_id as string;
      return {
        json: JSON.stringify({
          groups: [
            {
              group_id: gid,
              error_code: 0,
              partitions: [
                {
                  topic: "t",
                  partition: 0,
                  offset: gid === "g1" ? 10 : 20,
                  leader_epoch: -1,
                  metadata: null,
                  error_code: 0,
                },
              ],
            },
          ],
        }),
      };
    });
    const admin = AdminClient.create({ "bootstrap.servers": "x" }, undefined, {
      native: fake.native,
      onLeak: () => {},
    });
    const res = await promisify<any>((cb) =>
      admin.listConsumerGroupOffsets([{ groupId: "g1" }, { groupId: "g2", partitions: null }], cb),
    );
    expect(fake.requests.filter((r) => r.op === BRK_ADMIN_LIST_CONSUMER_GROUP_OFFSETS)).toHaveLength(2);
    expect(res.map((r: any) => r.groupId)).toEqual(["g1", "g2"]);
    expect(res[0].partitions[0]).toEqual({
      topic: "t",
      partition: 0,
      offset: 10,
      leaderEpoch: null,
      metadata: null,
      error: null,
    });
    admin.disconnect();
  });

  test("deleteRecords fan-out: results keep input order despite shuffled replies", async () => {
    // Answer partition 1 BEFORE partition 0 (delayed via enqueue order).
    const pending: { corr: bigint; entry: any }[] = [];
    const fake = adminFake((op, corr, req) => {
      expect(op).toBe(BRK_ADMIN_DELETE_RECORDS);
      pending.push({ corr, entry: req.partitions[0] });
      if (pending.length === 2) {
        // enqueue in reverse order
        for (const p of [...pending].reverse()) {
          fake.enqueueAdminResult(
            p.corr,
            0,
            JSON.stringify({
              partitions: [
                {
                  topic: p.entry.topic,
                  partition: p.entry.partition,
                  offset: 100 + p.entry.partition,
                  error_code: 0,
                },
              ],
            }),
          );
        }
      }
      return null; // enqueued manually above
    });
    const admin = AdminClient.create({ "bootstrap.servers": "x" }, undefined, {
      native: fake.native,
      onLeak: () => {},
    });
    const res = await promisify<any>((cb) =>
      admin.deleteRecords(
        [
          { topic: "t", partition: 0, offset: 5 },
          { topic: "t", partition: 1, offset: 5 },
        ],
        cb,
      ),
    );
    expect(res).toEqual([
      { topic: "t", partition: 0, lowWatermark: 100, error: null },
      { topic: "t", partition: 1, lowWatermark: 101, error: null },
    ]);
    admin.disconnect();
  });

  test("disconnect with pending requests → cb(err ERR__DESTROY), no hang", async () => {
    const fake = adminFake(() => null); // never answers
    const admin = AdminClient.create({ "bootstrap.servers": "x" }, undefined, {
      native: fake.native,
      onLeak: () => {},
    });
    const errPromise = promisify<void>((cb) =>
      admin.deleteTopic("m4-x", cb as (e: LibrdKafkaError | null) => void),
    ).catch((e: LibrdKafkaError) => e);
    // wait for the request to be sent before disconnecting
    await new Promise((r) => setTimeout(r, 20));
    expect(fake.requests).toHaveLength(1);
    admin.disconnect();
    const err = await errPromise;
    expect((err as LibrdKafkaError).code).toBe(ERROR_CODES.ERR__DESTROY);
  });
});

describe("AdminClient.createFrom", () => {
  test("rides the host client's handle; the admin's disconnect() never destroys the host", async () => {
    const fake = adminFake(() => ({
      json: JSON.stringify({ topics: [{ name: "m4-from", error_code: 0 }] }),
    }));
    const host = new Client({ "bootstrap.servers": "x" }, undefined, BRK_CLIENT_PRODUCER, {
      native: fake.native,
      onLeak: () => {},
    });
    await new Promise<void>((resolve, reject) =>
      host.connect({}, (e) => (e ? reject(e) : resolve())),
    );

    const admin = AdminClient.createFrom(host);
    await promisify<void>((cb) =>
      admin.createTopic({ topic: "m4-from" }, cb as (e: LibrdKafkaError | null) => void),
    );
    expect(fake.requests).toHaveLength(1);

    const destroysBefore = fake.names().filter((n) => n === "brk_client_destroy").length;
    admin.disconnect();
    await new Promise((r) => setTimeout(r, 10));
    expect(fake.names().filter((n) => n === "brk_client_destroy").length).toBe(destroysBefore);

    // the host client remains usable
    await promisify<string[]>((cb) => AdminClient.createFrom(host).listTopics(cb));
    await new Promise<void>((resolve) => {
      host.disconnect(() => resolve());
    });
  });

  test("createFrom with an unconnected client → every method returns a STATE error", async () => {
    const fake = adminFake(() => null);
    const host = new Client({ "bootstrap.servers": "x" }, undefined, BRK_CLIENT_PRODUCER, {
      native: fake.native,
      onLeak: () => {},
    });
    const admin = AdminClient.createFrom(host);
    const err = await promisify<void>((cb) =>
      admin.deleteTopic("x", cb as (e: LibrdKafkaError | null) => void),
    ).catch((e: LibrdKafkaError) => e);
    expect((err as LibrdKafkaError).code).toBe(ERROR_CODES.ERR__STATE);
  });
});

/* ========================================================================== */
/* OAUTHBEARER wiring                                                          */
/* ========================================================================== */

describe("OAUTHBEARER token refresh wiring", () => {
  function oauthFrame(config: string): Uint8Array {
    return encodeEventFrames([
      { type: BRK_EVENT_OAUTH_REFRESH, payload: oauthRefreshPayload(config) },
    ]);
  }

  function makeOauthClient(
    refreshCb: unknown,
    onSetToken?: (args: unknown[]) => void,
    onSetFailure?: (args: unknown[]) => void,
  ) {
    let fed = false;
    const calls: { name: string; args: unknown[] }[] = [];
    const overrides: Record<string, AnyFn> = {
      brk_conf_new: () => 1,
      brk_client_new: () => 2,
      brk_metadata: (_h: unknown, _t: unknown, _ms: unknown, out: BigUint64Array) => {
        const buf = new TextEncoder().encode(`${META_JSON}\0`);
        keepAlive.push(buf);
        out[0] = BigInt(ptr(buf));
        return buf.length - 1;
      },
      brk_events_poll: (_h: unknown, buf: Uint8Array) => {
        if (fed) return 0;
        fed = true;
        buf.set(oauthFrame("principal=admin scope=test"));
        return 1;
      },
      brk_oauthbearer_set_token: (...args: unknown[]) => {
        onSetToken?.(args);
        return 0;
      },
      brk_oauthbearer_set_token_failure: (...args: unknown[]) => {
        onSetFailure?.(args);
        return 0;
      },
    };
    const proxy = new Proxy(
      {},
      {
        get(_t, prop: string) {
          return (...args: unknown[]) => {
            calls.push({ name: prop, args });
            const fn = overrides[prop];
            return fn ? fn(...args) : 0;
          };
        },
      },
    );
    const conf: Record<string, unknown> = { "bootstrap.servers": "x" };
    if (refreshCb !== undefined) conf["oauthbearer_token_refresh_cb"] = refreshCb;
    const client = new Client(conf as any, undefined, BRK_CLIENT_PRODUCER, {
      native: proxy as unknown as BrkNative,
      onLeak: () => {},
    });
    return { client, calls };
  }

  async function connected(client: Client): Promise<void> {
    await new Promise<void>((resolve, reject) =>
      client.connect({}, (e) => (e ? reject(e) : resolve())),
    );
  }

  test("err-first cb: receives the oauthbearer_config; the token reaches FFI field for field", async () => {
    let receivedConfig: string | undefined;
    const setTokenArgs: unknown[][] = [];
    const { client } = makeOauthClient(
      (config: string, done: (err: Error | null, token?: unknown) => void) => {
        receivedConfig = config;
        done(null, {
          tokenValue: "jwt-token-here",
          lifetime: 1_755_000_000_000,
          principal: "admin",
          extensions: { traceId: "t-1" },
        });
      },
      (args) => setTokenArgs.push(args),
    );
    await connected(client);
    await new Promise((r) => setTimeout(r, 30));

    expect(receivedConfig).toBe("principal=admin scope=test");
    expect(setTokenArgs).toHaveLength(1);
    const [, tokenBuf, lifetime, principalBuf] = setTokenArgs[0]!;
    expect(cstr(tokenBuf)).toBe("jwt-token-here");
    expect(Number(lifetime)).toBe(1_755_000_000_000);
    expect(cstr(principalBuf)).toBe("admin");
    await new Promise<void>((r) => {
      client.disconnect(() => r());
    });
  });

  test("a Promise-returning cb (KafkaJS style) works too", async () => {
    const setTokenArgs: unknown[][] = [];
    const { client } = makeOauthClient(
      async (_config: string) => ({ tokenValue: "async-token" }),
      (args) => setTokenArgs.push(args),
    );
    await connected(client);
    await new Promise((r) => setTimeout(r, 30));
    expect(setTokenArgs).toHaveLength(1);
    expect(cstr(setTokenArgs[0]![1])).toBe("async-token");
    await new Promise<void>((r) => {
      client.disconnect(() => r());
    });
  });

  test("a failing cb → set_token_failure + event.error", async () => {
    const failures: unknown[][] = [];
    const { client } = makeOauthClient(
      (_config: string, done: (err: Error | null) => void) => done(new Error("OIDC crashed")),
      undefined,
      (args) => failures.push(args),
    );
    const errors: LibrdKafkaError[] = [];
    client.on("event.error", (e: LibrdKafkaError) => errors.push(e));
    await connected(client);
    await new Promise((r) => setTimeout(r, 30));
    expect(failures).toHaveLength(1);
    expect(cstr(failures[0]![1])).toContain("OIDC crashed");
    expect(errors.some((e) => e.message.includes("OIDC crashed"))).toBe(true);
    await new Promise<void>((r) => {
      client.disconnect(() => r());
    });
  });

  test("no cb → a clear AUTHENTICATION event.error", async () => {
    const { client } = makeOauthClient(undefined);
    const errors: LibrdKafkaError[] = [];
    client.on("event.error", (e: LibrdKafkaError) => errors.push(e));
    await connected(client);
    await new Promise((r) => setTimeout(r, 30));
    expect(errors).toHaveLength(1);
    expect(errors[0]!.code).toBe(ERROR_CODES.ERR__AUTHENTICATION);
    expect(errors[0]!.message).toContain("oauthbearer_token_refresh_cb");
    await new Promise<void>((r) => {
      client.disconnect(() => r());
    });
  });
});
