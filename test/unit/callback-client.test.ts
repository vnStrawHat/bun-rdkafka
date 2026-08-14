/**
 * Callback API — the base `Client` class: connect/disconnect flow, event
 * wiring, subclass hooks. Everything runs on a FAKE native function table
 * (injected via `ClientInternalOptions.native`) — no libbunrdkafka, no broker.
 *
 * `brk_metadata` alone is faked with a REAL pointer: the JSON is written into
 * a `Uint8Array` living in JS, its address returned via `ptr()` so
 * NativeClient's `CString` can read it (the fake brk_mem_free is a no-op).
 */

import { describe, expect, test } from "bun:test";
import { ptr } from "bun:ffi";
import type { BrkNative } from "../../packages/bun-rdkafka/src/ffi/loader.ts";
import {
  BRK_CLIENT_PRODUCER,
  BRK_ERR_KAFKA_OFFSET,
  BRK_EVENT_DR,
  BRK_EVENT_ERROR,
  BRK_EVENT_LOG,
  BRK_EVENT_STATS,
  BRK_EVENT_THROTTLE,
} from "../../packages/bun-rdkafka/src/ffi/types.ts";
import {
  Client,
  type ClientMetrics,
  type Metadata,
} from "../../packages/bun-rdkafka/src/callback/client.ts";
import { ERROR_CODES, LibrdKafkaError } from "../../packages/bun-rdkafka/src/core/errors.ts";
import type { BrkEvent } from "../../packages/bun-rdkafka/src/core/batch-decoder.ts";
import {
  drPayload,
  encodeEventFrames,
  errorPayload,
  logPayload,
  statsPayload,
  throttlePayload,
} from "./helpers/c-side-encoders.ts";

type AnyFn = (...args: any[]) => any;

const META: Metadata = {
  orig_broker_id: 1,
  orig_broker_name: "localhost:9092/1",
  brokers: [{ id: 1, host: "localhost", port: 9092 }],
  topics: [
    {
      name: "änte-🦆",
      partitions: [{ id: 0, leader: 1, replicas: [1], isrs: [1] }],
    },
  ],
};

/** Holds references to allocated JSON buffers so the pointers stay valid. */
const metaKeepAlive: Uint8Array[] = [];

/** A successful fake `brk_metadata`: writes a real JSON pointer into `out[0]`. */
function metadataOk(json: string = JSON.stringify(META)): AnyFn {
  return (_h: unknown, _topic: unknown, _timeout: unknown, out: BigUint64Array) => {
    const buf = new TextEncoder().encode(`${json}\0`);
    metaKeepAlive.push(buf);
    out[0] = BigInt(ptr(buf));
    return buf.length - 1;
  };
}

interface FakeNative {
  native: BrkNative;
  calls: { name: string; args: unknown[] }[];
  names: () => string[];
}

const DEFAULTS: Record<string, number> = {
  brk_conf_new: 1,
  brk_client_new: 2,
};

function fakeNative(overrides: Record<string, AnyFn> = {}): FakeNative {
  const calls: { name: string; args: unknown[] }[] = [];
  const proxy = new Proxy(
    {},
    {
      get(_target, prop: string) {
        return (...args: unknown[]) => {
          calls.push({ name: prop, args });
          const fn = overrides[prop];
          return fn ? fn(...args) : (DEFAULTS[prop] ?? 0);
        };
      },
    },
  );
  return {
    native: proxy as unknown as BrkNative,
    calls,
    names: () => calls.map((c) => c.name),
  };
}

function makeClient(overrides: Record<string, AnyFn> = {}) {
  const fake = fakeNative({ brk_metadata: metadataOk(), ...overrides });
  const client = new Client(
    { "bootstrap.servers": "localhost:9092" },
    undefined,
    BRK_CLIENT_PRODUCER,
    { native: fake.native, onLeak: () => {} },
  );
  return { client, fake };
}

function connectAsync(client: Client): Promise<Metadata> {
  return new Promise((resolve, reject) => {
    client.connect({}, (err, metadata) => (err ? reject(err) : resolve(metadata as Metadata)));
  });
}

function disconnectAsync(client: Client): Promise<ClientMetrics | undefined> {
  return new Promise((resolve, reject) => {
    client.disconnect((err, metrics) => (err ? reject(err) : resolve(metrics)));
  });
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/* ========================================================================== */
/* connect                                                                     */
/* ========================================================================== */

describe("Client.connect", () => {
  test("happy path: emits 'ready' (info, metadata) BEFORE the callback, state READY", async () => {
    const { client, fake } = makeClient();
    const order: string[] = [];
    let readyInfo: { name: string } | undefined;
    let readyMeta: Metadata | undefined;
    client.on("ready", (info: { name: string }, metadata: Metadata) => {
      order.push("ready");
      readyInfo = info;
      readyMeta = metadata;
    });

    expect(client.isConnected()).toBe(false);
    const metadata = await new Promise<Metadata>((resolve, reject) => {
      client.connect({ timeout: 1234 }, (err, md) => {
        order.push("cb");
        err ? reject(err) : resolve(md as Metadata);
      });
    });

    expect(order).toEqual(["ready", "cb"]);
    expect(readyInfo).toEqual({ name: client.name });
    expect(client.name).toMatch(/^producer#\d+$/);
    expect(metadata).toEqual(META);
    expect(readyMeta).toEqual(META);
    expect(client.isConnected()).toBe(true);
    expect(client.connectedTime()).toBeGreaterThanOrEqual(0);

    // The native path runs in order: conf → client_new → metadata.
    const names = fake.names();
    expect(names.indexOf("brk_conf_new")).toBeLessThan(names.indexOf("brk_client_new"));
    expect(names).toContain("brk_metadata");
    // metadataOptions.timeout is passed down to brk_metadata.
    const mdCall = fake.calls.find((c) => c.name === "brk_metadata");
    expect(mdCall?.args[2]).toBe(1234);

    await disconnectAsync(client);
  });

  test("metadata failure: emits 'event.error', cb(err), and destroys the handle", async () => {
    const TIMED_OUT_RET = BRK_ERR_KAFKA_OFFSET + ERROR_CODES.ERR__TIMED_OUT;
    let failMetadata = true;
    const ok = metadataOk();
    const { client, fake } = makeClient({
      brk_metadata: (...args: unknown[]) => (failMetadata ? TIMED_OUT_RET : ok(...args)),
    });
    const emitted: LibrdKafkaError[] = [];
    client.on("event.error", (err: LibrdKafkaError) => emitted.push(err));

    const err = await connectAsync(client).catch((e: LibrdKafkaError) => e);
    expect(err).toBeInstanceOf(LibrdKafkaError);
    expect((err as LibrdKafkaError).code).toBe(ERROR_CODES.ERR__TIMED_OUT);
    expect(emitted).toHaveLength(1);
    expect(emitted[0]?.code).toBe(ERROR_CODES.ERR__TIMED_OUT);
    expect(fake.names()).toContain("brk_client_destroy");
    expect(client.isConnected()).toBe(false);

    // After a failure, connecting again from scratch is allowed on the same instance.
    failMetadata = false;
    const metadata = await connectAsync(client);
    expect(metadata).toEqual(META);
    await disconnectAsync(client);
  });

  test("connect while connected: the cb gets ERR__STATE, no new handle", async () => {
    const { client, fake } = makeClient();
    await connectAsync(client);
    const before = fake.names().filter((n) => n === "brk_client_new").length;

    const err = await new Promise<LibrdKafkaError | null>((resolve) => {
      client.connect({}, (e) => resolve(e));
    });
    expect(err?.code).toBe(ERROR_CODES.ERR__STATE);
    const after = fake.names().filter((n) => n === "brk_client_new").length;
    expect(after).toBe(before);
    await disconnectAsync(client);
  });

  test("a broken config: cb(err) + event.error emit, no synchronous blow-up", async () => {
    const fake = fakeNative({
      brk_conf_set: (_c: unknown, _k: unknown, _v: unknown, errBuf: Uint8Array) => {
        errBuf.set(new TextEncoder().encode("bad value\0"));
        return BRK_ERR_KAFKA_OFFSET + ERROR_CODES.ERR__INVALID_ARG;
      },
    });
    const client = new Client({ "linger.ms": "-5" }, undefined, BRK_CLIENT_PRODUCER, {
      native: fake.native,
      onLeak: () => {},
    });
    const emitted: LibrdKafkaError[] = [];
    client.on("event.error", (err: LibrdKafkaError) => emitted.push(err));
    const err = await connectAsync(client).catch((e: LibrdKafkaError) => e);
    expect((err as LibrdKafkaError).code).toBe(ERROR_CODES.ERR__INVALID_ARG);
    expect((err as LibrdKafkaError).message).toContain("bad value");
    expect(emitted).toHaveLength(1);
    // The conf is destroyed since client_new never ran (NativeClient handles it — just must not throw sync).
  });
});

/* ========================================================================== */
/* disconnect                                                                  */
/* ========================================================================== */

describe("Client.disconnect", () => {
  test("emits 'disconnected' with metrics before the cb; destroys exactly once", async () => {
    const { client, fake } = makeClient();
    const t0 = Date.now();
    await connectAsync(client);

    const order: string[] = [];
    let metricsFromEvent: ClientMetrics | undefined;
    client.on("disconnected", (m: ClientMetrics) => {
      order.push("disconnected");
      metricsFromEvent = m;
    });
    const metrics = await new Promise<ClientMetrics | undefined>((resolve) => {
      client.disconnect((_e, m) => {
        order.push("cb");
        resolve(m);
      });
    });

    expect(order).toEqual(["disconnected", "cb"]);
    expect(metrics).toBeDefined();
    expect(metrics?.connectionOpened).toBeGreaterThanOrEqual(t0);
    expect(metricsFromEvent).toEqual(metrics as ClientMetrics);
    expect(client.isConnected()).toBe(false);
    expect(client.connectedTime()).toBe(0);
    expect(fake.names().filter((n) => n === "brk_client_destroy")).toHaveLength(1);

    // Idempotent: the second call never touches native; the cb still fires.
    await disconnectAsync(client);
    expect(fake.names().filter((n) => n === "brk_client_destroy")).toHaveLength(1);
  });

  test("disconnect interleaving connect: the connect cb is never called", async () => {
    const { client } = makeClient();
    let connectCbCalled = false;
    client.connect({}, () => {
      connectCbCalled = true;
    });
    // teardown within the same tick, before the metadata microtask runs.
    await disconnectAsync(client);
    await sleep(5);
    expect(connectCbCalled).toBe(false);
    expect(client.isConnected()).toBe(false);
  });
});

/* ========================================================================== */
/* Wiring event + hook                                                         */
/* ========================================================================== */

describe("wiring event frame → EventEmitter", () => {
  test("ERROR/LOG/STATS/THROTTLE hit the right events; DR goes to the onEventFrame hook", async () => {
    const frames = encodeEventFrames([
      { type: BRK_EVENT_ERROR, payload: errorPayload(ERROR_CODES.ERR__ALL_BROKERS_DOWN, false, "all down") },
      { type: BRK_EVENT_LOG, payload: logPayload(6, "FAIL", "hällo log") },
      { type: BRK_EVENT_STATS, payload: statsPayload('{"tx":1}') },
      { type: BRK_EVENT_THROTTLE, payload: throttlePayload(7, 250, "broker-7") },
      {
        type: BRK_EVENT_DR,
        payload: drPayload([{ opaqueId: 42n, err: 0, partition: 0, offset: 10, timestampMs: 1 }]),
      },
    ]);
    let fed = false;

    class TestClient extends Client {
      hookEvents: BrkEvent[] = [];
      ticks = 0;
      protected override onEventFrame(event: BrkEvent): void {
        this.hookEvents.push(event);
      }
      protected override pollTick(): number {
        this.ticks++;
        return 0;
      }
    }

    const fake = fakeNative({
      brk_metadata: metadataOk(),
      brk_events_poll: (_h: unknown, buf: Uint8Array) => {
        if (fed) return 0;
        fed = true;
        buf.set(frames);
        return 5;
      },
    });
    const client = new TestClient({ "bootstrap.servers": "x" }, undefined, BRK_CLIENT_PRODUCER, {
      native: fake.native,
      onLeak: () => {},
    });

    const got: Record<string, unknown[]> = { error: [], log: [], stats: [], throttle: [] };
    client.on("event.error", (e: LibrdKafkaError) => got["error"]!.push(e));
    client.on("event.log", (l: unknown) => got["log"]!.push(l));
    client.on("event.stats", (s: unknown) => got["stats"]!.push(s));
    client.on("event.throttle", (t: unknown) => got["throttle"]!.push(t));

    await connectAsync(client);
    await sleep(20); // let the scheduler run a few rounds

    expect(got["error"]).toHaveLength(1);
    expect((got["error"]![0] as LibrdKafkaError).code).toBe(ERROR_CODES.ERR__ALL_BROKERS_DOWN);
    expect((got["error"]![0] as LibrdKafkaError).message).toContain("all down");
    expect(got["log"]![0]).toEqual({ severity: 6, fac: "FAIL", message: "hällo log" });
    expect(got["stats"]![0]).toEqual({ message: '{"tx":1}' });
    expect(got["throttle"]![0]).toEqual({ brokerName: "broker-7", brokerId: 7, throttleTime: 250 });

    // DRs skip the base's emitter and go to the subclass hook.
    expect(client.hookEvents).toHaveLength(1);
    expect(client.hookEvents[0]?.type).toBe(BRK_EVENT_DR);
    // pollTick is called on every pump round.
    expect(client.ticks).toBeGreaterThan(0);

    await disconnectAsync(client);
  });
});

/* ========================================================================== */
/* getMetadata / queryWatermarkOffsets                                         */
/* ========================================================================== */

describe("getMetadata / queryWatermarkOffsets", () => {
  test("getMetadata passes the upstream shape through; errors when not connected", async () => {
    const { client } = makeClient();
    const errBefore = await new Promise<LibrdKafkaError | null>((resolve) => {
      client.getMetadata({}, (e) => resolve(e));
    });
    expect(errBefore?.code).toBe(ERROR_CODES.ERR__STATE);

    await connectAsync(client);
    const md = await new Promise<Metadata>((resolve, reject) => {
      client.getMetadata({ topic: "änte-🦆", timeout: 500 }, (e, m) =>
        e ? reject(e) : resolve(m as Metadata),
      );
    });
    expect(md.topics[0]?.name).toBe("änte-🦆");
    await disconnectAsync(client);
  });

  test("queryWatermarkOffsets map {low,high} → {lowOffset,highOffset}", async () => {
    const { client } = makeClient({
      brk_query_watermark: (
        _h: unknown,
        _t: unknown,
        _p: unknown,
        lo: BigInt64Array,
        hi: BigInt64Array,
      ) => {
        lo[0] = 3n;
        hi[0] = 99n;
        return 0;
      },
    });
    await connectAsync(client);
    const offsets = await new Promise<{ lowOffset: number; highOffset: number }>(
      (resolve, reject) => {
        client.queryWatermarkOffsets("t", 0, 500, (e, o) =>
          e ? reject(e) : resolve(o as { lowOffset: number; highOffset: number }),
        );
      },
    );
    expect(offsets).toEqual({ lowOffset: 3, highOffset: 99 });
    await disconnectAsync(client);
  });
});
