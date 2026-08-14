/**
 * NativeClient: state machine, buffer management (grow + one retry), and FFI
 * routing — verified with a FAKE native function table (injected via
 * `options.native`), so no libbunrdkafka is needed.
 */

import { describe, expect, test } from "bun:test";
import type { BrkNative } from "../../packages/bun-rdkafka/src/ffi/loader.ts";
import {
  BRK_CLIENT_CONSUMER,
  BRK_CLIENT_PRODUCER,
  BRK_ERR_BUFFER_TOO_SMALL,
  BRK_EVENT_DR,
} from "../../packages/bun-rdkafka/src/ffi/types.ts";
import { NativeClient } from "../../packages/bun-rdkafka/src/core/native-client.ts";
import { LibrdKafkaError } from "../../packages/bun-rdkafka/src/core/errors.ts";
import { encodeTpl } from "../../packages/bun-rdkafka/src/core/batch-decoder.ts";
import {
  decodeProduceBatch,
  drPayload,
  encodeEventFrames,
  encodeMessageBatch,
} from "./helpers/c-side-encoders.ts";

type AnyFn = (...args: any[]) => any;

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

const utf8 = (s: string) => new TextEncoder().encode(s);
const str = (b: Uint8Array | null) => (b === null ? "" : new TextDecoder().decode(b));

function makeClient(overrides: Record<string, AnyFn> = {}) {
  const fake = fakeNative(overrides);
  const client = new NativeClient({
    type: BRK_CLIENT_PRODUCER,
    properties: [["bootstrap.servers", "localhost:9092"]],
    native: fake.native,
    onLeak: () => {},
  });
  return { client, fake };
}

describe("state machine INIT → CONNECTING → READY → DISCONNECTING → CLOSED", () => {
  test("walks the whole lifecycle in order", () => {
    const { client, fake } = makeClient();
    expect(client.state).toBe("INIT");
    expect(client.isOpen).toBe(false);

    client.connect();
    expect(client.state).toBe("CONNECTING");
    expect(client.isOpen).toBe(true);
    expect(fake.names()).toEqual(["brk_conf_new", "brk_conf_set", "brk_client_new"]);

    client.markReady();
    expect(client.state).toBe("READY");

    client.disconnect();
    expect(client.state).toBe("CLOSED");
    expect(client.isClosed).toBe(true);
    expect(fake.names()).toContain("brk_client_destroy");
  });

  test("the conf is set in property order", () => {
    const seen: string[] = [];
    const fake = fakeNative({
      brk_conf_set: (_c: unknown, name: Uint8Array) => {
        seen.push(str(name.subarray(0, name.length - 1)));
        return 0;
      },
    });
    const client = new NativeClient({
      type: BRK_CLIENT_CONSUMER,
      properties: [
        ["bootstrap.servers", "a:9092"],
        ["group.id", "g1"],
      ],
      native: fake.native,
      onLeak: () => {},
    });
    client.connect();
    expect(seen).toEqual(["bootstrap.servers", "group.id"]);
  });

  test("FFI calls before connect throw INVALID_STATE without touching native", () => {
    const { client, fake } = makeClient();
    expect(() => client.outqLen()).toThrow(LibrdKafkaError);
    try {
      client.flush(10);
    } catch (err) {
      expect((err as LibrdKafkaError).code).toBe(-3);
      expect((err as LibrdKafkaError).origin).toBe("shim");
      expect((err as Error).message).toContain("INIT");
    }
    expect(fake.calls).toHaveLength(0);
  });

  test("FFI calls after disconnect are blocked too", () => {
    const { client } = makeClient();
    client.connect();
    client.disconnect();
    expect(() => client.outqLen()).toThrow(/CLOSED/);
  });

  test("a second connect() is rejected", () => {
    const { client } = makeClient();
    client.connect();
    expect(() => client.connect()).toThrow(LibrdKafkaError);
  });

  test("a double disconnect() destroys only once", () => {
    const { client, fake } = makeClient();
    client.connect();
    client.disconnect();
    client.disconnect();
    expect(fake.names().filter((n) => n === "brk_client_destroy")).toHaveLength(1);
  });

  test("disconnect() before connect goes straight to CLOSED", () => {
    const { client, fake } = makeClient();
    client.disconnect();
    expect(client.state).toBe("CLOSED");
    expect(fake.calls).toHaveLength(0);
  });

  test("the FinalizationRegistry is the last safety net when the user forgets disconnect()", async () => {
    const fake = fakeNative();
    const leaked: string[] = [];
    // The client only lives in this scope, then loses its references.
    (() => {
      const orphan = new NativeClient({
        type: BRK_CLIENT_PRODUCER,
        properties: [],
        native: fake.native,
        label: "OrphanProducer",
        onLeak: (label) => leaked.push(label),
      });
      orphan.connect();
    })();

    for (let i = 0; i < 20 && leaked.length === 0; i++) {
      Bun.gc(true);
      await Bun.sleep(1);
    }

    expect(leaked).toEqual(["OrphanProducer"]);
    expect(fake.names()).toContain("brk_client_destroy");
  });

  test("disconnect() unregisters the safety net (no double destroy)", async () => {
    const fake = fakeNative();
    const leaked: string[] = [];
    (() => {
      const client = new NativeClient({
        type: BRK_CLIENT_PRODUCER,
        properties: [],
        native: fake.native,
        onLeak: (label) => leaked.push(label),
      });
      client.connect();
      client.disconnect();
    })();

    for (let i = 0; i < 10; i++) {
      Bun.gc(true);
      await Bun.sleep(1);
    }

    expect(leaked).toEqual([]);
    expect(fake.names().filter((n) => n === "brk_client_destroy")).toHaveLength(1);
  });
});

describe("errors during client creation", () => {
  test("a failed conf_set → conf destroyed, INIT kept, message carrying the errstr", () => {
    const { client, fake } = makeClient({
      brk_conf_set: (_c: unknown, _n: unknown, _v: unknown, errstr: Uint8Array) => {
        errstr.set(utf8('No such configuration property: "abc"'));
        return -1186; // BRK_KAFKA_ERR(ERR__INVALID_ARG)
      },
    });
    expect(() => client.connect()).toThrow(/No such configuration property/);
    expect(client.state).toBe("INIT");
    expect(fake.names()).toContain("brk_conf_destroy");
    expect(fake.names()).not.toContain("brk_client_new");
  });

  test("client_new returning NULL → conf destroyed, error thrown with the errstr", () => {
    const { client, fake } = makeClient({
      brk_client_new: (_t: unknown, _c: unknown, errstr: Uint8Array) => {
        errstr.set(utf8("Failed to create thread"));
        return null;
      },
    });
    expect(() => client.connect()).toThrow(/Failed to create thread/);
    expect(client.state).toBe("INIT");
    expect(fake.names()).toContain("brk_conf_destroy");
  });

  test("conf_new returning NULL → a clear error", () => {
    const { client } = makeClient({ brk_conf_new: () => null });
    expect(() => client.connect()).toThrow(/brk_conf_new/);
  });
});

describe("client-type checks", () => {
  test("consumer APIs on a producer are blocked", () => {
    const { client } = makeClient();
    client.connect();
    expect(() => client.subscribe(["t"])).toThrow(/consumer/);
    expect(() => client.consumeBatch(10)).toThrow(/consumer/);
    expect(() => client.commit(null, true)).toThrow(/consumer/);
  });

  test("producer APIs on a consumer are blocked", () => {
    const fake = fakeNative();
    const client = new NativeClient({
      type: BRK_CLIENT_CONSUMER,
      properties: [],
      native: fake.native,
      onLeak: () => {},
    });
    client.connect();
    expect(() => client.flush(10)).toThrow(/producer/);
    expect(() => client.produceBatch([{ topic: "t", opaqueId: 1n }])).toThrow(/producer/);
  });
});

describe("produceBatch", () => {
  test("encodes one FFI call for the whole batch, returning per-record errors", () => {
    let payload: Uint8Array | undefined;
    const { client } = makeClient({
      brk_produce_batch: (
        _h: unknown,
        buf: Uint8Array,
        len: number,
        errOut: Int16Array,
        max: number,
      ) => {
        payload = buf.slice(0, len);
        errOut[0] = 0;
        errOut[1] = -184; // ERR__QUEUE_FULL for the second record
        return max;
      },
    });
    client.connect();

    const result = client.produceBatch([
      { topic: "a", opaqueId: 1n, value: "x" },
      { topic: "b", opaqueId: 2n, value: null },
    ]);

    expect(result.accepted).toBe(2);
    expect(Array.from(result.errors)).toEqual([0, -184]);
    const decoded = decodeProduceBatch(payload!);
    expect(decoded.map((r) => r.topic)).toEqual(["a", "b"]);
    expect(decoded[0]!.opaqueId).toBe(1n);
  });

  test("an empty batch makes no FFI call", () => {
    const { client, fake } = makeClient();
    client.connect();
    const before = fake.calls.length;
    expect(client.produceBatch([]).accepted).toBe(0);
    expect(fake.calls).toHaveLength(before);
  });

  test("a negative ret → LibrdKafkaError with the brk_last_error_string detail", () => {
    const { client } = makeClient({
      brk_produce_batch: () => -1005, // BRK_KAFKA_ERR(...)
      brk_last_error_string: (_h: unknown, buf: Uint8Array) => {
        const msg = utf8("detail from the shim");
        buf.set(msg);
        return msg.length;
      },
    });
    client.connect();
    expect(() => client.produceBatch([{ topic: "t", opaqueId: 1n }])).toThrow(
      /detail from the shim/,
    );
  });

  test("errOut grows with the batch", () => {
    const { client } = makeClient({
      brk_produce_batch: (_h: unknown, _b: unknown, _l: unknown, errOut: Int16Array, max: number) => {
        expect(errOut.length).toBeGreaterThanOrEqual(max);
        return max;
      },
    });
    client.connect();
    const records = Array.from({ length: 5000 }, (_, i) => ({
      topic: "t",
      opaqueId: BigInt(i + 1),
    }));
    expect(client.produceBatch(records).accepted).toBe(5000);
  });
});

describe("reusable buffers: growing per brk_last_required_size + one retry", () => {
  test("consumeBatch grows, retries, and decodes", () => {
    const message = encodeMessageBatch([
      {
        topicId: 0,
        partition: 1,
        offset: 10,
        timestampMs: 5,
        timestampType: 1,
        err: 0,
        key: null,
        value: "hi",
      },
    ]);
    const sizes: number[] = [];
    let attempt = 0;
    const fake = fakeNative({
      brk_consume_batch: (_h: unknown, buf: Uint8Array, cap: number) => {
        sizes.push(cap);
        attempt++;
        if (attempt === 1) return BRK_ERR_BUFFER_TOO_SMALL;
        buf.set(message);
        return 1;
      },
      brk_last_required_size: () => message.length,
      brk_topic_name: (_h: unknown, _id: unknown, buf: Uint8Array) => {
        const name = utf8("orders");
        buf.set(name);
        return name.length;
      },
    });
    const client = new NativeClient({
      type: BRK_CLIENT_CONSUMER,
      properties: [],
      native: fake.native,
      onLeak: () => {},
      js: { consumeBufferBytes: 8 },
    });
    client.connect();

    const msgs = client.consumeBatch(10);
    expect(sizes[0]).toBe(8);
    expect(sizes[1]).toBeGreaterThanOrEqual(message.length);
    expect(fake.names().filter((n) => n === "brk_consume_batch")).toHaveLength(2);
    expect(msgs).toHaveLength(1);
    expect(msgs[0]!.topic).toBe("orders");
    expect(str(msgs[0]!.value)).toBe("hi");
  });

  test("still too small after the retry → throws BRK_ERR_BUFFER_TOO_SMALL (no infinite retry)", () => {
    let attempts = 0;
    const fake = fakeNative({
      brk_events_poll: () => {
        attempts++;
        return BRK_ERR_BUFFER_TOO_SMALL;
      },
      brk_last_required_size: () => 1_000_000,
    });
    const client = new NativeClient({
      type: BRK_CLIENT_PRODUCER,
      properties: [],
      native: fake.native,
      onLeak: () => {},
      js: { eventBufferBytes: 16 },
    });
    client.connect();
    expect(() => client.pollEvents()).toThrow(/BRK_ERR_BUFFER_TOO_SMALL/);
    expect(attempts).toBe(2);
  });

  test("a grown buffer is kept for later calls", () => {
    const caps: number[] = [];
    let first = true;
    const fake = fakeNative({
      brk_events_poll: (_h: unknown, _b: unknown, cap: number) => {
        caps.push(cap);
        if (first) {
          first = false;
          return BRK_ERR_BUFFER_TOO_SMALL;
        }
        return 0;
      },
      brk_last_required_size: () => 4096,
    });
    const client = new NativeClient({
      type: BRK_CLIENT_PRODUCER,
      properties: [],
      native: fake.native,
      onLeak: () => {},
      js: { eventBufferBytes: 32 },
    });
    client.connect();
    client.pollEvents();
    client.pollEvents();
    expect(caps[0]).toBe(32);
    expect(caps[1]).toBe(4096);
    expect(caps[2]).toBe(4096); // never shrinks back
  });
});

describe("pollEvents", () => {
  test("decodes event frames returned by the shim", () => {
    const frames = encodeEventFrames([
      {
        type: BRK_EVENT_DR,
        payload: drPayload([
          { opaqueId: 5n, err: 0, partition: 0, offset: 1, timestampMs: 2 },
        ]),
      },
    ]);
    const { client } = makeClient({
      brk_events_poll: (_h: unknown, buf: Uint8Array) => {
        buf.set(frames);
        return 1;
      },
    });
    client.connect();
    const events = client.pollEvents();
    expect(events).toHaveLength(1);
    expect(events[0]!.type).toBe(BRK_EVENT_DR);
  });

  test("no events → an empty array, no decoding", () => {
    const { client } = makeClient({ brk_events_poll: () => 0 });
    client.connect();
    expect(client.pollEvents()).toEqual([]);
  });

  test("the default timeout is 0 (NFR-2: never block the event loop)", () => {
    const { client, fake } = makeClient();
    client.connect();
    client.pollEvents(64);
    const call = fake.calls.find((c) => c.name === "brk_events_poll")!;
    expect(call.args[3]).toBe(64); // max_events
    expect(call.args[4]).toBe(0); // timeout_ms
  });
});

describe("topic name interning", () => {
  test("a miss calls brk_topic_name once, then caches", () => {
    const { client, fake } = makeClient({
      brk_topic_name: (_h: unknown, _id: unknown, buf: Uint8Array) => {
        const name = utf8("注文-topic");
        buf.set(name);
        return name.length;
      },
    });
    client.connect();
    expect(client.topics.resolve(3)).toBe("注文-topic");
    expect(client.topics.resolve(3)).toBe("注文-topic");
    expect(fake.names().filter((n) => n === "brk_topic_name")).toHaveLength(1);
  });

  test("brk_topic_name uses its own buffer, never clobbering a tpl mid-decode", () => {
    // M1 regression: assignment()/position()/committed() write the tpl into
    // scratch and decode in place; decoding calls back into brk_topic_name on
    // a cache miss. If both paths shared one buffer, the topic name would
    // overwrite the very tpl being read.
    const tpl = encodeTpl([
      { topicId: 7, partition: 0, offset: 100 },
      { topicId: 7, partition: 1, offset: 200 },
      { topicId: 8, partition: 5, offset: 300 },
    ]).toBytes();
    const fake = fakeNative({
      brk_assignment: (_h: unknown, buf: Uint8Array) => {
        buf.set(tpl);
        return 3;
      },
      brk_topic_name: (_h: unknown, id: number, buf: Uint8Array) => {
        // A name longer than the tpl ⇒ guaranteed corruption if buffers were shared.
        const name = utf8(`topic-${id}-${"x".repeat(64)}`);
        buf.set(name);
        return name.length;
      },
    });
    const client = new NativeClient({
      type: BRK_CLIENT_CONSUMER,
      properties: [],
      native: fake.native,
      onLeak: () => {},
    });
    client.connect();

    expect(client.assignment()).toEqual([
      { topic: `topic-7-${"x".repeat(64)}`, topicId: 7, partition: 0, offset: 100, leaderEpoch: -1, metadata: null },
      { topic: `topic-7-${"x".repeat(64)}`, topicId: 7, partition: 1, offset: 200, leaderEpoch: -1, metadata: null },
      { topic: `topic-8-${"x".repeat(64)}`, topicId: 8, partition: 5, offset: 300, leaderEpoch: -1, metadata: null },
    ]);
    client.disconnect();
  });

  test("the intern table is cleared on disconnect", () => {
    const { client } = makeClient();
    client.connect();
    client.topics.set(1, "t");
    expect(client.topics.size).toBe(1);
    client.disconnect();
    expect(client.topics.size).toBe(0);
  });
});

describe("consumer API", () => {
  test("subscribe encode STRING LIST", () => {
    const fake = fakeNative({
      brk_subscribe: (_h: unknown, buf: Uint8Array, len: number) => {
        const view = new DataView(buf.buffer, buf.byteOffset, len);
        expect(view.getUint32(0, true)).toBe(2);
        return 0;
      },
    });
    const client = new NativeClient({
      type: BRK_CLIENT_CONSUMER,
      properties: [],
      native: fake.native,
      onLeak: () => {},
    });
    client.connect();
    client.subscribe(["a", "b"]);
    expect(fake.names()).toContain("brk_subscribe");
  });

  test("commit(null) sends a NULL tpl + the async flag", () => {
    const fake = fakeNative();
    const client = new NativeClient({
      type: BRK_CLIENT_CONSUMER,
      properties: [],
      native: fake.native,
      onLeak: () => {},
    });
    client.connect();
    client.commit(null, true);
    const call = fake.calls.find((c) => c.name === "brk_commit")!;
    expect(call.args[1]).toBeNull();
    expect(call.args[2]).toBe(0);
    expect(call.args[3]).toBe(1);
  });

  test("assign encodes the tpl and passes the mode", () => {
    const fake = fakeNative();
    const client = new NativeClient({
      type: BRK_CLIENT_CONSUMER,
      properties: [],
      native: fake.native,
      onLeak: () => {},
    });
    client.connect();
    client.assign([{ topic: "t", partition: 0, offset: 5 }], 1);
    const call = fake.calls.find((c) => c.name === "brk_assign")!;
    expect((call.args[1] as Uint8Array).length).toBe(call.args[2] as number);
    expect(call.args[3]).toBe(1);
  });
});
