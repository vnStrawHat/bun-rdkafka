import { describe, expect, test } from "bun:test";
import {
  ConfigBuilder,
  ConfigError,
  DEFAULT_JS_OPTIONS,
  buildConfig,
  stringifyConfigValue,
} from "../../packages/bun-rdkafka/src/core/config.ts";

const propMap = (pairs: [string, string][]) => Object.fromEntries(pairs);

describe("librdkafka property extraction", () => {
  test("keeps names as-is, stringifying values by type", () => {
    const { properties } = buildConfig({
      "metadata.broker.list": "a:9092,b:9092",
      "linger.ms": 5,
      "enable.idempotence": true,
      "enable.auto.commit": false,
      "queue.buffering.max.kbytes": 1048576n,
      "topic.metadata.refresh.interval.ms": -1,
    });
    expect(propMap(properties)).toEqual({
      "metadata.broker.list": "a:9092,b:9092",
      "linger.ms": "5",
      "enable.idempotence": "true",
      "enable.auto.commit": "false",
      "queue.buffering.max.kbytes": "1048576",
      "topic.metadata.refresh.interval.ms": "-1",
    });
  });

  test("keeps declaration order; a duplicate property lets the later one win", () => {
    const { properties } = buildConfig(
      { "client.id": "first", "linger.ms": 1 },
      { "client.id": "second" },
    );
    expect(properties.map(([k]) => k)).toEqual(["client.id", "linger.ms"]);
    expect(propMap(properties)["client.id"]).toBe("second");
  });

  test("arrays join with commas (debug, enabled_events…)", () => {
    const { properties } = buildConfig({ debug: ["broker", "topic", "msg"] });
    expect(propMap(properties)["debug"]).toBe("broker,topic,msg");
  });

  test("null/undefined are skipped", () => {
    const { properties } = buildConfig({ "client.id": null, "group.id": undefined, a: 1 });
    expect(properties).toEqual([["a", "1"]]);
  });

  test("large numbers avoid exponent notation", () => {
    expect(stringifyConfigValue("k", 1e21)).toBe("1000000000000000000000");
    expect(stringifyConfigValue("k", 1.5)).toBe("1.5");
  });

  test("unstringifiable values give a clear error", () => {
    expect(() => buildConfig({ "some.prop": { nested: true } })).toThrow(ConfigError);
    expect(() => buildConfig({ "some.prop": Number.NaN })).toThrow(ConfigError);
  });

  test("topic config is merged in (the shim routes it)", () => {
    const { properties } = buildConfig(
      { "bootstrap.servers": "x:9092" },
      { "auto.offset.reset": "earliest", acks: -1 },
    );
    expect(propMap(properties)).toEqual({
      "bootstrap.servers": "x:9092",
      "auto.offset.reset": "earliest",
      acks: "-1",
    });
  });
});

describe("option js.*", () => {
  test("defaults match design §5.2/§5.3/§5.4", () => {
    const { js } = buildConfig({});
    expect(js).toEqual(DEFAULT_JS_OPTIONS);
    expect(js.pollIdleMaxMs).toBe(50);
    expect(js.consumeBufferBytes).toBe(4 * 1024 * 1024);
    expect(js.producerMaxPending).toBe(100_000);
    expect(js.consumerZeroCopy).toBe(false);
  });

  test("accepts user values and does NOT leak them into librdkafka", () => {
    const { js, properties } = buildConfig({
      "bootstrap.servers": "x:9092",
      "js.poll.idle.max.ms": 10,
      "js.poll.interval.ms": 250,
      "js.consume.buffer.bytes": 1024,
      "js.event.buffer.bytes": 2048,
      "js.producer.max.pending": 7,
      "js.consumer.max.batch.size": 64,
      "js.consumer.zero.copy": true,
      "js.poll.worker": true,
    });
    expect(js).toEqual({
      pollIdleMaxMs: 10,
      pollIntervalMs: 250,
      consumeBufferBytes: 1024,
      eventBufferBytes: 2048,
      producerMaxPending: 7,
      consumerMaxBatchSize: 64,
      consumerZeroCopy: true,
      pollWorker: true,
    });
    expect(properties).toEqual([["bootstrap.servers", "x:9092"]]);
  });

  test("js.producer.max.pending defaults to queue.buffering.max.messages", () => {
    expect(buildConfig({ "queue.buffering.max.messages": 500 }).js.producerMaxPending).toBe(500);
    // An explicit value still wins.
    expect(
      buildConfig({
        "queue.buffering.max.messages": 500,
        "js.producer.max.pending": 9,
      }).js.producerMaxPending,
    ).toBe(9);
  });

  test("unknown js.* keys are rejected (typo protection)", () => {
    expect(() => buildConfig({ "js.pol.idle.max.ms": 10 })).toThrow(ConfigError);
    try {
      buildConfig({ "js.unknown": 1 });
    } catch (err) {
      expect((err as Error).message).toContain("js.poll.idle.max.ms");
    }
  });

  test("wrong types are rejected", () => {
    expect(() => buildConfig({ "js.poll.idle.max.ms": "10" })).toThrow(ConfigError);
    expect(() => buildConfig({ "js.consumer.zero.copy": 1 })).toThrow(ConfigError);
    expect(() => buildConfig({ "js.consume.buffer.bytes": -1 })).toThrow(ConfigError);
  });
});

describe("function property", () => {
  test("callbacks are extracted, never sent to librdkafka", () => {
    const rebalance = () => {};
    const commit = () => {};
    const oauth = () => {};
    const { callbacks, properties } = buildConfig({
      "group.id": "g1",
      rebalance_cb: rebalance,
      offset_commit_cb: commit,
      oauthbearer_token_refresh_cb: oauth,
    });
    expect(callbacks.rebalance_cb).toBe(rebalance);
    expect(callbacks.offset_commit_cb).toBe(commit);
    expect(callbacks.oauthbearer_token_refresh_cb).toBe(oauth);
    expect(properties).toEqual([["group.id", "g1"]]);
  });

  test("rebalance_cb: true merely enables the event (like node-rdkafka)", () => {
    const { callbacks, properties } = buildConfig({
      rebalance_cb: true,
      offset_commit_cb: false,
    });
    expect(callbacks.rebalance_cb).toBe(true);
    expect(callbacks.offset_commit_cb).toBeUndefined();
    expect(properties).toEqual([]);
  });

  test("unknown functions are rejected instead of silently dropped", () => {
    expect(() => buildConfig({ my_cb: () => {} })).toThrow(ConfigError);
  });

  test("a callback key with a wrongly typed value is rejected", () => {
    expect(() => buildConfig({ rebalance_cb: 42 })).toThrow(ConfigError);
    expect(() => buildConfig({ oauthbearer_token_refresh_cb: "x" })).toThrow(ConfigError);
  });
});

describe("incremental ConfigBuilder", () => {
  test("chained add()/set() then build()", () => {
    const built = new ConfigBuilder()
      .add({ "bootstrap.servers": "a:9092" })
      .add({ "js.poll.idle.max.ms": 5 })
      .set("linger.ms", 100)
      .build();
    expect(propMap(built.properties)).toEqual({
      "bootstrap.servers": "a:9092",
      "linger.ms": "100",
    });
    expect(built.js.pollIdleMaxMs).toBe(5);
  });

  test("build() returns an independent copy", () => {
    const builder = new ConfigBuilder({ a: 1 });
    const first = builder.build();
    builder.set("b", 2);
    expect(first.properties).toEqual([["a", "1"]]);
    expect(builder.build().properties).toHaveLength(2);
  });
});
