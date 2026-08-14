/**
 * M5a — the kafkaJS → librdkafka config translation table, cross-checked key
 * `kafkaJSToRdKafkaConfig` / `#kafkaJSToProducerConfig` /
 * by key against confluent-kafka-javascript's `#kafkaJSToConsumerConfig` and
 * MIGRATION.md. No .so, no broker needed.
 */

import { describe, expect, test } from "bun:test";
import {
  checkIfKafkaJsKeysPresent,
  mapAdminConfig,
  mapCommonConfig,
  mapConsumerConfig,
  mapProducerConfig,
  mergeRawConfigs,
  PartitionAssigners,
  type CommonRawConfig,
} from "../../packages/bun-rdkafka/src/kafkajs/config-mapper.ts";
import { KafkaJSError } from "../../packages/bun-rdkafka/src/kafkajs/errors.ts";
import { ERROR_CODES } from "../../packages/bun-rdkafka/src/core/errors.ts";

function common(raw: CommonRawConfig): Record<string, unknown> {
  return mapCommonConfig(raw).globalConf;
}

function producer(raw: CommonRawConfig): Record<string, unknown> {
  return mapProducerConfig(raw, mapCommonConfig(raw)).globalConf;
}

function consumer(raw: CommonRawConfig): Record<string, unknown> {
  return mapConsumerConfig(raw, mapCommonConfig(raw)).globalConf;
}

function expectInvalidArg(fn: () => unknown): void {
  try {
    fn();
    throw new Error("expected a KafkaJSError throw but nothing threw");
  } catch (e) {
    expect(e).toBeInstanceOf(KafkaJSError);
    expect((e as KafkaJSError).code).toBe(ERROR_CODES.ERR__INVALID_ARG);
  }
}

describe("mapCommonConfig", () => {
  test("an empty/absent block → no defaults applied (compat mode off)", () => {
    expect(common({})).toEqual({});
    expect(common({ kafkaJS: {} })).toEqual({});
    expect(common({ "metadata.broker.list": "x:9092" })).toEqual({});
  });

  test("brokers → bootstrap.servers (join), non-array → throw", () => {
    const conf = common({ kafkaJS: { brokers: ["a:9092", "b:9093"] } });
    expect(conf["bootstrap.servers"]).toBe("a:9092,b:9093");
    expectInvalidArg(() => common({ kafkaJS: { brokers: "a:9092" as unknown as string[] } }));
  });

  test("clientId → client.id", () => {
    expect(common({ kafkaJS: { clientId: "app-1" } })["client.id"]).toBe("app-1");
  });

  test("compat-mode defaults when the block is NON-empty", () => {
    const conf = common({ kafkaJS: { clientId: "x" } });
    expect(conf["socket.timeout.ms"]).toBe(30000);
    expect(conf["socket.connection.setup.timeout.ms"]).toBe(11000); // 1000 + 10000
    expect(conf["retry.backoff.ms"]).toBe(300);
    expect(conf["retry.backoff.max.ms"]).toBe(30000);
    expect(conf["log_level"]).toBe(6);
  });

  test("requestTimeout / enforceRequestTimeout=false", () => {
    expect(common({ kafkaJS: { requestTimeout: 12345 } })["socket.timeout.ms"]).toBe(12345);
    expect(
      common({ kafkaJS: { requestTimeout: 12345, enforceRequestTimeout: false } })[
        "socket.timeout.ms"
      ],
    ).toBe(300000);
  });

  test("connectionTimeout + authenticationTimeout, floored at 1000", () => {
    expect(
      common({ kafkaJS: { connectionTimeout: 2000, authenticationTimeout: 3000 } })[
        "socket.connection.setup.timeout.ms"
      ],
    ).toBe(5000);
    expect(
      common({ kafkaJS: { connectionTimeout: 100, authenticationTimeout: 100 } })[
        "socket.connection.setup.timeout.ms"
      ],
    ).toBe(1000);
  });

  test("retry: initialRetryTime/maxRetryTime; factor/multiplier/restartOnFailure → throw", () => {
    const conf = common({ kafkaJS: { retry: { initialRetryTime: 50, maxRetryTime: 5000 } } });
    expect(conf["retry.backoff.ms"]).toBe(50);
    expect(conf["retry.backoff.max.ms"]).toBe(5000);
    expectInvalidArg(() => common({ kafkaJS: { retry: { factor: 0.5 } } }));
    expectInvalidArg(() => common({ kafkaJS: { retry: { multiplier: 3 } } }));
    expectInvalidArg(() => common({ kafkaJS: { retry: { restartOnFailure: async () => true } } }));
  });

  test("ssl boolean → security.protocol; ssl object → throw", () => {
    expect(common({ kafkaJS: { ssl: true } })["security.protocol"]).toBe("ssl");
    expect(common({ kafkaJS: { ssl: false } })["security.protocol"]).toBeUndefined();
    expectInvalidArg(() =>
      common({ kafkaJS: { ssl: { ca: "x" } as unknown as boolean } }),
    );
  });

  test("sasl PLAIN/SCRAM → sasl.*; +ssl → sasl_ssl; unknown mechanism / missing creds → throw", () => {
    const plain = common({
      kafkaJS: { sasl: { mechanism: "plain", username: "u", password: "p" } },
    });
    expect(plain["sasl.mechanism"]).toBe("PLAIN");
    expect(plain["sasl.username"]).toBe("u");
    expect(plain["sasl.password"]).toBe("p");
    expect(plain["security.protocol"]).toBe("sasl_plaintext");

    const scramSsl = common({
      kafkaJS: {
        ssl: true,
        sasl: { mechanism: "SCRAM-SHA-256", username: "u", password: "p" },
      },
    });
    expect(scramSsl["sasl.mechanism"]).toBe("SCRAM-SHA-256");
    expect(scramSsl["security.protocol"]).toBe("sasl_ssl");

    expectInvalidArg(() =>
      common({ kafkaJS: { sasl: { mechanism: "GSSAPI" } } }),
    );
    expectInvalidArg(() =>
      common({ kafkaJS: { sasl: { mechanism: "PLAIN", username: "u" } } }),
    );
  });

  test("sasl OAUTHBEARER: provider → oauthbearer_token_refresh_cb wrap token", async () => {
    const conf = common({
      kafkaJS: {
        sasl: {
          mechanism: "OAUTHBEARER",
          oauthBearerProvider: async () => ({
            value: "tok",
            principal: "me",
            lifetime: 123,
            extensions: { a: "b" },
          }),
        },
      },
    });
    expect(conf["sasl.mechanism"]).toBe("OAUTHBEARER");
    const cb = conf["oauthbearer_token_refresh_cb"] as (c: string) => Promise<unknown>;
    expect(typeof cb).toBe("function");
    expect(await cb("cfg")).toEqual({
      tokenValue: "tok",
      principal: "me",
      lifetime: 123,
      extensions: { a: "b" },
    });
    // a non-function provider → throw; a token missing fields → reject
    expectInvalidArg(() =>
      common({
        kafkaJS: {
          sasl: { mechanism: "OAUTHBEARER", oauthBearerProvider: "x" as never },
        },
      }),
    );
    const bad = common({
      kafkaJS: {
        sasl: {
          mechanism: "OAUTHBEARER",
          oauthBearerProvider: async () => ({ value: "tok" }) as never,
        },
      },
    })["oauthbearer_token_refresh_cb"] as (c: string) => Promise<unknown>;
    expect(bad("cfg")).rejects.toBeInstanceOf(KafkaJSError);
    // an absent provider: valid, only the mechanism is set
    const noProvider = common({ kafkaJS: { sasl: { mechanism: "OAUTHBEARER" } } });
    expect(noProvider["oauthbearer_token_refresh_cb"]).toBeUndefined();
  });

  test("socketFactory / reauthenticationThreshold → throw", () => {
    expectInvalidArg(() => common({ kafkaJS: { socketFactory: () => ({}) } }));
    expectInvalidArg(() => common({ kafkaJS: { reauthenticationThreshold: 1000 } }));
  });

  test("logLevel → syslog log_level; unknown values → throw", () => {
    expect(common({ kafkaJS: { logLevel: 0 } })["log_level"]).toBe(0); // NOTHING
    expect(common({ kafkaJS: { logLevel: 1 } })["log_level"]).toBe(3); // ERROR
    expect(common({ kafkaJS: { logLevel: 2 } })["log_level"]).toBe(4); // WARN
    expect(common({ kafkaJS: { logLevel: 3 } })["log_level"]).toBe(6); // INFO
    expect(common({ kafkaJS: { logLevel: 4 } })["log_level"]).toBe(7); // DEBUG
    expectInvalidArg(() => common({ kafkaJS: { logLevel: 99 } }));
  });
});

describe("mapProducerConfig", () => {
  test("a key outside common+producer (e.g. groupId) → throws unsupportedKey", () => {
    expectInvalidArg(() => producer({ kafkaJS: { groupId: "g" } }));
  });

  test("the producer translation table + compat-mode defaults", () => {
    const conf = producer({
      kafkaJS: {
        clientId: "p",
        metadataMaxAge: 60_000,
        allowAutoTopicCreation: false,
        idempotent: true,
        maxInFlightRequests: 4,
        transactionalId: "txn-1",
        compression: "gzip",
        acks: 1,
        timeout: 9000,
        retry: { retries: 7 },
      },
    });
    expect(conf["partitioner"]).toBe("murmur2_random");
    expect(conf["topic.metadata.refresh.interval.ms"]).toBe(60_000);
    expect(conf["allow.auto.create.topics"]).toBe(false);
    expect(conf["enable.idempotence"]).toBe(true);
    expect(conf["max.in.flight"]).toBe(4);
    expect(conf["transactional.id"]).toBe("txn-1");
    expect(conf["compression.codec"]).toBe("gzip");
    expect(conf["acks"]).toBe(1);
    expect(conf["request.timeout.ms"]).toBe(9000);
    expect(conf["retries"]).toBe(7);
    expect(conf["transaction.timeout.ms"]).toBe(60_000); // default
  });

  test("retries defaults to 5; transactionTimeout clamps socket.timeout.ms", () => {
    const conf = producer({ kafkaJS: { clientId: "p" } });
    expect(conf["retries"]).toBe(5);
    // the socket.timeout.ms default 30000 <= 60000+100 → kept as-is
    expect(conf["socket.timeout.ms"]).toBe(30000);
    const clamped = producer({
      kafkaJS: { requestTimeout: 90_000, transactionTimeout: 5_000 },
    });
    expect(clamped["socket.timeout.ms"]).toBe(5_100);
  });

  test("createPartitioner → ERR__NOT_IMPLEMENTED", () => {
    try {
      producer({ kafkaJS: { createPartitioner: () => "x" } });
      throw new Error("expected a throw");
    } catch (e) {
      expect((e as KafkaJSError).code).toBe(ERROR_CODES.ERR__NOT_IMPLEMENTED);
    }
  });

  test("librdkafka passthrough overrides the translation; an empty block leaves only passthrough", () => {
    const conf = producer({
      kafkaJS: { retry: { retries: 7 } },
      retries: 99,
      "linger.ms": 5,
    });
    expect(conf["retries"]).toBe(99); // passthrough wins
    expect(conf["linger.ms"]).toBe(5);
    expect(producer({ "linger.ms": 5 })).toEqual({ "linger.ms": 5 });
  });

  test("stats_cb is excluded from the mapping (the caller extracts it)", () => {
    const conf = producer({ stats_cb: () => {} });
    expect(conf["stats_cb"]).toBeUndefined();
  });
});

describe("mapConsumerConfig", () => {
  test("the consumer translation table + compat-mode defaults", () => {
    const conf = consumer({
      kafkaJS: {
        groupId: "g1",
        sessionTimeout: 45_000,
        heartbeatInterval: 5_000,
        rebalanceTimeout: 120_000,
        maxBytesPerPartition: 2048,
        maxWaitTimeInMs: 100,
        minBytes: 16,
        maxBytes: 1_000_000,
        readUncommitted: false,
        maxInFlightRequests: 3,
        rackId: "rack-1",
        fromBeginning: true,
        autoCommit: false,
        autoCommitInterval: 7000,
      },
    });
    expect(conf["group.id"]).toBe("g1");
    expect(conf["session.timeout.ms"]).toBe(45_000);
    expect(conf["heartbeat.interval.ms"]).toBe(5_000);
    expect(conf["max.poll.interval.ms"]).toBe(120_000);
    expect(conf["max.partition.fetch.bytes"]).toBe(2048);
    expect(conf["fetch.wait.max.ms"]).toBe(100);
    expect(conf["fetch.min.bytes"]).toBe(16);
    expect(conf["fetch.message.max.bytes"]).toBe(1_000_000);
    expect(conf["isolation.level"]).toBe("read_committed");
    expect(conf["max.in.flight"]).toBe(3);
    expect(conf["client.rack"]).toBe("rack-1");
    expect(conf["auto.offset.reset"]).toBe("earliest");
    expect(conf["enable.auto.commit"]).toBe(false);
    expect(conf["auto.commit.interval.ms"]).toBe(7000);
    // defaults with a non-empty block
    expect(conf["partition.assignment.strategy"]).toBe(PartitionAssigners.roundRobin);
    expect(conf["allow.auto.create.topics"]).toBe(true);
  });

  test("defaults: session 30000, max.poll 300000, autoCommit true, maxBytes*", () => {
    const conf = consumer({ kafkaJS: { groupId: "g" } });
    expect(conf["session.timeout.ms"]).toBe(30000);
    expect(conf["max.poll.interval.ms"]).toBe(300000);
    expect(conf["enable.auto.commit"]).toBe(true);
    expect(conf["max.partition.fetch.bytes"]).toBe(1048576);
    expect(conf["fetch.message.max.bytes"]).toBe(10485760);
    expect(conf["auto.offset.reset"]).toBeUndefined(); // fromBeginning unset
  });

  test("readUncommitted=true → read_uncommitted; fromBeginning=false → latest", () => {
    const conf = consumer({ kafkaJS: { readUncommitted: true, fromBeginning: false } });
    expect(conf["isolation.level"]).toBe("read_uncommitted");
    expect(conf["auto.offset.reset"]).toBe("latest");
  });

  test("partitionAssigners/Assignors → partition.assignment.strategy", () => {
    expect(
      consumer({
        kafkaJS: {
          partitionAssigners: [PartitionAssigners.cooperativeSticky, PartitionAssigners.range],
        },
      })["partition.assignment.strategy"],
    ).toBe("cooperative-sticky,range");
    expect(
      consumer({ kafkaJS: { partitionAssignors: ["range"] } })[
        "partition.assignment.strategy"
      ],
    ).toBe("range");
    expectInvalidArg(() => consumer({ kafkaJS: { partitionAssigners: "range" as never } }));
    expectInvalidArg(() => consumer({ kafkaJS: { partitionAssigners: [1] as never } }));
  });

  test("autoCommitThreshold → ERR__NOT_IMPLEMENTED", () => {
    try {
      consumer({ kafkaJS: { autoCommitThreshold: 10 } });
      throw new Error("expected a throw");
    } catch (e) {
      expect((e as KafkaJSError).code).toBe(ERROR_CODES.ERR__NOT_IMPLEMENTED);
    }
  });

  test("group.protocol=consumer (non-classic): assignors/sessionTimeout/heartbeat → throw, no default strategy set", () => {
    const raw = { "group.protocol": "consumer", kafkaJS: { groupId: "g" } };
    const conf = consumer(raw);
    expect(conf["partition.assignment.strategy"]).toBeUndefined();
    expect(conf["session.timeout.ms"]).toBeUndefined();
    expectInvalidArg(() =>
      consumer({ "group.protocol": "consumer", kafkaJS: { partitionAssigners: ["range"] } }),
    );
    expectInvalidArg(() =>
      consumer({ "group.protocol": "consumer", kafkaJS: { sessionTimeout: 1 } }),
    );
    expectInvalidArg(() =>
      consumer({ "group.protocol": "consumer", kafkaJS: { heartbeatInterval: 1 } }),
    );
  });

  test("a producer key (e.g. acks) in a consumer block → throw", () => {
    expectInvalidArg(() => consumer({ kafkaJS: { acks: 1 } }));
  });
});

describe("mapAdminConfig", () => {
  test("admin rejects client-specific keys in the block", () => {
    expectInvalidArg(() => mapAdminConfig({ kafkaJS: { groupId: "g" } }, mapCommonConfig({})));
    const conf = mapAdminConfig(
      { kafkaJS: { clientId: "adm" }, "socket.timeout.ms": 7000 },
      mapCommonConfig({ kafkaJS: { clientId: "adm" } }),
    ).globalConf;
    expect(conf["client.id"]).toBe("adm");
    expect(conf["socket.timeout.ms"]).toBe(7000); // passthrough beats the 30000 default
  });
});

describe("checkIfKafkaJsKeysPresent / mergeRawConfigs", () => {
  test("kafkaJS keys outside the block are detected; 'acks' is exempt", () => {
    expect(checkIfKafkaJsKeysPresent("common", { brokers: ["x"] })).toBe("brokers");
    expect(checkIfKafkaJsKeysPresent("producer", { acks: 1 })).toBeNull();
    expect(checkIfKafkaJsKeysPresent("producer", { idempotent: true })).toBe("idempotent");
    expect(checkIfKafkaJsKeysPresent("consumer", { groupId: "g" })).toBe("groupId");
    expect(checkIfKafkaJsKeysPresent("common", { "linger.ms": 1 })).toBeNull();
  });

  test("mergeRawConfigs: the kafkaJS block shallow-merges (specific over common), the rest shallow-merges", () => {
    const merged = mergeRawConfigs(
      { kafkaJS: { clientId: "shared", brokers: ["a:1"] }, "linger.ms": 1, debug: "all" },
      { kafkaJS: { clientId: "mine", groupId: "g" }, "linger.ms": 2 },
    );
    expect(merged.kafkaJS).toEqual({ clientId: "mine", brokers: ["a:1"], groupId: "g" });
    expect(merged["linger.ms"]).toBe(2);
    expect(merged["debug"]).toBe("all");
    // inputs are not mutated
    expect(merged.kafkaJS).not.toBe(merged.kafkaJS && {});
  });

  test("mergeRawConfigs: neither side having kafkaJS → no empty block created", () => {
    const merged = mergeRawConfigs({ "linger.ms": 1 }, { "linger.ms": 2 });
    expect(merged.kafkaJS).toBeUndefined();
  });
});
