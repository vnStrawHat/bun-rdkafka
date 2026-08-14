/**
 * test/integration/security.integration.test.ts — M4 security against a real
 * broker: SASL/PLAIN (roundtrip + a wrong password), SCRAM-SHA-256, and the
 * OAUTHBEARER token-refresh flow (client-side; the broker need not accept the
 * mechanism — the point is proving refresh_cb → set_token travels the whole
 * FFI path).
 *
 * The SASL broker is its OWN container (bun-rdkafka-test-kafka-sasl, port
 * 9094) — stopped in afterAll, never touching the shared PLAINTEXT broker.
 */

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { Buffer } from "node:buffer";
import {
  SASL_CREDENTIALS,
  integrationAvailable,
  startKafkaSasl,
  stopKafkaSasl,
} from "./docker-kafka.ts";
import { SliceProducer, waitFor } from "./slice-harness.ts";
import { Producer } from "../../packages/bun-rdkafka/src/callback/producer.ts";
import { Kafka } from "../../packages/bun-rdkafka/src/kafkajs/kafka.ts";
import {
  KafkaConsumer,
  type Message,
} from "../../packages/bun-rdkafka/src/callback/kafka-consumer.ts";
import type { LibrdKafkaError } from "../../packages/bun-rdkafka/src/core/errors.ts";
import type { OauthBearerToken } from "../../packages/bun-rdkafka/src/callback/client.ts";

const AVAILABLE = await integrationAvailable();
const RUN_ID = Date.now().toString(36);

let brokers = "localhost:9094";

function saslConfig(extra: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    "bootstrap.servers": brokers,
    "security.protocol": "sasl_plaintext",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": SASL_CREDENTIALS.username,
    "sasl.password": SASL_CREDENTIALS.password,
    ...extra,
  };
}

function connectAsync(client: Producer | KafkaConsumer, timeout = 15_000): Promise<void> {
  return new Promise((resolve, reject) => {
    client.connect({ timeout }, (err) => (err ? reject(err) : resolve()));
  });
}

function disconnectAsync(client: Producer | KafkaConsumer): Promise<void> {
  return new Promise((resolve) => {
    client.disconnect(() => resolve());
  });
}

/** Produces `count` messages then flushes — using the real Producer (M2). */
async function produceOver(
  config: Record<string, unknown>,
  topic: string,
  count: number,
): Promise<void> {
  const producer = new Producer({ ...config, "linger.ms": 5 });
  await connectAsync(producer);
  for (let i = 0; i < count; i++) {
    producer.produce(topic, null, Buffer.from(`sec-${i}`), `k${i}`);
  }
  await new Promise<void>((resolve, reject) => {
    producer.flush(10_000, (err) => (err ? reject(err) : resolve()));
  });
  await disconnectAsync(producer);
}

/** Consumes `count` messages from the topic — the real KafkaConsumer (M3). */
async function consumeOver(
  config: Record<string, unknown>,
  topic: string,
  groupId: string,
  count: number,
): Promise<Message[]> {
  const consumer = new KafkaConsumer({
    ...config,
    "group.id": groupId,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": false,
    "fetch.wait.max.ms": 10,
    "fetch.queue.backoff.ms": 10,
  });
  await connectAsync(consumer);
  const got: Message[] = [];
  consumer.on("data", (m: Message) => got.push(m));
  consumer.subscribe([topic]);
  consumer.consume();
  await waitFor(() => got.length >= count, 30_000, () => `all ${count} messages over SASL (got ${got.length})`);
  await disconnectAsync(consumer);
  return got;
}

describe.skipIf(!AVAILABLE)("M4 security (dedicated SASL broker)", () => {
  beforeAll(async () => {
    const kafka = await startKafkaSasl();
    brokers = kafka.brokers;
  }, 240_000);

  afterAll(() => stopKafkaSasl());

  test("SASL/PLAIN: produce + consume roundtrip", async () => {
    const topic = `m4-sec-plain-${RUN_ID}`;
    await produceOver(saslConfig(), topic, 5);
    const got = await consumeOver(saslConfig(), topic, `m4-sec-g-plain-${RUN_ID}`, 5);
    expect(got.length).toBeGreaterThanOrEqual(5);
    expect(got.map((m) => m.value?.toString())).toContain("sec-0");
    expect(got.map((m) => m.value?.toString())).toContain("sec-4");
  }, 120_000);

  test("SASL/PLAIN with a wrong password: a clear authentication error event, no crash", async () => {
    // The Callback API connect() blocks in metadata (cold path) then tears down when it
    // fails ⇒ no chance to poll error events. Use slice-harness (connect does
    // not wait for metadata) to observe ERROR events while auth fails.
    const producer = new SliceProducer({
      config: saslConfig({ "sasl.password": "wrong-pass" }),
    });
    producer.connect();
    await waitFor(
      () => producer.errorEvents.some((e) => /authenticat|SASL/i.test(e.reason)),
      15_000,
      () =>
        `an ERROR event spelling out the authentication failure (got: ${producer.errorEvents
          .map((e) => e.reason)
          .join(" | ")})`,
    );
    producer.disconnect();

    // And the Callback API connect() must fail cleanly (no hang, no crash).
    const cbProducer = new Producer(saslConfig({ "sasl.password": "wrong-pass" }));
    const err = await connectAsync(cbProducer, 6_000).catch((e: LibrdKafkaError) => e);
    expect(err).toBeTruthy();
    await disconnectAsync(cbProducer);
  }, 60_000);

  test("SASL/SCRAM-SHA-256: produce + consume roundtrip", async () => {
    const scram = saslConfig({ "sasl.mechanisms": "SCRAM-SHA-256" });
    const topic = `m4-sec-scram-${RUN_ID}`;
    await produceOver(scram, topic, 5);
    const got = await consumeOver(scram, topic, `m4-sec-g-scram-${RUN_ID}`, 5);
    expect(got.length).toBeGreaterThanOrEqual(5);
  }, 120_000);

  test("OAUTHBEARER: refresh_cb invoked with the oauthbearer_config, set_token OK over FFI", async () => {
    // An unsecured JWT (alg=none) — the broker need not accept it; the test's aim is
    // flow client-side: event OAUTH_REFRESH → cb → brk_oauthbearer_set_token.
    const b64url = (s: string) =>
      Buffer.from(s).toString("base64").replace(/=+$/, "").replace(/\+/g, "-").replace(/\//g, "_");
    const now = Math.floor(Date.now() / 1000);
    const jwt = `${b64url(JSON.stringify({ alg: "none", typ: "JWT" }))}.${b64url(
      JSON.stringify({ sub: "admin", iat: now, exp: now + 3600 }),
    )}.`;

    let receivedConfig: string | undefined;
    const oauthErrors: LibrdKafkaError[] = [];
    const producer = new Producer({
      "bootstrap.servers": brokers,
      "security.protocol": "sasl_plaintext",
      "sasl.mechanisms": "OAUTHBEARER",
      "sasl.oauthbearer.config": "principal=admin",
      oauthbearer_token_refresh_cb: (config: string, done: (e: Error | null, t?: OauthBearerToken) => void) => {
        receivedConfig = config;
        done(null, { tokenValue: jwt, lifetime: Date.now() + 3_600_000, principal: "admin" });
      },
    });
    producer.on("event.error", (e: LibrdKafkaError) => {
      if (/oauth/i.test(e.message)) oauthErrors.push(e);
    });

    // The broker has no OAUTHBEARER ⇒ connect is allowed to fail — not asserted.
    await connectAsync(producer, 5_000).catch(() => {});
    await waitFor(() => receivedConfig !== undefined, 10_000, () => "refresh_cb invoked");
    expect(receivedConfig).toBe("principal=admin");
    // a successful set_token = the wiring fires no oauth-context event.error
    // (broker auth failures are a different matter, filtered by message).
    expect(oauthErrors).toHaveLength(0);
    await disconnectAsync(producer);
  }, 60_000);

  test("setSaslCredentials (M6): a wrong password → corrected → successful re-auth", async () => {
    // Proves brk_sasl_set_credentials's real function: a client starts with a
    // wrong password (continuous auth failures), the correct credentials go in
    // via FFI, librdkafka reconnects → metadata succeeds.
    const producer = new SliceProducer({
      config: saslConfig({
        "sasl.password": "wrong-pass",
        "reconnect.backoff.ms": 100,
        "reconnect.backoff.max.ms": 500,
      }),
    });
    producer.connect();
    await waitFor(
      () => producer.errorEvents.some((e) => /authenticat|SASL/i.test(e.reason)),
      15_000,
      () => "auth failing with the wrong password before the credential change",
    );

    producer.client.saslSetCredentials(SASL_CREDENTIALS.username, SASL_CREDENTIALS.password);
    // After the change: the next authentication must succeed → metadata OK.
    await waitFor(
      () => {
        try {
          return producer.client.metadata(null, 2_000).length > 0;
        } catch {
          return false;
        }
      },
      20_000,
      () => "metadata succeeding after the correct credential change",
    );
    producer.disconnect();
  }, 60_000);

  test("setSaslCredentials after connect on the Callback API + KafkaJS API (NOT_IMPLEMENTED no more)", async () => {
    // Callback API: connect with correct creds → changing (to the same creds) never throws.
    const cbProducer = new Producer(saslConfig());
    await connectAsync(cbProducer);
    expect(cbProducer.setSaslCredentials(SASL_CREDENTIALS.username, SASL_CREDENTIALS.password)).toBe(
      cbProducer,
    );
    await disconnectAsync(cbProducer);

    // KafkaJS API: setSaslCredentials after connect goes through rd_kafka_sasl_set_credentials.
    const kafka = new Kafka(saslConfig());
    const producer = kafka.producer();
    await producer.connect();
    producer.setSaslCredentials({
      username: SASL_CREDENTIALS.username,
      password: SASL_CREDENTIALS.password,
    });
    const topic = `m6-sasl-cred-${RUN_ID}`;
    const meta = await producer.send({ topic, messages: [{ value: "after-setSaslCredentials" }] });
    expect(meta[0]?.errorCode).toBe(0);
    await producer.disconnect();
  }, 120_000);
});
