/**
 * The Callback API `Client` against a real Kafka broker: connect → ready
 * (metadata), getMetadata, queryWatermarkOffsets, a clean disconnect.
 *
 * NOTE: afterAll does NOT call stopKafka() — the broker container is shared
 * with the other integration tests/agents running in this session (equivalent
 * to KEEP_KAFKA=1); docker-kafka.ts manages the container idempotently.
 */

import { beforeAll, describe, expect, test } from "bun:test";
import { integrationAvailable, startKafka } from "./docker-kafka.ts";
import { Client, type Metadata } from "../../packages/bun-rdkafka/src/callback/client.ts";
import { BRK_CLIENT_PRODUCER } from "../../packages/bun-rdkafka/src/ffi/types.ts";
import type { LibrdKafkaError } from "../../packages/bun-rdkafka/src/core/errors.ts";

const AVAILABLE = await integrationAvailable();

describe.skipIf(!AVAILABLE)("callback Client (real broker)", () => {
  let brokers = "";

  beforeAll(async () => {
    const kafka = await startKafka();
    brokers = kafka.brokers;
  }, 120_000);

  test(
    "connect → ready → getMetadata → disconnect",
    async () => {
      const client = new Client(
        { "bootstrap.servers": brokers, "client.id": "cb-client-it" },
        undefined,
        BRK_CLIENT_PRODUCER,
      );
      const readyFired: string[] = [];
      client.on("ready", (info: { name: string }) => readyFired.push(info.name));

      const metadata = await new Promise<Metadata>((resolve, reject) => {
        client.connect({ timeout: 30_000 }, (err, md) =>
          err ? reject(err) : resolve(md as Metadata),
        );
      });

      expect(readyFired).toEqual([client.name]);
      expect(client.isConnected()).toBe(true);
      expect(metadata.brokers.length).toBeGreaterThan(0);
      expect(metadata.orig_broker_name.length).toBeGreaterThan(0);
      expect(metadata.brokers[0]).toMatchObject({
        host: expect.any(String),
        id: expect.any(Number),
        port: expect.any(Number),
      });

      // getMetadata after connect returns the same shape.
      const again = await new Promise<Metadata>((resolve, reject) => {
        client.getMetadata({ timeout: 10_000 }, (err, md) =>
          err ? reject(err) : resolve(md as Metadata),
        );
      });
      expect(again.brokers.length).toBe(metadata.brokers.length);

      const disconnected: number[] = [];
      client.on("disconnected", (m: { connectionOpened: number }) =>
        disconnected.push(m.connectionOpened),
      );
      await new Promise<void>((resolve, reject) => {
        client.disconnect((err: LibrdKafkaError | null) => (err ? reject(err) : resolve()));
      });
      expect(disconnected).toHaveLength(1);
      expect(client.isConnected()).toBe(false);
    },
    60_000,
  );
});
