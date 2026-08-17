/**
 * examples/streams.ts — the Stream API (mirrors the "Stream API" snippets of
 * confluent-kafka-javascript's INTRODUCTION.md).
 *
 * Writes a few messages to a topic through a Writable `ProducerStream`, then
 * reads them back through a Readable `ConsumerStream`.
 *
 *   KAFKA_BROKERS=localhost:9092 bun run examples/streams.ts
 */

import { Buffer } from "node:buffer";
import { once } from "node:events";

import { KafkaConsumer, Producer, type Message } from "../packages/bun-rdkafka/src/index.ts";

const brokers = process.env["KAFKA_BROKERS"] ?? "localhost:9092";
const topicName = process.env["KAFKA_TOPIC"] ?? "streams-example";

/* ----------------------------------------------------------- write stream */

// This call returns a new writable stream to our topic. The producer is
// created and connected for us; writes made before `ready` are queued.
const writeStream = Producer.createWriteStream(
  { "bootstrap.servers": brokers },
  {},
  {
    topic: topicName,
    autoClose: true, // flush + disconnect the producer on end()
    pollInterval: 1000,
  },
);

// Make sure to listen to the stream's error event if you want it to be
// durable. Otherwise, any error will bubble up as an uncaught exception.
writeStream.on("error", (err) => {
  console.error("Error in our producer stream", err);
});

for (let i = 0; i < 10; i++) {
  // `write()` only tells us the message was queued, not that it reached Kafka.
  const queuedSuccess = writeStream.write(Buffer.from(`Awesome message ${i}`));
  if (!queuedSuccess) {
    console.log("Too many messages in our queue already");
    await once(writeStream, "drain");
  }
}
writeStream.end();
await once(writeStream, "close");
console.log("Producer stream closed — 10 messages delivered.");

/* ------------------------------------------------------------ read stream */

// Note that this creates a new consumer + stream on each call, so call it
// once and store it. `topics` may also be a RegExp or a function of the
// broker metadata.
const readStream = KafkaConsumer.createReadStream(
  {
    "bootstrap.servers": brokers,
    "group.id": `streams-example-${Date.now()}`,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": false,
  },
  {},
  {
    topics: [topicName],
    fetchSize: 8, // messages requested from the consumer per fetch
    waitInterval: 200, // max ms to wait before retrying when idle
  },
);

readStream.on("error", (err) => {
  // Bun's Readable async iterator destroys the stream with an AbortError when
  // the `for await` loop breaks (Node passes no error) — not a Kafka failure.
  if ((err as { code?: string }).code === "ABORT_ERR") return;
  console.error("Error in our consumer stream", err);
});

// The stream is objectMode: every chunk is a `Message`. It is also an async
// iterable, so `for await` works and releases the consumer on break.
let seen = 0;
for await (const message of readStream as AsyncIterable<Message>) {
  console.log(`Got message ${message.topic}[${message.partition}]@${message.offset}: ${message.value?.toString()}`);
  if (++seen === 10) {
    // You can also get the `consumer` from the stream to use consumer methods —
    // e.g. commit the offsets read so far (before the break destroys the stream
    // and disconnects the consumer).
    readStream.consumer.commit();
    break;
  }
}
console.log("Consumer stream closed:", readStream.destroyed);
