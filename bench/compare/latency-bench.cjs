/*
 * End-to-end latency bench: produces steadily at ~RATE msg/s while the
 * consumer measures now - sentTs. 5s warmup, DURATION_S measured (default
 * 20s), prints p50/p99 (ms).
 * Env: LIB, TOPIC, BROKERS, RATE (default 10000), DURATION_S.
 */
const {
  LIB,
  TOPIC,
  BROKERS = "localhost:9092",
  RATE = "10000",
  DURATION_S = "20",
} = process.env;

const lib = require(LIB);
const { Producer, KafkaConsumer } = lib;
const rate = +RATE;
const durationMs = +DURATION_S * 1000;
const WARMUP_MS = 5000;
const TICK_MS = 5;
const perTick = Math.max(1, Math.round((rate * TICK_MS) / 1000));

const producer = new Producer(
  {
    "metadata.broker.list": BROKERS,
    "queue.buffering.max.ms": 1,
    "log.connection.close": false,
  },
  { "request.required.acks": 1 },
);
const consumer = new KafkaConsumer(
  {
    "metadata.broker.list": BROKERS,
    "group.id": `lat-${Date.now()}-${Math.floor(Math.random() * 1e6)}`,
    "enable.auto.commit": false,
    "fetch.queue.backoff.ms": 10,
    "fetch.wait.max.ms": 5,
    "topic.metadata.refresh.interval.ms": 2000,
    "log.connection.close": false,
  },
  { "auto.offset.reset": "latest" },
);

const lat = [];
let benchStart = 0;
let interval = null;

let done = false;
function finish() {
  if (done) return;
  done = true;
  clearInterval(interval);
  lat.sort((a, b) => a - b);
  const q = (p) => lat[Math.min(lat.length - 1, Math.floor(p * lat.length))];
  console.log(
    JSON.stringify({
      samples: lat.length,
      p50_ms: +q(0.5).toFixed(2),
      p99_ms: +q(0.99).toFixed(2),
    }),
  );
  producer.disconnect(() => consumer.disconnect(() => process.exit(0)));
  setTimeout(() => process.exit(0), 3000);
}

consumer.on("data", (m) => {
  const now = Date.now();
  const sentTs = Number(m.value.toString("ascii", 0, 13));
  if (!Number.isFinite(sentTs)) return; // the topic-creating "init" message
  const inWarmup = now - benchStart < WARMUP_MS;
  if (!inWarmup) lat.push(now - sentTs);
  if (now - benchStart >= WARMUP_MS + durationMs) finish();
});

consumer.on("ready", () => {
  consumer.subscribe([TOPIC]);
  consumer.consume();
});

producer.connect();
producer.on("ready", () => {
  // create the topic before the consumer subscribes (producer-side auto-create)
  producer.produce(TOPIC, -1, Buffer.from("init"), null);
  producer.poll();
  setTimeout(() => consumer.connect(), 1000);
  producer.setPollInterval(50);
  benchStart = Date.now() + 1000;
  const pad = Buffer.alloc(87, 46); // 13 byte ts + 87 = 100B payload
  interval = setInterval(() => {
    for (let i = 0; i < perTick; i++) {
      const buf = Buffer.concat([Buffer.from(String(Date.now())), pad]);
      try {
        producer.produce(TOPIC, -1, buf, null);
      } catch {
        /* queue-full never happens at this rate; skip the tick */
      }
    }
  }, TICK_MS);
});

consumer.on("event.error", (e) => console.error("event.error", e && e.message));

setTimeout(() => {
  console.error("timeout latency bench");
  process.exit(1);
}, WARMUP_MS + durationMs + 60000);
