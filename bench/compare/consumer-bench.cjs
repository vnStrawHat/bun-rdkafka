/*
 * Consumer throughput bench — flowing mode (the 'data' event) for both
 * libraries. The topic must already hold >= WARMUP + N messages. A fresh group
 * id per run. Env: LIB, TOPIC, N, WARMUP, BROKERS. Prints one JSON line and
 * exits.
 */
const {
  LIB,
  TOPIC,
  N = "500000",
  WARMUP = "100000",
  BROKERS = "localhost:9092",
  CONSUMER_EXTRA = "{}", // extra consumer conf (JSON), e.g. {"js.consume.prefetch":true}
} = process.env;

const lib = require(LIB);
const KafkaConsumer = lib.KafkaConsumer;
const n = +N;
const warm = +WARMUP;
const total = warm + n;

const consumer = new KafkaConsumer(
  {
    "metadata.broker.list": BROKERS,
    "group.id": `bench-${Date.now()}-${Math.floor(Math.random() * 1e6)}`,
    "enable.auto.commit": false,
    "fetch.queue.backoff.ms": 10,
    "log.connection.close": false,
    ...JSON.parse(CONSUMER_EXTRA),
  },
  { "auto.offset.reset": "earliest" },
);

let count = 0;
let t0 = 0;

consumer.on("event.error", (e) => console.error("event.error", e && e.message));
consumer.on("data", () => {
  count++;
  if (count === warm) t0 = performance.now();
  if (count === total) {
    const dt = (performance.now() - t0) / 1000;
    console.log(
      JSON.stringify({ msgs_per_s: Math.round(n / dt), dt_s: +dt.toFixed(3) }),
    );
    consumer.disconnect(() => process.exit(0));
    setTimeout(() => process.exit(0), 3000);
  }
});

consumer.connect();
consumer.on("ready", () => {
  consumer.subscribe([TOPIC]);
  consumer.consume();
});

setTimeout(() => {
  console.error(`timeout: only ${count}/${total} received after 180s`);
  process.exit(1);
}, 180000);
