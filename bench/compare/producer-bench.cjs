/*
 * Producer throughput bench — shared by bun-rdkafka and
 * confluent-kafka-javascript. Env: LIB (module path), TOPIC, N (measured),
 * WARMUP (uncounted), SIZE (bytes), ACKS (1|all), BROKERS. Prints one JSON
 * result line and exits.
 */
const {
  LIB,
  TOPIC,
  N = "500000",
  WARMUP = "100000",
  SIZE = "100",
  ACKS = "1",
  BROKERS = "localhost:9092",
} = process.env;

const lib = require(LIB);
const Producer = lib.Producer;
const n = +N;
const warm = +WARMUP;
const size = +SIZE;
const total = warm + n;
const value = Buffer.alloc(size, 97);

const producer = new Producer(
  {
    "metadata.broker.list": BROKERS,
    dr_cb: true,
    "queue.buffering.max.ms": 5,
    "batch.num.messages": 10000,
    "queue.buffering.max.messages": 1000000,
    "log.connection.close": false,
  },
  { "request.required.acks": ACKS === "all" ? -1 : +ACKS },
);

producer.setPollInterval(10);
producer.on("event.error", (e) => {
  console.error("event.error", e && e.message);
});

producer.connect();
producer.on("ready", () => {
  let sent = 0;
  let dr = 0;
  let t0 = 0;

  producer.on("delivery-report", (err) => {
    if (err) {
      console.error("DR error", err.message);
      process.exit(1);
    }
    dr++;
    if (dr === warm) t0 = performance.now();
    if (dr === total) {
      const dt = (performance.now() - t0) / 1000;
      console.log(
        JSON.stringify({
          msgs_per_s: Math.round(n / dt),
          mb_per_s: +((n * size) / dt / 1048576).toFixed(1),
          dt_s: +dt.toFixed(3),
        }),
      );
      producer.disconnect(() => process.exit(0));
      setTimeout(() => process.exit(0), 3000);
    }
  });

  const pump = () => {
    while (sent < total) {
      try {
        producer.produce(TOPIC, -1, value, null);
        sent++;
      } catch (e) {
        // QUEUE_FULL (or an earlier record error surfacing) — yield for DRs, then retry
        producer.poll();
        setTimeout(pump, 2);
        return;
      }
      if (sent % 20000 === 0) producer.poll();
    }
    producer.poll();
  };
  pump();
});

setTimeout(() => {
  console.error("timeout: the bench did not finish within 180s");
  process.exit(1);
}, 180000);
