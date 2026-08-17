# examples
Runnable examples for both API styles; ported from the upstream example set in M2/M5.

- `streams.ts` — the Stream API: `Producer.createWriteStream` + `KafkaConsumer.createReadStream` (mirrors the "Stream API" snippets of upstream's INTRODUCTION.md). `KAFKA_BROKERS=localhost:9092 bun run examples/streams.ts`
