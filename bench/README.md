# bench

Producer/consumer benchmarks + the confluent-kafka-javascript baseline (design §12).
Real measured results: [RESULTS.md](./RESULTS.md)

| Script | Purpose |
|---|---|
| `m1-baseline.ts` | M1 baseline: produce N messages of 100 B → wait for the last DR → consume all N. Measures msgs/s in both directions, CPU, RSS over time. Spins up the docker broker itself via `test/integration/docker-kafka.ts`. |
| `compare/run.ts` | M6 head-to-head vs confluent-kafka-javascript (installed under `bench/upstream/`): producer/consumer throughput + e2e latency, same librdkafka config on both sides, 3-run median. `--quick` for a smoke run. |

```sh
bun run bench/m1-baseline.ts                          # 1,000,000 messages
TOTAL=500000 bun run bench/m1-baseline.ts             # reduce the load
KAFKA_BROKERS=host:9092 bun run bench/m1-baseline.ts  # use an existing broker
docker rm -f bun-rdkafka-test-kafka                   # clean up the broker
```

The script does **not** remove the container when done (for fast re-runs and log inspection).
