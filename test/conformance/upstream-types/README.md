# upstream-types — verbatim copy of confluent-kafka-javascript's official `.d.ts`

This directory contains a **verbatim, unmodified copy** of the type declaration files from
the `types/` directory of [confluentinc/confluent-kafka-javascript][upstream]. They are the
*source of truth* for the conformance test `test/conformance/api-surface.test.ts`: the test
parses these files with the TypeScript Compiler API and compares the API surface with
`packages/bun-rdkafka/src/index.ts`.

| Field | Value |
|---|---|
| Repo | https://github.com/confluentinc/confluent-kafka-javascript |
| Branch | `master` |
| Commit SHA | `62e2a4a6a1a849c74593454d971bfeb25c95c212` |
| Commit date | 2026-08-04 |
| Downloaded on | 2026-08-14 |
| Original path | `types/` |

## Files

| File | Contents |
|---|---|
| `rdkafka.d.ts` | Callback API (FR-1): `Client`, `KafkaConsumer`, `Producer`, `HighLevelProducer`, `AdminClient`/`IAdminClient`, `features`, `librdkafkaVersion`, admin enums |
| `kafkajs.d.ts` | `KafkaJS` namespace (FR-2): `Kafka`, `Producer`, `Consumer`, `Admin`, `KafkaJS*Error`, `logLevel`, `CompressionTypes`, `PartitionAssigners` |
| `errors.d.ts` | `CODES.ERRORS` — the full librdkafka error-code table (generated from `src-cpp/rdkafkacpp.h`) |
| `config.d.ts` | `GlobalConfig` / `TopicConfig` / producer & consumer variants (types only, no runtime values) |
| `LICENSE.upstream.txt` | Copy of upstream's `LICENSE.txt` |

## License

The files in this directory are **Copyright (c) 2023 Confluent, Inc.** and are released
under the **MIT license** — see `LICENSE.upstream.txt`. They are included in the repo solely
for API cross-checking in tests and are **not** packaged when publishing `bun-rdkafka` (the
`test/` directory is not in the `files` field of `packages/bun-rdkafka/package.json`).

## Updating

To upgrade to a newer upstream commit:

```sh
SHA=<commit-sha>
for f in rdkafka.d.ts kafkajs.d.ts config.d.ts errors.d.ts; do
  curl -sSfL -o "test/conformance/upstream-types/$f" \
    "https://raw.githubusercontent.com/confluentinc/confluent-kafka-javascript/$SHA/types/$f"
done
curl -sSfL -o test/conformance/upstream-types/LICENSE.upstream.txt \
  "https://raw.githubusercontent.com/confluentinc/confluent-kafka-javascript/$SHA/LICENSE.txt"
```

Then update the table above and run `bun test test/conformance` — any new API appearing
upstream will fail the test until it is implemented or recorded in
`test/conformance/exclusions.ts`.

[upstream]: https://github.com/confluentinc/confluent-kafka-javascript
