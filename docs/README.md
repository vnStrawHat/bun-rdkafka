# bun-rdkafka — Documentation

A native Kafka client for Bun: binds [librdkafka](https://github.com/confluentinc/librdkafka)
via `bun:ffi`, with an API compatible with [confluent-kafka-javascript](https://github.com/confluentinc/confluent-kafka-javascript).

| Document | Contents |
|---|---|
| [01-spec.md](./01-spec.md) | Requirements specification: goals, scope, functional/non-functional requirements, supported platforms, v1.0 acceptance criteria, risks |
| [02-detail-design.md](./02-detail-design.md) | Detailed design: 4-layer architecture, C shim ABI, packed binary protocol, PollScheduler, the two API layers, memory management, prebuilt packaging, CI/CD, testing & benchmarking, ADRs |
| [03-implementation-plan.md](./03-implementation-plan.md) | Implementation plan: 8 milestones (M0–M8), dependencies, ~14–18 week estimate, schedule risks |

**Architecture in one paragraph:** a C shim (`libbunrdkafka`) statically links librdkafka
together with all of its dependencies into exactly one shared library, flattens the API into
~60 FFI-friendly `brk_*` functions, and turns every librdkafka callback into an event queue;
the TypeScript side dlopens this library via `bun:ffi` and actively polls/drains in batches
using a binary protocol (one FFI call ↔ thousands of messages), and on top of that builds the
Callback API (node-rdkafka style) and the `KafkaJS` namespace (promisified) exactly like
confluent-kafka-javascript. Prebuilt binaries are built by GitHub Actions for
linux/macos/windows (x64 + arm64) and attached to GitHub Releases; a postinstall script
downloads the matching binary (SHA-256 verified) and falls back to building from the
bundled C sources when no prebuilt matches (design ADR-8).
