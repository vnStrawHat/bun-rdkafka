# Changelog

## v0.2.1 (2026-08-17)

### Features

- **kafkajs:** typed kafkaJS block and constructor configs (upstream names)
- **types:** typed librdkafka/js.* config for the callback API + generator
- **types:** typed events for the callback API (Client<Events>)
- **consumer:** experimental shim-side prefetch thread (js.consume.prefetch)

### Documentation

- **migration:** drop shipped APIs from "not yet implemented", note typed configs/events and the js.* reference
- **readme:** js.* configuration reference and typed-config notes

### Other

- **compare:** --prefetch row, CONSUMER_EXTRA, per-case topic cleanup + results

**Full diff:** [v0.2.0...v0.2.1](https://github.com/vnStrawHat/bun-rdkafka/compare/v0.2.0...v0.2.1)

## v0.2.0 (2026-08-17)

### Features

- **kafkajs:** close the Consumer API gaps — committed, storeOffsets, logger, dependentAdmin, rebalance assignmentFns
- **callback:** JS-side partitioner_cb for Producer/HighLevelProducer
- **callback:** HighLevelProducer.setTopicKeySerializer / setTopicValueSerializer
- **callback:** add the Stream API (ProducerStream / ConsumerStream)
- **callback:** event_cb, connection.failure, warning, getLastError, offsetsForTimes
- **native:** ABI 2 — brk_features + brk_offsets_for_times, features() export and default export

### Bug Fixes

- **native:** forward librdkafka's log queue to the main queue so event.log actually fires
- **callback:** fail queued ProducerStream writes if the stream is destroyed before ready

### Documentation

- three-style quick start (KafkaJS/callback/stream), install options, current platform matrix, consolidated bench results
- record the closed KafkaJS Consumer gaps (spec FR-2, MIGRATION notes, M8 status)
- document the Stream API (spec FR-1, MIGRATION, plan M8) + examples/streams.ts

### Maintenance

- make the byte-mode ConsumerStream test independent of chunk boundaries
- assert the shim ABI against BRK_ABI_VERSION instead of a hard-coded 1
- callback-gaps integration suite; route ERROR frames through Client.onErrorEvent
- KafkaJS gaps integration suite (EOS sendOffsets, committed, storeOffsets, logger, dependentAdmin, assignmentFns, pause fn)
- **integration:** Stream API round trips against a real broker
- hermetic unit tests for ProducerStream / ConsumerStream

**Full diff:** [v0.1.3...v0.2.0](https://github.com/vnStrawHat/bun-rdkafka/compare/v0.1.3...v0.2.0)

## v0.1.3 (2026-08-15)

### Bug Fixes

- **release:** publish from inside the package dir to avoid npm git-shorthand parsing
- **native:** silence MSVC C4200 for the flexible array member in brk_frame

### Maintenance

- **native:** treat librdkafka headers as SYSTEM to silence upstream header warnings

**Full diff:** [v0.1.2...v0.1.3](https://github.com/vnStrawHat/bun-rdkafka/compare/v0.1.2...v0.1.3)

## v0.1.2 (2026-08-15)

### Bug Fixes

- **test:** make loader resolution tests hermetic

### Documentation

- reflect 3-target prebuilt matrix after dropping darwin

### Maintenance

- run TS unit tests before native builds; verify artifacts post-build
- **native:** implement BRK_STATIC_DEPS for portable linux prebuilts
- **release:** tag and bump only after successful builds
- cache native deps, ccache and vcpkg binary archives
- drop linux-arm64 prebuilt target
- drop darwin targets from the build matrix

**Full diff:** [v0.1.1...v0.1.2](https://github.com/vnStrawHat/bun-rdkafka/compare/v0.1.1...v0.1.2)

## v0.1.1 (2026-08-14)

### Features

- initial release of @vnstrawhat/bun-rdkafka

### Bug Fixes

- **native:** disable cyrus SASL for portable prebuilts; support vcpkg header layout
- **ci:** install zstd/lz4 devel in AlmaLinux container; drop invalid lz4 vcpkg feature

### Maintenance

- **ci:** assert librdkafka builtin.features on every prebuilt target
