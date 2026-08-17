# Contributing to bun-rdkafka

## Repository layout

```
native/               C shim (libbunrdkafka) + CMake build; include/bunrdkafka.h is the ABI
packages/bun-rdkafka/ the published package (ffi / core / callback / kafkajs layers;
                      scripts/install.ts = postinstall prebuilt download + source-build
                      fallback, see design ADR-8; prebuilds/ is populated at install time)
test/unit/            no broker, no native library needed
test/conformance/     API-surface comparison against upstream .d.ts (no broker)
test/integration/     real broker via docker (helpers in test/integration/docker-kafka.ts)
bench/                benchmarks + RESULTS.md (measured numbers only)
docs/                 spec, detailed design, implementation plan
```

## Prerequisites

- Bun ≥ 1.2 (`bun --version`)
- CMake ≥ 3.22, a C11 compiler (gcc/clang; MSVC on Windows), Ninja (optional but the default generator in the build script)
- Development headers for OpenSSL, zlib, zstd, lz4 (Linux/macOS local builds link them dynamically; e.g. `apt install libssl-dev zlib1g-dev libzstd-dev liblz4-dev`)
- Docker (integration tests only)

## Building the native shim

```sh
bun install
bun run build:native      # cmake -S native -B native/build && build → native/build/libbunrdkafka.so
```

Notes:

- librdkafka is pinned by the `librdkafka.version` file (single source of truth) and fetched/built statically via CMake FetchContent. Version bumps must be their own PR, run the full integration suite, **and regenerate the typed config properties**: `bun run gen:config` rewrites `packages/bun-rdkafka/src/core/librdkafka-config.ts` from the fetched `CONFIGURATION.md` (the `Producer`/`KafkaConsumer` constructor types and editor completion come from that file).
- CMake options: `BRK_STATIC_DEPS=ON` (fully static OpenSSL/zlib/zstd/lz4 — release builds; currently a to-do that errors with guidance), `BRK_USE_SYSTEM_RDKAFKA=ON` (use `find_package(RdKafka)`, e.g. from vcpkg on Windows — see `native/vcpkg.json`).
- OIDC support requires `WITH_CURL=ON` (currently off; see the note in `native/CMakeLists.txt`).
- On memory-constrained machines build with `cmake --build native/build -j2`.

## Running tests

```sh
bun run test              # unit + conformance — no broker, no native library required
bun run typecheck         # tsc --noEmit (strict)

# integration — needs docker; reuses/starts container "bun-rdkafka-test-kafka"
KEEP_KAFKA=1 bun test test/integration
```

Integration environment variables (see `test/integration/docker-kafka.ts`):

| Variable | Effect |
|---|---|
| `KEEP_KAFKA=1` | don't remove the broker container afterwards (reuse across runs — recommended) |
| `KAFKA_IMAGE` | force a specific broker image. Docker Hub may be unreachable in some environments; any registry mirror of `apache/kafka` works (the helper auto-detects locally present images) |
| `KAFKA_PORT` | host port (default 9092) |
| `KAFKA_BROKERS` | use an existing broker; docker is not touched |
| `BUN_RDKAFKA_LIB_PATH` | explicit path to `libbunrdkafka.*` (otherwise: the package's `prebuilds/<target>/`, then the dev fallback `native/build/`) |

## ABI rules (the important part)

`native/include/bunrdkafka.h` is the **single source of truth** for the C↔TS contract: symbols, error-code conventions, and the packed binary formats are specified there. When you change it:

1. Update the header, the shim implementation, and `packages/bun-rdkafka/src/ffi/{symbols,types}.ts` plus the affected codecs/fixtures **in the same change**. The `test/unit/ffi-contract.test.ts` suite parses the header at test time and fails on any drift (constants, function names, arities).
2. Binary-format changes ripple: the TPL codec alone feeds commit/committed/assign/pause/rebalance/offset-commit paths. Change the shared codec on each side, then prove it with the full test suite.
3. Breaking format changes are acceptable pre-release without bumping `BRK_ABI_VERSION`; after the first release, any break bumps it.
4. No C→JS callbacks, ever. All native→JS communication goes through pollable queues and packed buffers (see `docs/02-detail-design.md`).
5. Hot-path buffers are JS-allocated and C-written; C must never retain a pointer into a JS buffer after the call returns.

## Language policy

All documentation, code comments, commit messages, and test descriptions are written in **English**.

## Benchmarks

```sh
bun run bench/m1-baseline.ts        # standalone produce/consume baseline (starts its own broker)
bun bench/compare/run.ts            # head-to-head vs @confluentinc/kafka-javascript
                                    # (first: cd bench/upstream && bun add @confluentinc/kafka-javascript)
```

Record any published numbers in `bench/RESULTS.md` with date, hardware, method, and a repro command — measured values only, no extrapolation.

## API documentation

```sh
bun run docs:api                    # TypeDoc → docs/api/ (gitignored)
```

## Releasing

Releases are cut from the GitHub Actions **Release** workflow (`Actions → Release →
Run workflow`) by choosing a version bump type: **patch**, **minor**, or **major**.
The workflow then:

1. bumps `packages/bun-rdkafka/package.json` accordingly;
2. generates the `CHANGELOG.md` section from **Conventional Commits** since the last
   `v*` tag (`scripts/changelog.ts` — preview locally with
   `bun scripts/changelog.ts --next <version> --dry-run`);
3. commits `chore(release): v<next>`, tags, and pushes;
4. builds the 5-target matrix, attaches `libbunrdkafka-<target>.tar.gz` + `SHA256SUMS`
   to the GitHub Release (the changelog section is the release body);
5. npm-publishes `@vnstrawhat/bun-rdkafka` with provenance (requires the `NPM_TOKEN`
   secret; skipped otherwise; already-published versions are skipped, so re-running a
   failed release is safe).

Because the changelog is generated from commit messages, **use Conventional Commits**
(`feat: …`, `fix(consumer): …`, `perf!: …`, a `BREAKING CHANGE:` footer where
relevant). Pushing a `v*` tag manually also triggers steps 4–5, provided the tagged
commit already carries the matching package version.
