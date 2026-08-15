# Changelog

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
