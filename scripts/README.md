# scripts

Repo-level tooling. The distribution scripts live **inside the package** so they
ship in the npm tarball: see `packages/bun-rdkafka/scripts/` —

| Script | Role |
|---|---|
| `install.ts` | postinstall hook & `bun-rdkafka-install` bin: downloads the prebuilt library from the GitHub Release (SHA-256 verified), falls back to a source build (design ADR-8) |
| `install-plan.ts` | pure decision logic for `install.ts` (unit-tested without network) |
| `prepack.ts` / `postpack.ts` | copy `native/` + `librdkafka.version` into the npm tarball at publish time, and clean up afterwards |
