# Percept — CI/CD strategy

**Status:** decided. Companion to `DECISIONS.md`.

## Decisions

### 1. Rust toolchain — pinned stable
Pinned to **stable 1.84** via `rust-toolchain.toml` (rustfmt + clippy +
`llvm-tools-preview` + aarch64 target included). Bumped explicitly via PR
when a feature or fix requires it. Rationale: predictable builds; clippy
lints don't shift under us between PRs.

### 2. Container image — yes, multi-arch on release
Built and pushed on every `v*` tag. Target registry:
`ghcr.io/wowow4978-vorfeed/percept`. Platforms: `linux/amd64` + `linux/arm64`.
Tags: `vX.Y.Z` and `latest`. No `main` / `nightly` images in v1 — they'd
dilute the "single binary, edge profile" story.

> The `Dockerfile` lands with the first code slice. Until then the container
> job is gated on its presence and no-ops.

### 3. Code coverage — set up, no gate
`cargo llvm-cov` on the test job emits LCOV; the file is uploaded to Codecov
on PRs and pushes to `main`. **No coverage threshold in v1** — we want the
signal without yet another blocking check while the codebase is small.

Codecov token: stored as the `CODECOV_TOKEN` repo secret. Public repos can
push without one, but the token stabilises uploads.

### 4. cargo-deny — strong copyleft denied
- **Advisories:** deny all RUSTSEC advisories; build fails on unpatched
  issues. Yanked crates are denied too.
- **Licenses allowed:** MIT, Apache-2.0 (with LLVM exception), BSD-2/3-Clause,
  ISC, Unicode-DFS-2016, Unicode-3.0, Zlib, MPL-2.0, CC0-1.0.
- **Licenses denied (implicitly, anything not in `allow`):** GPL-2.0/3.0,
  LGPL-2.1/3.0, AGPL-1.0/3.0, SSPL-1.0 — incompatible with shipping a
  permissively-licensed binary.
- **MPL-2.0 kept** because it's weak file-level copyleft and pervasive in
  Rust deps (e.g. `webpki-roots`). Revisit if a specific dep becomes a
  problem.
- **Bans:** `multiple-versions = warn` (start permissive, tighten when the
  duplicate count is informative); `wildcards = deny`.

## Workflows

### `ci.yml` — PR gate + push to `main`
Five jobs, all guarded on `hashFiles('**/Cargo.toml') != ''` so they cleanly
skip (and count as passing for branch protection) until the first code slice
lands.

| Job | Command | Runner |
|---|---|---|
| `fmt` | `cargo fmt --all --check` | `ubuntu-24.04` |
| `clippy` | `cargo clippy --workspace --all-targets --all-features -- -D warnings` | `ubuntu-24.04` |
| `test` | `cargo llvm-cov --workspace --all-features --lcov` + Codecov upload | `ubuntu-24.04` |
| `check-arm` | `cargo zigbuild --workspace --target aarch64-unknown-linux-gnu` | `ubuntu-24.04` |
| `deny` | `cargo deny check` | `ubuntu-24.04` |

`actions-rust-lang/setup-rust-toolchain@v1` honours `rust-toolchain.toml` and
bundles `Swatinem/rust-cache`, so every job is cached transparently.

`check-arm` uses `cargo-zigbuild` rather than `cross` — no Docker-in-Actions
overhead, ~30 s setup. It only does `check`-level work (no link), which
catches API breakage on aarch64 without paying for a full ARM build on every
PR; a real ARM build runs on release.

### `release.yml` — tag `v*`
Three jobs:

1. **`build`** — matrix over `(x86_64-unknown-linux-gnu, ubuntu-24.04)` and
   `(aarch64-unknown-linux-gnu, ubuntu-24.04-arm)` (native ARM runners — no
   cross-compile in the release path). Produces
   `percept-<version>-<target>.tar.gz` with the binary + `LICENSE` and a
   matching `.sha256`.
2. **`release`** — downloads artifacts, attaches them to the GitHub Release,
   auto-generates notes from PR titles.
3. **`container`** — multi-arch image build via buildx + QEMU; tagged with
   the semver version and `latest`; pushed to GHCR using the workflow's own
   `GITHUB_TOKEN`. Gated on `Dockerfile` presence.

### Branch protection
After the first Cargo project lands, require `fmt`, `clippy`, `test`,
`check-arm`, and `deny` as status checks on PRs to `main`.

## Out of scope for v1
- **Nightly / MSRV matrix** — single pinned stable until we have a reason.
- **Benchmark CI** — DESIGN §11 targets are validated manually on the Pi 5
  reference deployment (Appendix B) until there's something worth tracking
  automatically.
- **Standalone `cargo audit`** — `cargo deny check advisories` covers it.
- **SBOM** — tractable later via `cargo-cyclonedx`; not in v1.
- **Container vulnerability scan** (Trivy etc.) — defer until v1 ships.
- **CodeQL / SAST** — Rust support is shallow; skip.
