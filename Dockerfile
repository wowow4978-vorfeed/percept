# Multi-stage build for the Percept v0.1.0 release image.
#
# DESIGN Appendix A originally called for a Distroless base, but ONNX
# Runtime musl support is the blocker — and slice 4 swapped FastEmbed
# for a deterministic placeholder embedder, so the runtime needs no ONNX
# libs at all. v0.1.0 ships on `debian:bookworm-slim` (~ 30 MB base).
# Distroless can land once the slice-4 follow-up wires the real
# embedder with a known native-deps story.

# --- build stage ---
FROM rust:1.85-bookworm AS builder
WORKDIR /build

# Copy the workspace manifests + lockfile first so the dep build layer
# is cached across source-only edits. (Cargo doesn't have a "vendor
# manifests" mode, so we copy the lot and rely on Docker's diff to
# decide what to rebuild.)
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates ./crates

# Static binary is not possible without a musl rebuild (rusqlite needs
# the SQLite C lib; openssl-sys etc. via reqwest's rustls path). We
# build a gnu binary and ship a thin debian runtime.
RUN cargo build --release --bin percept --locked

# --- runtime stage ---
FROM debian:bookworm-slim AS runtime

# CA certs for outbound TLS (forwarder -> hub over HTTPS; HTTP MCP
# tools also need TLS roots when the operator points at an https://
# peer URL). `tini` keeps the container reaping zombies cleanly so
# `kill -INT` propagates to the shutdown signal handler.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates tini \
    && rm -rf /var/lib/apt/lists/*

# Non-root user; data_dir is owned by it so a bind-mounted volume
# inherits the right ownership on `chown -R 1000:1000 ...` at deploy
# time. Operators bind-mounting with a different uid should mirror it.
RUN useradd --system --uid 1000 --create-home --home-dir /var/lib/percept percept

COPY --from=builder /build/target/release/percept /usr/local/bin/percept
COPY docs/sample.percept.toml /etc/percept/percept.toml
RUN chown -R percept:percept /etc/percept /var/lib/percept

USER percept
WORKDIR /var/lib/percept
VOLUME ["/var/lib/percept"]
EXPOSE 7878

# RUST_LOG can be overridden at `docker run` time.
ENV RUST_LOG=info

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/percept"]
CMD ["serve", "--config", "/etc/percept/percept.toml"]
