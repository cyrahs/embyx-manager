# The embyx-manager image: the server (`serve`), the merge Job's worker (`merge-worker`) and the
# front end. The runtime is what the deployment and the merge Job rely on: Debian 13 with its
# ffmpeg / ffprobe (7.1.x, SVT-AV1 2.3.0), CA certificates, time zone data, `/etc/mime.types` as
# ffmpeg's packages leave it, the `embyx` user (uid 1000), `embyx-manager` on PATH for the merge
# Job's command, and the EMBYX_MANAGER_HOST / EMBYX_MANAGER_PORT defaults, port, entry point and
# command the Python image had, so either image runs on the same manifests.
#
#   docker build -t embyx-manager:local .

# ---- frontend build ----
FROM node:22-alpine AS frontend
WORKDIR /build/frontend
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci --ignore-scripts
COPY frontend/ ./
# Vite writes the build to frontend/dist (frontend/vite.config.ts).
RUN npm run build

# ---- Rust toolchain with cargo-chef ----
# The same toolchain as rust-toolchain.toml, so rustup has nothing to download in later stages.
FROM rust:1.97.0-slim-trixie AS chef
# BoringSSL (the AVBase client, wreq) needs cmake, clang and libclang; perl and make for its build,
# git for the patches btls-sys applies to its source.
RUN apt-get update \
    && apt-get install -y --no-install-recommends cmake clang libclang-dev git make perl pkg-config g++ \
    && rm -rf /var/lib/apt/lists/*
RUN cargo install cargo-chef --locked --version 0.1.78
WORKDIR /build
ENV CARGO_INCREMENTAL=0

# The dependency recipe: only manifests and the lock decide it, so a code change keeps the
# dependency layer cached.
FROM chef AS planner
COPY Cargo.toml Cargo.lock rust-toolchain.toml rustfmt.toml clippy.toml ./
COPY crates ./crates
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY rust-toolchain.toml ./
COPY --from=planner /build/recipe.json recipe.json
RUN cargo chef cook --release --locked --recipe-path recipe.json -p embyx-manager
COPY Cargo.toml Cargo.lock rustfmt.toml clippy.toml ./
COPY crates ./crates
RUN cargo build --release --locked -p embyx-manager \
    && install -D -m 0755 target/release/embyx-manager /opt/embyx-manager/embyx-manager

# ---- runtime ----
FROM debian:trixie-slim
# The merge Job runs this image too and concatenates, remuxes and re-encodes with ffmpeg.
# ca-certificates and tzdata as python:3.13-slim (the Python image's base) carried them; the
# clients use their own bundled roots (webpki-roots).
RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg ca-certificates tzdata \
    && rm -rf /var/lib/apt/lists/*
RUN useradd --create-home --uid 1000 embyx
COPY --from=builder /opt/embyx-manager/embyx-manager /opt/embyx-manager/embyx-manager
# The front end sits next to the executable (`<exe dir>/static`, D59).
COPY --from=frontend /build/frontend/dist /opt/embyx-manager/static
ENV PATH="/opt/embyx-manager:$PATH" \
    EMBYX_MANAGER_HOST=127.0.0.1 \
    EMBYX_MANAGER_PORT=8000
USER embyx
EXPOSE 8000
ENTRYPOINT ["embyx-manager"]
CMD ["serve"]
