# Tessera container image (OP-10): one binary, every role, configured by
# flags at start (OP-11). Build with `docker build -t tessera .`.
#
# The binary links against glibc; a statically linked musl build is not
# verified yet, so the runtime image is Debian slim rather than scratch.

FROM rust:1.85-bookworm AS builder
WORKDIR /src
COPY . .
RUN cargo build --release -p tessera --locked

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --create-home --home-dir /var/lib/tessera tessera
COPY --from=builder /src/target/release/tessera /usr/local/bin/tessera
USER tessera
VOLUME ["/var/lib/tessera"]
EXPOSE 8080 5432
ENTRYPOINT ["tessera"]
CMD ["run", "--data-dir", "/var/lib/tessera", "--listen", "0.0.0.0:8080", "--pg-listen", "0.0.0.0:5432"]
