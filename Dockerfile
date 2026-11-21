# Tessera container image (OP-10): one statically linked binary, every
# role, configured by flags at start (OP-11). Build with
# `docker build -t tessera .`.
#
# The builder targets musl, so the binary carries its C library and the
# runtime image needs none: a distroless static base with CA roots and
# a non-root user, and nothing else to patch. The data directory is
# shipped owned by that user, so that the volume made from it is
# writable without a shell to fix it up. CI runs the image, which a
# binary that still needed a C library could not do.

FROM rust:1.89-alpine AS builder
RUN apk add --no-cache build-base
WORKDIR /src
COPY . .
RUN cargo build --release -p tessera --locked && mkdir -p /var/lib/tessera

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /src/target/release/tessera /usr/local/bin/tessera
# The license goes with the binary it covers (Apache 2.0, section 4).
COPY --from=builder /src/LICENSE /src/NOTICE /licenses/
COPY --from=builder --chown=nonroot:nonroot /var/lib/tessera /var/lib/tessera
VOLUME ["/var/lib/tessera"]
EXPOSE 8080 5432
ENTRYPOINT ["/usr/local/bin/tessera"]
CMD ["run", "--data-dir", "/var/lib/tessera", "--listen", "0.0.0.0:8080", "--pg-listen", "0.0.0.0:5432"]
