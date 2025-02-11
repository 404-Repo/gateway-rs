FROM rust:1.84-alpine3.21 AS builder

RUN apk add --no-cache build-base alpine-sdk musl-dev
RUN apk add --repository=https://dl-cdn.alpinelinux.org/alpine/edge/community --no-cache mold=~2.36
WORKDIR /app
COPY . /app
RUN cargo build --release

FROM alpine:3.21
RUN apk add --no-cache libstdc++
COPY --from=builder /app/target/release/gateway /app/config.toml /usr/local/bin/
CMD ["/usr/local/bin/gateway", "-c", "/usr/local/bin/config.toml"]
