FROM rust:1.87.0-alpine3.22 AS builder

RUN apk add --no-cache build-base alpine-sdk musl-dev openssl
RUN apk add --repository=https://dl-cdn.alpinelinux.org/alpine/edge/community --no-cache mold>=2.39.1

WORKDIR /app
COPY . /app

RUN cargo build --release

RUN mkdir -p /app/certs && \
    openssl ecparam -name prime256v1 -genkey -noout -out /app/certs/key.pem && \
    openssl req -new -x509 -key /app/certs/key.pem -out /app/certs/cert.pem -days 1825 -subj "/CN=localhost"

FROM alpine:3.22
RUN apk add --no-cache libstdc++
COPY --from=builder /app/target/release/gateway /app/config.toml /usr/local/bin/
COPY --from=builder /app/certs /etc/certs
CMD ["/usr/local/bin/gateway", "-c", "/usr/local/bin/config.toml"]
