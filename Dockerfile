FROM rust:1.90.0-alpine3.22 AS builder

RUN apk add --no-cache build-base alpine-sdk musl-dev openssl linux-headers

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
RUN --mount=type=bind,source=.git,target=/app/.git,ro \
    sh -c "mkdir -p src && echo 'fn main() {}' > src/main.rs && cargo fetch --locked && rm -rf src"

COPY . /app

RUN --mount=type=bind,source=.git,target=/app/.git,ro cargo build --release

RUN mkdir -p /app/certs && \
    openssl ecparam -name prime256v1 -genkey -noout -out /app/certs/key.pem && \
    openssl req -new -x509 -key /app/certs/key.pem -out /app/certs/cert.pem -days 1825 -subj "/CN=localhost"

FROM alpine:3.22
RUN apk add --no-cache libstdc++
COPY --from=builder /app/target/release/gateway /usr/local/bin/
COPY --from=builder /app/certs /etc/certs
CMD ["/usr/local/bin/gateway"]
