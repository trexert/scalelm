FROM rust

WORKDIR /usr/src/http_frontend
COPY rust/Cargo.toml rust/Cargo.lock .
COPY rust/src src

RUN cargo install --path . --bin http_frontend

CMD ["http_frontend"]