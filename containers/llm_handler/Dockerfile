FROM rust

WORKDIR /usr/src/llm_handler
COPY rust/Cargo.toml rust/Cargo.lock .
COPY rust/src src

RUN cargo install --path . --bin llm_handler --locked

CMD ["llm_handler"]