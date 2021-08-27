FROM rust:1.54 as build
WORKDIR /src

COPY ./ ./

RUN rustup component add rustfmt

RUN cargo test

RUN cargo build --release

FROM rust:1.54-slim

WORKDIR /app

COPY --from=build /src/target/release/mesg .

ENTRYPOINT ["./mesg", "--port 4001"]