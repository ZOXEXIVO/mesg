FROM rust:1.54 as build
WORKDIR /src

COPY ./ ./

RUN cargo test

RUN rustup component add rustfmt

RUN cargo build --release

FROM rust:1.54-slim

WORKDIR /app

COPY --from=build /src/target/release/mesg .

ENTRYPOINT ["./mesg"]