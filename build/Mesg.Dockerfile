FROM rust:1.67.1 as build
WORKDIR /src

COPY ./ ./

RUN rustup component add rustfmt

RUN apt-get update && apt-get -y install cmake protobuf-compiler

RUN cargo test

RUN apt-get update && apt-get -y install cmake protobuf-compiler

RUN cargo build --release

FROM rust:1.67.1-slim

WORKDIR /app

COPY --from=build /src/target/release/mesg .

ENTRYPOINT ["./mesg"]