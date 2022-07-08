FROM rust:1.62
WORKDIR /testing

COPY testing/ ./testing 
COPY src/server/transport/proto/mesg.proto ./src/server/transport/proto/mesg.proto

WORKDIR testing

RUN rustup component add rustfmt

RUN apt-get update && apt-get -y install cmake protobuf-compiler

RUN cargo test