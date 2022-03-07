FROM rust:1.59
WORKDIR /testing

COPY testing/ ./testing 
COPY src/server/transport/proto/mesg.proto ./src/server/transport/proto/mesg.proto

WORKDIR testing

RUN rustup component add rustfmt

RUN cargo test