FROM rust:1.73
WORKDIR /source

COPY . .
RUN cargo build
CMD ["/source/target/debug/rust-neon-example"]
