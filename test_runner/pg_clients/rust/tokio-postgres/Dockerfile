FROM rust:1.76
WORKDIR /source

COPY . .
RUN cargo build
CMD ["/source/target/debug/rust-neon-example"]
