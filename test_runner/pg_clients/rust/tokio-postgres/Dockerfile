FROM rust:1.70
WORKDIR /source

COPY . .
RUN cargo build
CMD ["/source/target/debug/rust-neon-example"]
