FROM rust:1.67
WORKDIR /source

COPY . .
RUN cargo build
CMD ["/source/target/debug/rust-neon-example"]
