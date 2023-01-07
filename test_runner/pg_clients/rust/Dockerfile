FROM rust:1.66 AS build
WORKDIR /source
COPY . .
RUN cargo build --release

FROM rust:1.66
WORKDIR /app
COPY --from=build /source/target/release/rust-neon .
CMD ["/app/rust-neon"]
