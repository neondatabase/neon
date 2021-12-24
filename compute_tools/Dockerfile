# First transient image to build compute_tools binaries
FROM rust:slim-buster AS rust-build

RUN mkdir /compute_tools
WORKDIR /compute_tools

COPY . /compute_tools/

RUN cargo build --release

# Final image that only has one binary
FROM debian:buster-slim

COPY --from=rust-build /compute_tools/target/release/zenith_ctl /usr/local/bin/zenith_ctl
