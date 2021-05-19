# syntax=docker/dockerfile:1
FROM rust:latest

RUN apt-get update

# Dependencies For building PostgreSQL
RUN apt-get install -y \
  build-essential \
  libreadline-dev \
  zlib1g-dev \
  flex \
  bison \
  libxml2-dev \
  libssl-dev

# Extra packages needed by Zenith modifications to Postgres sources
RUN apt-get install -y \
  libcurl4-openssl-dev

# Extra packages needed for Zenith. 'clang' is needed for bindgen, and
# pytest-3 is used to run tests.
RUN apt-get install -y \
  clang \
  python3-pip \
  python3-psycopg2

# We need a newer version of pytest (6.0) than is available in Debian Buster (3.1),
# so install it using pip rather than apt
RUN pip3 install pytest

# postgres refuses to run as root, so create a user
RUN useradd --create-home --home-dir /home/ubuntu --shell /bin/bash zenith
USER zenith
