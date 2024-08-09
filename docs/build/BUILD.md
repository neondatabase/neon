# Build Dependencies

The following subsection describes (build) dependencies.
After building, read `./RUN-LOCAL.md` to get a running local neon instance.

## Support Script

Build support scripts can be found at `/scripts/build/`.

## Base Dependencies

To build the project from source the following prerequisites are needed:

- Rustc (latest source)
- Google Protobuf Compiler (27.3)
- Python3 / Poetry (Pytest Test Framework)
- Clang Compiler
- CMake
- PostgreSQL Development Header / Source Code

## Ubuntu / Debian Packages

```bash
sudo apt install build-essential libtool libreadline-dev zlib1g-dev flex bison libseccomp-dev \
libssl-dev clang pkg-config libpq-dev cmake postgresql-client libcurl4-openssl-dev \
openssl python3-poetry lsof libicu-dev
```

> Ubuntu 22.04.4 does not provide suitable `protobuf-compiler` package! Get the latest.

## Fedora Packages

```bash
dnf install flex bison readline-devel zlib-devel openssl-devel \
  libseccomp-devel perl clang cmake postgresql postgresql-contrib protobuf-compiler \
  protobuf-devel libcurl-devel openssl poetry lsof libicu-devel libpq-devel python3-devel \
  libffi-devel
```

## Arch Linux

```bash
pacman -S base-devel readline zlib libseccomp openssl clang \
postgresql-libs cmake postgresql protobuf curl lsof
```

## Compile Protobuf 27.3

In case your distribution does not provide suitable protobuf packages.

### Get Protobuf Binary

Get the binary distribution @
https://github.com/protocolbuffers/protobuf/releases/download/v27.3/

### Compile Protobuf Source

In case the binary distribution does not work, compile by yourself.

```bash
wget https://github.com/protocolbuffers/protobuf/releases/download/v27.3/protobuf-27.3.tar.gz
```

Protobuf CMake build is broken, so bazel has to be installed.

1. On Debian, add the bazel repository.

```bash
sudo apt install apt-transport-https curl gnupg -y
curl -fsSL https://bazel.build/bazel-release.pub.gpg | gpg --dearmor >bazel-archive-keyring.gpg
sudo mv bazel-archive-keyring.gpg /usr/share/keyrings
echo "deb [arch=amd64 signed-by=/usr/share/keyrings/bazel-archive-keyring.gpg] https://storage.googleapis.com/bazel-apt stable jdk1.8" | sudo tee /etc/apt/sources.list.d/bazel.list
```

2. Install bazel

```bash
sudo apt install bazel -y
```

3. Build

```bash
cd protobuf-
bazel build :protoc :protobuf
```

4. Install Protobuf

```bash
cp bazel-bin/protoc /usr/local/bin
chown root:root /usr/local/bin/protoc

cp -R src/google /usr/local/include
chown -R root:root /usr/local/include/google
```

## Install Rust

[Install Rust](https://www.rust-lang.org/tools/install)

```bash
# recommended approach from https://www.rust-lang.org/tools/install
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

### Rustc Version

The project uses [rust toolchain file](./rust-toolchain.toml) to define the version it's built with in CI for testing
and local builds.

This file is automatically picked up by [`rustup`](https://rust-lang.github.io/rustup/overrides.html#the-toolchain-file)
that installs (if absent) and uses the toolchain version pinned in the file.

**rustup** users who want to build with another toolchain can use the [`rustup override`](https://rust-lang.github.io/rustup/overrides.html#directory-overrides)
command to set a specific toolchain for the project's directory.

**non-rustup** users most probably are not getting the same toolchain automatically from the file, so are responsible to manually
verify that their toolchain matches the version in the file. Newer rustc versions most probably will work fine, yet older ones might
not be supported due to some new features used by the project or the crates.

## macOS (12.3.1)

1. Install XCode and dependencies

```bash
xcode-select --install
brew install protobuf openssl flex bison icu4c pkg-config

# add openssl to PATH, required for ed25519 keys generation in neon_local
echo 'export PATH="$(brew --prefix openssl)/bin:$PATH"' >> ~/.zshrc
```

2. [Install Rust](https://www.rust-lang.org/tools/install)

```bash
# recommended approach from https://www.rust-lang.org/tools/install
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

3. Install PostgreSQL Client

```bash
# from https://stackoverflow.com/questions/44654216/correct-way-to-install-psql-without-full-postgres-on-macos
brew install libpq
brew link --force libpq
```

# Building on Linux

Build neon and patched postgres.

```bash
# Note: The path to the neon sources can not contain a space.

git clone --recursive https://github.com/neondatabase/neon.git
cd neon

# The preferred and default is to make a debug build. This will create a
# demonstrably slower build than a release build. For a release build,
# use "BUILD_TYPE=release make -j`nproc` -s"
# Remove -s for the verbose build log

make -j`nproc` -s
```

# Building on OSX

Build neon and patched postgres.

```bash
# Note: The path to the neon sources can not contain a space.

git clone --recursive https://github.com/neondatabase/neon.git
cd neon

# The preferred and default is to make a debug build. This will create a
# demonstrably slower build than a release build. For a release build,
# use "BUILD_TYPE=release make -j`sysctl -n hw.logicalcpu` -s"
# Remove -s for the verbose build log

make -j`sysctl -n hw.logicalcpu` -s
```

# Cleanup

For cleaning up the source tree from build artifacts, run `make clean` in the source directory.

For removing every artifact from build and configure steps, run `make distclean`, and also consider
removing the cargo binaries in the `target` directory, as well as the database in the `.neon` directory.

> Note that removing the `.neon` directory will remove your database, with all data in it. You have been warned!
