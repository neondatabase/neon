# Architecture Naming Scheme

## Summary

Neon computes are going to support multiple CPU architectures, including 64-bit
x86 and ARM. Architecture naming schemes are fairly inconsistent, at least when
it comes to these two architectures in particular. Sometimes 64-bit x86 is known
as `amd64` and at other times `x86_64`. For 64-bit ARM, it's a similar story
with `arm64` and `aarch64`.

## Motivation

Consistency when referring to these architectures across the platform in
important not only so that everyone is on the same page, but also because we
have architecture-dependent portions of the platform. One notable example is
remote extensions. Remote extensions need to be compiled and packaged into
compressed tarballs that are then downloaded by computes on demand. Downloading
a tarball for the wrong architecture will cause `dlopen(3)` to fail when
Postgres attempts to load the library. The compressed tarballs are located in an
S3 bucket with object keys of the form
`$BUILD_TAG/$ARCH/$PG_VERSION_NUM/${EXTENSION_NAME}.tar.zst` to mitigate the
potential for failure. The build and deployment pipeline for remote extensions
needs to be in lock step with the code in the compute that actually fetches
remote extensions.

## Impacted Components

- Control Plane when persisting the target architecture in compute flags if
  specified.
- Austocaling when scheduling compute pods.
- `compute_ctl` when downloading remote extensions.
- Build and deployment pipeline for remote extensions.

## Prior Art for CPU Architecture Names

- [Rust](https://doc.rust-lang.org/std/env/consts/constant.ARCH.html)
  - `x86_64`
  - `aarch64`
- [Go](https://pkg.go.dev/internal/goarch#pkg-constants)
  - `amd64`
  - `arm64`
- Kubernetes
  - Because Kubernetes is written in Go, it inherits the naming scheme.

Going all in on either the Rust naming scheme or the Go naming scheme can save
us a branch when getting the architecture, so we should pick one and force the
other side to conform.

## Decision

The heart of our platform is Kubernetes. We will inherit the Go naming
scheme just like Kubernetes. This makes things easy for the autoscaling team
when they do pod scheduling on nodes.
