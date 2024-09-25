
# Example docker compose configuration

The configuration in this directory is used for testing Neon docker images: it is
not intended for deploying a usable system. To run a development environment where
you can experiment with a miniature Neon system, use `cargo neon` rather than container images.

This configuration does not start the storage controller, because the controller
needs a way to reconfigure running computes, and no such thing exists in this setup.

