This directory contains libraries that are specific for proxy.

Currently, it contains a signficant fork/refactoring of rust-postgres that no longer reflects the API
of the original library. Since it was so significant, it made sense to upgrade it to it's own set of libraries.

Proxy needs unique access to the protocol, which explains why such heavy modifications were necessary.
