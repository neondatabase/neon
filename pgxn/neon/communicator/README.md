This package will evolve into a "compute-pageserver communicator"
process and machinery. For now, it just provides wrappers on the
neon-shmem Rust crate, to allow using it in the C implementation of
the LFC.

At compilation time, pgxn/neon/communicator/ produces a static
library, libcommunicator.a. It is linked to the neon.so extension
library. 
