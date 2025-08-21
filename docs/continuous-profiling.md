# Continuous Crofiling (Compute)

The continuous profiling of the compute node is performed by `perf` or `bcc-tools`, the latter is preferred.

The executables profiled are all the postgres-related ones only, excluding the actual compute code (Rust). This can be done as well but
was not the main goal.

## Tools

The aforementioned tools are available within the same Docker image as
the compute node itself, but the corresponding dependencies linux the
linux kernel headers and the linux kernel itself are not and can't be
for obvious reasons. To solve that, as we run the compute nodes as a
virtual machine (qemu), we need to deliver these dependencies to it.
This is done by the `autoscaling` part, which builds and deploys the
kernel headers, needed modules, and the `perf` binary into an ext4-fs
disk image, which is later attached to the VM and is symlinked to be
made available for the compute node.

## Output

The output of the profiling is always a binary file in the same format
of `pprof`. It can, however, be archived by `gzip` additionally, if the
corresponding argument is provided in the JSON request.

## REST API

### Test profiling

One can test the profiling after connecting to the VM and running:

```sh
curl -X POST -H "Content-Type: application/json" http://localhost:3080/profile/cpu -d '{"profiler": {"BccProfile": null}, "sampling_frequency": 99, "timeout_seconds": 5, "archive": false}' -v --output profile.pb
```

This uses the `Bcc` profiler and does not archive the output. The
profiling data will be saved into the `profile.pb` file locally.

**Only one profiling session can be run at a time.**

To check the profiling status (to see whether it is already running or
not), one can perform the `GET` request:

```sh
curl http://localhost:3080/profile/cpu -v
```

The profiling can be stopped by performing the `DELETE` request:

```sh
curl -X DELETE http://localhost:3080/profile/cpu -v
```

## Supported profiling data

For now, only the CPU profiling is done and ther is no heap profiling.
Also, only the postgres-related executables are tracked, the compute
(Rust) part itself **is not tracked**.
