# `vm-monitor`

The `vm-monitor` (or just monitor) is a core component of the autoscaling system,
along with the `autoscale-scheduler` and the `autoscaler-agent`s. The monitor has
two primary roles: 1) notifying agents when immediate upscaling is necessary due
to memory conditions and 2) managing Postgres' file cache and a cgroup to carry
out upscaling and downscaling decisions.

## More on scaling

We scale Postgres' by starting it in a cgroup called `neon-postgres`. We then
control its memory usage by setting the cgroup's `memory.{max,high}`. We scale
CPU using NeonVM, our in-house QEMU tool for use with Kubernetes.
