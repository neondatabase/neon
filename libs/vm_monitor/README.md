# `vm-monitor`

The `vm-monitor` (or just monitor) is a core component of the autoscaling system,
along with the `autoscale-scheduler` and the `autoscaler-agent`s. The monitor has
two primary roles: 1) notifying agents when immediate upscaling is necessary due
to memory conditions and 2) managing Postgres' file cache and a cgroup to carry
out upscaling and downscaling decisions.

## More on scaling

We scale CPU and memory using NeonVM, our in-house QEMU tool for use with Kubernetes.
To control thresholds for receiving memory usage notifications, we start Postgres
in the `neon-postgres` cgroup and set its `memory.{max,high}`.

* See also: [`neondatabase/autoscaling`](https://github.com/neondatabase/autoscaling/)
* See also: [`neondatabase/vm-monitor`](https://github.com/neondatabase/vm-monitor/),
where initial development of the monitor happened. The repository is no longer
maintained but the commit history may be useful for debugging.
