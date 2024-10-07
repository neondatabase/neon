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

## Structure

The `vm-monitor` is loosely comprised of a few systems. These are:
* the server: this is just a simple `axum` server that accepts requests and
upgrades them to websocket connections. The server only allows one connection at
a time. This means that upon receiving a new connection, the server will terminate
and old one if it exists.
* the filecache: a struct that allows communication with the Postgres file cache.
On startup, we connect to the filecache and hold on to the connection for the
entire monitor lifetime.
* the cgroup watcher: the `CgroupWatcher` polls the `neon-postgres` cgroup's memory
usage and sends rolling aggregates to the runner.
* the runner: the runner marries the filecache and cgroup watcher together,
communicating with the agent throught the `Dispatcher`, and then calling filecache
and cgroup watcher functions as needed to upscale and downscale
