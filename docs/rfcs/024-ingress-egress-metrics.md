# Ingress-Egress Metrics

# Goals
* Track ingress/egress traffic directly from a compute node, which may be sending stuff due to 
  a custom extension.
* Lay the groundwork for further traffic management on the compute node. 

# Background
As we move towards allowing custom extensions on the compute node, we need to cope with the fact
that these extensions are untrusted. They could send or receive things from the open internet or
even try to send bogus requests around on the internal network. Although we currently use the proxy
to measure incoming/outgoing traffic, custom extensions will potentially be able to interact with
the outside without involving the proxy. This has a high potential for abuse, so we need some ability
to measure and lock down usage. This RFC concerns itself with the first part (measuring).

# Possible Implementations
* Service Mesh: There are lots of different implementations, but typically they have a 
  proxy (proxy-per-node) that all traffic within the cluster goes through, and this proxy(-ies)
  forwards metrics to the control plane. This seems more complicated than we need because: 
  - We only really need this for our compute nodes (not safekeepers and pageservers)
  - We control our compute nodes and have full power to customize it (which afaict is not an 
    assumption that most service meshes can make)
  - We already have a control plane, having two is more trouble than it's worth
  - Having all traffic go through yet another proxy will add some amount of latency
  - "You wanted a banana but what you got was a gorilla holding the banana and the entire jungle."

* eBPF Program: This option essentially implements what we actually need from the service mesh
  in its minimal form. The program will have very minimal overhead and will be run on each
  compute node. It has the advantages of being small, fully-customizable, and performant. One
  thing to consider is that it's not well-supported on MacOS, but given that we always run
  everything in Linux docker containers this doesn't seem like an issue. 
  
# In Detail
The eBPF program itself will be written in C and compiled with the eBPF backend as part of the 
Compute Image. The program will be loaded into the kernel by `compute_ctl` just before
it starts Postgres. The program will construct an eBPF ArrayMap that contains two elements:
ingress bytes and egress bytes.
```C
struct 
{
        __uint(type, BPF_MAP_TYPE_ARRAY);
        __type(key, u32);
        __type(value, u64);
        __uint(max_entries, 2);
} ingress_egress_counter_map SEC(".maps");
```

On the kernel side, the eBPF program will intercept all packets,
check the IP address to see if it is going in/outside of the network, and if so atomically increment
the corresponding counter. On the userspace side, `compute_ctl` will `mmap` this array of counters
(it isn't explicitly documented anywhere that you can do this, but I found
[this patch](https://lwn.net/Articles/804180) that lets you mmap. Once per minute, `compute_ctl`
will `atomic_exchange` these counters with `0`, wrap them up in a JSON, and send them to the
control plane, in much the same format as in 021-metering.md.

```json
{
    "metric" : "compute_ingress_bytes",
    "endpoint_id" : "super-gigachad-117",
    "event_start_time" : ...,
    "event_stop_time" : ...,
    "value" : ...
}
```
