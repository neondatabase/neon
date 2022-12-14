# Storage broker

Storage broker targets two issues:
- Allowing safekeepers and pageservers learn which nodes also hold their
  timelines, and timeline statuses there.
- Avoiding O(n^2) connections between storage nodes while doing so.

This is used
- By pageservers to determine the most advanced and alive safekeeper to pull WAL from.
- By safekeepers to synchronize on the timeline: advance
  `remote_consistent_lsn`, `backup_lsn`, choose who offloads WAL to s3.

Technically, it is a simple stateless pub-sub message broker based on tonic
(grpc) making multiplexing easy. Since it is stateless, fault tolerance can be
provided by k8s; there is no built in replication support, though it is not hard
to add.

Currently, the only message is `SafekeeperTimelineInfo`. Each safekeeper, for
each active timeline, once in a while pushes timeline status to the broker.
Other nodes subscribe and receive this info, using it per above.

Broker serves /metrics on the same port as grpc service. 

grpcurl can be used to check which values are currently being pushed:
```
grpcurl -proto broker/proto/broker.proto -d '{"all":{}}' -plaintext localhost:50051 storage_broker.BrokerService/SubscribeSafekeeperInfo
```
