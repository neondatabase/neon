# Self-Hosted

Run book for orchestrating a self-hosted Neon, regardless of Kubernetes or bare metal deployment.

---

# Orchestration PEMDAS

### Notes
- PageServer does an automatic `re-attach` `upcall` to `storage_controller` that inserts itself into the Storcon DB.
- SafeKeepers require a manual `POST` to `storage_controller` to upsert them into Storcon DB.
- Unsure how to automate Compute deployment with Tenant/Timeline IDs.

Dependency                                        |  Component
-------------------------------------------------------------------------------------
StorconDB                                         | PageServer & `storage_controller`
Compute + StorconDB (their DSNs set)              | `ctrl_plane_self_hosted`
PageServer w/ Tenant,Timeline + SafeKeepers       | Compute  (needs initial `config.json` with TenantID,TimelineID,SafeKeepers,PageServerConnString)
dir w/ `metadata.json`,`pageserver|identity.toml` | PageServer

### Initialization 

Set-Up

1. `storage_broker` 
2. StorconDB 
3. `storage_controller` 
4. PageServers - `identity.toml` + `metadata.json` register Node with Storcon DB etc.
- Registered with Storage Controller but no Tenant information, yet.
5. SafeKeepers

State 

1. SK `post` of its info to `storage_controller` API, and setting `scheduling_policy` to `Active`
2. `tenant` and `timeline` create for PS to `storage_controller` API
- I accidentally registered my PS2 with the `identity.toml` of PS1 (NodeID: 1234)
- `curl -X DELETE localhost:1234/control/v1/node/1234` [source](https://github.com/neondatabase/neon/blob/045ae13e060c3717c921097444d5c6b09925e87c/storage_controller/src/http.rs#L2108)
- `storage_controller` logs relaxed; StorconDB updated NodeId
3. Start Compute with initial TenantID + TimelineID, SK + PS info
4. Start `ctrl_plane_self_hosted` with DSNs.

DSNs of Compute + StorconDB have to be constant for Hook to work.

### Taking things down, bringing them back up
1. GCS tenant JSON upload failed on `PUT`so I shut everything down and went fix it
2. Turn on `storage_controller`and it's got no nodes/safekeepers yet
3. Broker and Storcon DB are running doing nothing
4. PageServer is compiling  still... SafeKeepers, too.
5. Pageservers look happy, `200`s in `storage_controller` log
- `Detaching tenant, control plane omitted it in re-attach response tenant_id=... shard_id=...` seems weird. Grep this.
- It's like the tenant never got created. I stopped both PS.
- Re-ran the `curl -X POST /v1/tenant` with the ID and `shard_parameters`. 
- Registered 2 shards for PS1 in Storcon DB but not with PS2 
- Compute Hook Notify going off in `storage_controller` which is good, it's waiting for Compute to show its face. 
- `upload consumption_metrics failed, will retry ... error sending request for url (http://localhost/metrics)`
- `storage_controller`: "`builder error for url (localhost:3000/notify-attach): URL scheme is not allowed`"
- Going to bed.


### Disorganized train of thought and steps
- I was going to start compute, but needed to make a timeline ID, so i ran the `curl`:
`RemotePath(\"tenants/99336152a31c64b41034e4e904629ce9-0102/tenant-manifest-00000001.json\")`
- Let's go test this in our GCS test.
- Oh, lol. My test overwrote the JSON file. 
- If you want to reset state, drop the `.neon/pageserver*/tenants/` directories.
- `storage_controller` runs diesel migrations on StorconDB when it starts. It never saves state itself. StorconDB does.
- okay, i had to delete the bucket path too to restart.
- so far so good. 
- make timeline with `curl`
- error: `failed to subscribe for timeline`
- so i shut down PS1, now looks like PS2 is off to the races. it was a DNS in `pageserver.toml`,needed to call `storage_broker`, `localhost`.- 
- PS2 error logs, same issue, so i shut down it. now PS1 logs are working. edited its `pageserver.toml` and restarted.
- both `nodes`  actie in StorconDB, but in `tenant_shards` table, both shards are still on PS1.

### SafeKeeper error?
- `couldn't find any active safekeeper for new timeline` -> after i did `curl` to create Timeline ID.
- this [api call](https://github.com/neondatabase/neon/blob/045ae13e060c3717c921097444d5c6b09925e87c/storage_controller/src/http.rs#L1551) required to activate SKs
- `SafekeeperSchedulingPolicyRequest` with `scheduling_policy: `SkSchedulingPolicy::Active` as request body in `post`.
```
curl -X POST localhost:1234/control/v1/safekeeper/3/scheduling_policy -d '{"scheduling_policy":"Active"}'
```
- then re-ping the `curl` for the tenant's timeline.

### Switching Shards, controlling PageServer config from `storage_controller` API.
- i see 2 shard (tenants with `-{shard}` suffix) on my `pageserver1/` directory
- i see `[mode.Attached]`and `attach_mode = "Single"`
- what would multi be?
- let's try migrating the shard over to PS2. [source](https://github.com/neondatabase/neon/blob/main/libs/pageserver_api/src/controller_api.rs#L186)
```
curl -X PUT localhost:1234/control/v1/tenant/99336152a31c64b41034e4e904629ce9-0002/migrate_secondary -d '{"node_id":1235, "origin_node_id":1234}'
```
- Reconciler error due to malformed URL for my `ctrl_plane_self_hosted` URL.
- is it just because Compute isn't up yet and Comptue Hook isn't doing anything? Yes. Yes.
- Let's start Compute

### Tenant Shard Migrate
- `TenantShardMigrateRequest` struct [source](https://github.com/neondatabase/neon/blob/main/libs/pageserver_api/src/controller_api.rs#L186)
```
curl -X PUT localhost:1234/control/v1/tenant/${tenant}-0002/migrate -d '{"node_id":1235}'
```
- failed with `Precondition failed: Migration to a worse-scoring node` 
- i changed the AZs to be both `ps1`. still not working. Both are set to `scheduling_policy` `Active`.
- i tried forcing it and it didnt stick:
```
curl -X PUT localhost:1234/control/v1/tenant/${tenant}-0002/migrate -d '{"node_id":1235,"origin_node_id":1234,"migration_config":{"override_scheduler":true}}'
```
- John's RFC [source](https://github.com/neondatabase/neon/blob/55f91cf10b30c3c648ac1301b95cd049bd7f0e21/docs/rfcs/028-pageserver-migration.md#cutover-procedure) from 2023
on what a live shard migration should look like.
- I'm going to check out `neon_local` for its `--shard-count` parameter on tenant creation and `--num-pageservers` on init, as my Neon quickstart PSs each have Attached Single and shards.
- TenantCreateRequest in Neon Local [source](https://github.com/neondatabase/neon/blob/55f91cf10b30c3c648ac1301b95cd049bd7f0e21/control_plane/src/bin/neon_local.rs#L1121https://github.com/neondatabase/neon/blob/55f91cf10b30c3c648ac1301b95cd049bd7f0e21/control_plane/src/bin/neon_local.rs#L1121)


- it looks like i've got `[mode.Secondary]` with `warm=true` on PS2 now, somehow. not sure which command did it. let's restart and retrace.
- is the final step to hit `/scheduling_policy` for tenant endpoint? let's try.
```
curl -X PUT localhost:1234/control/v1/tenant/99336152a31c64b41034e4e904629ce9/policy -d '{"placement":{"Attached":2}, "scheduling":"Active"}'
```
- that did something. now i have both shards on each PS under `tenants/`
- note: seeing a lot of `Starting download of layer` in PS2 logs, too. is it "attaching"?

- `storage_controller` logs are weird. got a "background reconcile identified optimization for attachment" with a `MigrateAttachment` and the new node `1235`.
-  it errored on an assertion of: `!self.secondary.contains(&new_secondary)`
- let's start this again.

### All of the Above Re-Done:
- StorconDB -> Broker -> `storage_controller` -> Sks -> PSs -> Policy+Register Sks > Tenant w/ just 2 shards param `curl` + Timeline > Start Compute
```
curl -X PUT localhost:1234/control/v1/tenant/${tenant}-0002/migrate -d '{"node_id":1235,"origin_node_id":1234,"migration_config":{"override_scheduler":true}}'
```
logs:
```
2025-05-17T03:40:34.756392Z  INFO request{method=PUT path=/control/v1/tenant/99336152a31c64b41034e4e904629ce9-0002/migrate request_id=97034c4f-989e-4f15-bdb4-0b3fdbdd805a}: Handling request                      
2025-05-17T03:40:34.756783Z  INFO request{method=PUT path=/control/v1/tenant/99336152a31c64b41034e4e904629ce9-0002/migrate request_id=97034c4f-989e-4f15-bdb4-0b3fdbdd805a}: scheduler selected node 1234 (elegible
 nodes [1234, 1235], hard exclude: [], soft exclude: ScheduleContext { nodes: {NodeId(1234): AffinityScore(2)}, mode: Normal }, preferred_az: Some(AvailabilityZone("ps1")))                                       
2025-05-17T03:40:34.756857Z  INFO request{method=PUT path=/control/v1/tenant/99336152a31c64b41034e4e904629ce9-0002/migrate request_id=97034c4f-989e-4f15-bdb4-0b3fdbdd805a}: Found a lower scoring location! 1234 i
s better than 1235 (NodeAttachmentSchedulingScore { az_match: AttachmentAzMatch(Yes), affinity_score: AffinityScore(2), utilization_score: 0, total_attached_shard_count: 0, node_id: NodeId(0) } is better than No
deAttachmentSchedulingScore { az_match: AttachmentAzMatch(No), affinity_score: AffinityScore(0), utilization_score: 0, total_attached_shard_count: 0, node_id: NodeId(0) })                                        
2025-05-17T03:40:34.756903Z  INFO request{method=PUT path=/control/v1/tenant/99336152a31c64b41034e4e904629ce9-0002/migrate request_id=97034c4f-989e-4f15-bdb4-0b3fdbdd805a}: Migrating to a worse-scoring node 1235
 (optimiser would prefer 1234)                                                                                                                                                                                     
2025-05-17T03:40:34.757366Z  INFO request{method=PUT path=/control/v1/tenant/99336152a31c64b41034e4e904629ce9-0002/migrate request_id=97034c4f-989e-4f15-bdb4-0b3fdbdd805a}:spawn_reconciler{tenant_id=99336152a31c
64b41034e4e904629ce9 shard_id=0002}: Spawning Reconciler (ActiveNodesDirty) seq=5                                                                                                                                  
2025-05-17T03:40:34.757634Z  INFO reconciler{seq=5 tenant_id=99336152a31c64b41034e4e904629ce9 shard_id=0002}: maybe_live_migrate: destination is None or attached                                                  
2025-05-17T03:40:34.757682Z  INFO reconciler{seq=5 tenant_id=99336152a31c64b41034e4e904629ce9 shard_id=0002}: Observed configuration correct after refresh. Notifying compute. node_id=1234                        
2025-05-17T03:40:34.757779Z  INFO reconciler{seq=5 tenant_id=99336152a31c64b41034e4e904629ce9 shard_id=0002}:notify{tenant_id=99336152a31c64b41034e4e904629ce9 shard_id=0002}: Skipping notification because remote
 state already matches (ComputeHookNotifyRequest { tenant_id: 99336152a31c64b41034e4e904629ce9, preferred_az: Some("ps1"), stripe_size: Some(ShardStripeSize(65558)), shards: [ComputeHookNotifyRequestShard { node
_id: NodeId(1234), shard_number: ShardNumber(0) }, ComputeHookNotifyRequestShard { node_id: NodeId(1234), shard_number: ShardNumber(1) }] })                                                                       
2025-05-17T03:40:34.757889Z  INFO reconciler{seq=5 tenant_id=99336152a31c64b41034e4e904629ce9 shard_id=0002}: Observed configuration requires update. node_id=1235                                                 
2025-05-17T03:40:34.757943Z  INFO reconciler{seq=5 tenant_id=99336152a31c64b41034e4e904629ce9 shard_id=0002}: location_config(1235 (localhost)) calling: LocationConfig { mode: Secondary, generation: None, second
ary_conf: Some(LocationConfigSecondary { warm: true }), shard_number: 0, shard_count: 2, shard_stripe_size: 65558, tenant_conf: TenantConfig { checkpoint_distance: None, checkpoint_timeout: None, compaction_targ
et_size: None, compaction_period: None, compaction_threshold: None, compaction_upper_limit: None, compaction_algorithm: None, compaction_shard_ancestor: None, compaction_l0_first: None, compaction_l0_semaphore: 
None, l0_flush_delay_threshold: None, l0_flush_stall_threshold: None, gc_horizon: None, gc_period: None, image_creation_threshold: None, pitr_interval: None, walreceiver_connect_timeout: None, lagging_wal_timeou
t: None, max_lsn_wal_lag: None, eviction_policy: None, min_resident_size_override: None, evictions_low_residence_duration_metric_threshold: None, heatmap_period: Some(60s), lazy_slru_download: None, timeline_get
_throttle: None, image_layer_creation_check_threshold: None, image_creation_preempt_threshold: None, lsn_lease_length: None, lsn_lease_length_for_ts: None, timeline_offloading: None, wal_receiver_protocol_overri
de: None, rel_size_v2_enabled: None, gc_compaction_enabled: None, gc_compaction_verification: None, gc_compaction_initial_threshold_kb: None, gc_compaction_ratio_percent: None, sampling_ratio: None } }

background_reconcile: Skipping migration of 99336152a31c64b41034e4e904629ce9-0002 to 1235 (localhost) because secondary isn't ready: SecondaryProgress { heatmap_mtime: None, lay
ers_downloaded: 0, layers_total: 0, bytes_downloaded: 0, bytes_total: 0 }

```
- it looks like PS2 logs are never downloading stuff so it's never gonna migrate?
- Ok cool! i did:
```
curl -X PUT localhost:1234/control/v1/tenant/99336152a31c64b41034e4e904629ce9/policy -d '{"placement":{"Attached":1}, "scheduling":"Active"}'
```
and it migrated BOTH shards as active to PS2 but as warm secondarys on PS1. then `storage_controller` undid it:
```
Found a lower scoring location! 1234 is better than 1235 
(NodeAttachmentSchedulingScore { az_match: AttachmentAzMatch(Yes), affinity_score: AffinityScore(1), utilization_score: 0, total_attached_shard_count: 0, node_id: NodeId(0) } 
is better than NodeAttachmentSchedulingScore { az_match: AttachmentAzMatch(No), affinity_score: AffinityScore(1), utilization_score: 0, total_attached_shard_count: 0, node_id: NodeId(0) })
```
- lol i wonder if we just make PS the same Az?


### Starting Compute
- so far so good
- i try my `curl` for shard migration again, and comptue hook fails. 
- request had node ids "1234, 1234" and we're supposed to have 1245?
- oh.....
- fixed. think migration worked.

### SafeKeeper `partial_backup` job failure at upload / delete?
- [source code](https://github.com/neondatabase/neon/blob/a7ce323949d277fa720a612d710b810903c1b1ff/safekeeper/src/wal_backup_partial.rs#L556C1-L565C70) for `failed to upload` error.
-  had to catch `404 NotFound` in delete objects as not a failure. All good. GCS thing.


--- 

(2 days later, GCS fixes done)

### PageServer update configuration using HTTP, `storage_controller`, JSON payloads.

- i'm going to do a shard migrate now to my second node, now that everything is humming
- it succeeded, but PS2 has no tenant still, though its logs went off.

### Config PageServer
- this is _everything above_ `[tenant_config]` header in `pageserver.toml`
- i wonder if a good distinction here is "node" rather than PageServer. This is the machine configuration that the tenant lives on.
- struct `PageserverConf` [source](https://github.com/neondatabase/neon/blob/55f91cf10b30c3c648ac1301b95cd049bd7f0e21/pageserver/src/config.rs#L50C12-L50C26)
- for now, I can't find / not going to worry about modifying these parameters on the fly.
- Here's that struct again, but as `ConfigToml`. I don't think there's a way to `curl` and see these. Just use the file or pass `-c` args to the binary. [source](https://github.com/neondatabase/neon/blob/532d9b646e4eaab6e0d94da8a6f890a9c834647c/libs/pageserver_api/src/config.rs#L102)

### Config Tenant
- this is the `[tenant_config]` header in `pageserver.toml`
- show me the current tenant config
```
curl -X GET localhost:1234/v1/tenant/$tenant/config
```
- configure tenant with payload of these fields: [source](https://github.com/neondatabase/neon/blob/main/libs/pageserver_api/src/models.rs#L641)
```
curl -X PUT localhost:1234/v1/tenant/config -d "{\"tenant_id\": \"$tenant\", \"compaction_target_size\":1234512345, \"checkpoint_timeout\":\"30m\"}"
```

### Sharding and MultiplePageServer Grok 
- I had 2 PS running. All i did was make a third, PS3, with the same AZ as PS1 and started it. I did nothing else.
Here is the log story for tracing through the PS code:

# Start-up
```
POST path=/upcall/v1/re-attach: Handling request
POST path=/upcall/v1/re-attach: Registered pageserver 457 (ps1), now have 3 pageservers
POST path=/upcall/v1/re-attach:re_attach: Incremented 0 tenants' generations
POST path=/upcall/v1/re-attach: Incremented 0 tenant shards' generations node_id=457
POST path=/upcall/v1/re-attach: Marking 457 warming-up on reattach
POST path=/upcall/v1/re-attach: Request handled, status: 200 OK
```

# Check for `LocationConfig`
```
2025-05-18T03:05:13.881662Z  INFO spawn_heartbeat_driver:node_activate_reconcile{node_id=457}: Loaded 0 LocationConfigs
2025-05-18T03:05:13.881813Z  INFO spawn_heartbeat_driver: Node 457 tran:wsition to active
```

# Reconciler
```
background_reconcile:optimize_attachment{shard_id=0002}: Found a lower scoring location! 457 is better than 1234 (NodeAttachmentSchedu
lingScore { az_match: AttachmentAzMatch(Yes), affinity_score: AffinityScore(0), utilization_score: 0, total_attached_shard_count: 0, node_id: NodeId(0) } is better than NodeAttachmentSchedulingScore { az_match: 
AttachmentAzMatch(Yes), affinity_score: AffinityScore(1), utilization_score: 0, total_attached_shard_count: 0, node_id: NodeId(0) })
background_reconcile: Identified optimization for attachment: ScheduleOptimization { sequence: 8, action: CreateSecondary(NodeId(457)) } tenant_shard_id=99336152a31c64b41034e4e9
04629ce9-0002
background_reconcile: Applying optimization: ScheduleOptimization { sequence: 8, action: CreateSecondary(NodeId(457)) } tenant_shard_id=99336152a31c64b41034e4e904629ce9-0002
background_reconcile:spawn_reconciler{tenant_id=99336152a31c64b41034e4e904629ce9 shard_id=0002}: Spawning Reconciler (ActiveNodesDirty) seq=9
reconciler{shard_id=0002}: maybe_live_migrate: destination is None or attached
reconciler{shard_id=0002}: Observed configuration correct after refresh. Notifying compute. node_id=1234
reconciler{shard_id=0002}:notify{tenant_id=99336152a31c64b41034e4e904629ce9 shard_id=0002}: Skipping notification because remote
 state already matches (ComputeHookNotifyRequest { tenant_id: 99336152a31c64b41034e4e904629ce9, preferred_az: Some("ps1"), stripe_size: Some(ShardStripeSize(65558)), shards: [ComputeHookNotifyRequestShard { node
_id: NodeId(1234), shard_number: ShardNumber(0) }, ComputeHookNotifyRequestShard { node_id: NodeId(1234), shard_number: ShardNumber(1) }] })
reconciler{shard_id=0002}: Observed configuration already correct. node_id=1235
reconciler{shard_id=0002}: Observed configuration requires update. node_id=457
reconciler{shard_id=0002}: location_config(457 (localhost)) calling: LocationConfig { mode: Secondary, generation: None, second

reconciler{seq=9 tenant_id=99336152a31c64b41034e4e904629ce9 shard_id=0002}: location_config(457 (localhost)) complete:
```

# Optimize Attachment Finds Better! Gets it warm
```
2025-05-18T03:06:53.870901Z  INFO background_reconcile:optimize_attachment{tenant_id=99336152a31c64b41034e4e904629ce9 shard_id=0002}: Found a lower scoring location! 457 is better than 1234 (NodeAttachmentSchedu
lingScore { az_match: AttachmentAzMatch(Yes), affinity_score: AffinityScore(0), utilization_score: 0, total_attached_shard_count: 0, node_id: NodeId(0) } is better than NodeAttachmentSchedulingScore { az_match: 
AttachmentAzMatch(Yes), affinity_score: AffinityScore(1), utilization_score: 0, total_attached_shard_count: 0, node_id: NodeId(0) })
2025-05-18T03:06:53.870994Z  INFO background_reconcile: Identified optimization for attachment: ScheduleOptimization { sequence: 9, action: MigrateAttachment(MigrateAttachment { old_attached_node_id: NodeId(1234
), new_attached_node_id: NodeId(457) }) } tenant_shard_id=99336152a31c64b41034e4e904629ce9-0002
2025-05-18T03:06:53.877235Z  INFO Heartbeat round complete for 3 nodes, 0 warming-up, 0 offline
2025-05-18T03:06:53.878255Z  INFO Heartbeat round complete for 3 safekeepers, 0 offline
2025-05-18T03:06:53.878437Z  INFO background_reconcile: 99336152a31c64b41034e4e904629ce9-0002 secondary on 457 (localhost) is warm enough for migration: SecondaryProgress { heatmap_mtime: Some(SystemTime(SystemT
ime { tv_sec: 1747454485, tv_nsec: 494000000 })), layers_downloaded: 4, layers_total: 4, bytes_downloaded: 10805248, bytes_total: 10805248 }
2025-05-18T03:06:53.878516Z  INFO background_reconcile: Applying optimization: ScheduleOptimization { sequence: 9, action: MigrateAttachment(MigrateAttachment { old_attached_node_id: NodeId(1234), new_attached_n
ode_id: NodeId(457) }) } tenant_shard_id=99336152a31c64b41034e4e904629ce9-0002
```
# Live migrate, calls Stale on PS1 Node
```
2025-05-18T03:06:53.879266Z  INFO reconciler{seq=10 tenant_id=99336152a31c64b41034e4e904629ce9 shard_id=0002}: Live migrating 1234 (localhost)->457 (localhost)
2025-05-18T03:06:53.879319Z  INFO reconciler{seq=10 tenant_id=99336152a31c64b41034e4e904629ce9 shard_id=0002}: 🔁 Switching origin node 1234 (localhost) to stale mode
2025-05-18T03:06:53.879373Z  INFO reconciler{seq=10 tenant_id=99336152a31c64b41034e4e904629ce9 shard_id=0002}: location_config(1234 (localhost)) calling: LocationConfig { mode: AttachedStale, generation: Some(3
```

# Live Migrate, downloads layers and sets to attachedmulti
```
2025-05-18T03:06:54.075451Z  INFO reconciler{seq=10 tenant_id=99336152a31c64b41034e4e904629ce9 shard_id=0002}: 🔁 Downloading latest layers to destination node 457 (localhost)
2025-05-18T03:06:54.187888Z  INFO reconciler{seq=10 tenant_id=99336152a31c64b41034e4e904629ce9 shard_id=0002}: Downloads to 457 (localhost) complete: 4/4 layers, 10805248/10805248 bytes
2025-05-18T03:06:54.260882Z  INFO reconciler{seq=10 tenant_id=99336152a31c64b41034e4e904629ce9 shard_id=0002}: 🔁 Attaching to pageserver 457 (localhost)
2025-05-18T03:06:54.260960Z  INFO reconciler{seq=10 tenant_id=99336152a31c64b41034e4e904629ce9 shard_id=0002}: location_config(457 (localhost)) calling: LocationConfig { mode: AttachedMulti, generation: Some(4)
```

# Finish (it turnned PS1 Node into a `Secondary` for Shard 0002)
```
2025-05-18T03:06:54.269580Z  INFO reconciler{seq=10 tenant_id=99336152a31c64b41034e4e904629ce9 shard_id=0002}: 🕑 Waiting for LSN to catch up...
2025-05-18T03:06:54.827785Z  INFO reconciler{seq=10 tenant_id=99336152a31c64b41034e4e904629ce9 shard_id=0002}: 🕑 LSN origin 0/15D0290 vs destination 0/15D0290 timeline_id=814ce0bd2ae452e11575402e8296b64d
2025-05-18T03:06:54.827877Z  INFO reconciler{seq=10 tenant_id=99336152a31c64b41034e4e904629ce9 shard_id=0002}: ✅ LSN caught up.  Proceeding...
2025-05-18T03:06:54.827921Z  INFO reconciler{seq=10 tenant_id=99336152a31c64b41034e4e904629ce9 shard_id=0002}: 🔁 Notifying compute to use pageserver 457 (localhost)

2025-05-18T03:07:13.871645Z  INFO background_reconcile: Identified optimization for attachment: ScheduleOptimization { sequence: 10, action: RemoveSecondary(NodeId(1234)) } tenant_shard_id=99336152a31c64b41034e4
e904629ce9-0002                                                                                                                                                                                                    
2025-05-18T03:07:13.871787Z  INFO background_reconcile: Applying optimization: ScheduleOptimization { sequence: 10, action: RemoveSecondary(NodeId(1234)) } tenant_shard_id=99336152a31c64b41034e4e904629ce9-0002  
2025-05-18T03:07:13.871997Z  INFO background_reconcile:spawn_reconciler{tenant_id=99336152a31c64b41034e4e904629ce9 shard_id=0002}: Spawning Reconciler (ActiveNodesDirty) seq=11                                   
2025-05-18T03:07:13.872242Z  INFO reconciler{seq=11 tenant_id=99336152a31c64b41034e4e904629ce9 shard_id=0002}: maybe_live_migrate: destination is None or attached                                                 
2025-05-18T03:07:13.872287Z  INFO reconciler{seq=11 tenant_id=99336152a31c64b41034e4e904629ce9 shard_id=0002}: Observed configuration correct after refresh. Notifying compute. node_id=457                        
2025-05-18T03:07:13.872404Z  INFO reconciler{seq=11 tenant_id=99336152a31c64b41034e4e904629ce9 shard_id=0002}:notify{tenant_id=99336152a31c64b41034e4e904629ce9 shard_id=0002}: Skipping notification because remot
e state already matches (ComputeHookNotifyRequest { tenant_id: 99336152a31c64b41034e4e904629ce9, preferred_az: Some("ps1"), stripe_size: Some(ShardStripeSize(65558)), shards: [ComputeHookNotifyRequestShard { nod
e_id: NodeId(457), shard_number: ShardNumber(0) }, ComputeHookNotifyRequestShard { node_id: NodeId(1234), shard_number: ShardNumber(1) }] }) 

```

### `LocationConfig`
- Yeah I think LocationConfig is the Tenant + the Shard on the PS.
- Like SK has s dance of: start binary, register with StorconDB, flick to active and shit,
- PS has a flick-on with Location

# Loading in DeSoto now with multiple Shards and PS:
- Errors abound  on PS
```
page_service_conn_main{peer_addr=127.0.0.1:35788 application_name=4075 compute_mode=primary}: Stopped due to handler reconnect request
handler requested reconnect: getpage@lsn request routed to wrong shard
```
- and on Compute:
```
pageserver_receive disconnect: psql end of copy data:
```










### Helpful Misc Links
- Allowed PageServer Errors: [source](https://github.com/neondatabase/neon/blob/532d9b646e4eaab6e0d94da8a6f890a9c834647c/test_runner/fixtures/pageserver/allowed_errors.py#L44)
- John's festive greetings [source](https://discord.com/channels/1176467419317940276/1194680868439076874/1318525983267098666)

---

# Quick Start commands with `cargo run` in 12 Tmux panes

### Reset State
```
rm -rf .neon/pageserver*/last_consumption_metrics.json .neon/pageserver*/tenants/ .neon/safekeeper*/99*
```

### Testing GCS
- Use a test bucket.
```
GCS_TEST_BUCKET="acrelab-production-us1c-transfer" cargo test --test test_real_gcs
```

### Run 
1. Start `storage_broker`:
```
cargo run --bin storage_broker -- --listen-addr=0.0.0.0:50051
```

2. Start Storage Controller DB 
```
 docker run --rm --name=standalonepg -p 5432:5432 \
	 -e POSTGRES_PASSWORD=postgres \
	 -e POSTGRES_DB=storage_controller \
	 postgres:16
```

3. Start `storage_controller` using `--control-plane-url` 
```
cargo run --bin storage_controller -- \
	--listen 0.0.0.0:1234 \
	--dev \
	--database-url postgresql://postgres:postgres@localhost:5432/storage_controller \
	--max-offline-interval 10s \
	--max-warming-up-interval 30s \
	--control-plane-url localhost:3000
```

4. Start SafeKeepers (3)
- Deploy 3 (for Paxos) SafeKeepers, incrementing `--id`, `--listen-pg`'s port's right-most digit; same for `--list_http`.
- Note `sk` variable in this and next step
```
typeset sk=1
cargo run --bin safekeeper -- --id=${sk} \
  --listen-pg="localhost:545${sk}" \
  --listen-http='0.0.0.0:767${sk}' \
  --broker-endpoint='http://localhost:50051' \
  -D .neon/safekeeper${sk}/ \
  --remote-storage='{bucket_name="$BUCKET_NAME",prefix_in_bucket="neon/safekeeper/"}' \
  --enable-offload --delete-offloaded-wal --wal-reader-fanout
```

5. Upsert SafeKeepers in Storcon DB
- Follow-up `curl POST` to `storage_controller` to get SK information into `storage_controller` database.
  - This request struct [here](https://github.com/neondatabase/neon/blob/main/storage_controller/src/persistence.rs#L2196)
  - Track `id`, I think. Stopping this SK1 will cause `storage_controller` logs to spaz. Restart it + `post` and it's okay.
  I experimented with posting a new port but to the same `id` and it registered in `storage_controller` database at the same SK as expected,
  causing `storage_controller` logs to relax.
```
for sk in 1 2 3; do
  curl -X POST localhost:1234/control/v1/safekeeper/${sk} \
    -d "{
      \"id\": ${sk}, 
      \"region_id\": \"us-central\", 
      \"version\": 1, 
      \"host\":\"localhost\", 
      \"port\":545${sk}, 
      \"http_port\":767${sk}, 
      \"availability_zone_id\": \"sk-${sk}\"
    }"
done
```

6. Start PageServers (2)
e.g.
```
cargo run --bin pageserver -- --workdir .neon/pageserver{number}
```

7. Initialize tenants and timelines for two active PageServers
```
typeset tenant="99336152a31c64b41034e4e904629ce9"
typeset timeline="814ce0bd2ae452e11575402e8296b64d"

typeset storcon_api="http://localhost:1234/v1/tenant"
curl -X POST $storcon_api -d '{
  "new_tenant_id": "99336152a31c64b41034e4e904629ce9",
  "shard_parameters": {"count":2, "stripe_size":65558},
  "placement_policy": {"Attached": 2}
}'

curl -X POST ${storcon_api}/${tenant}/timeline -d '{"new_timeline_id": "$timeline", "pg_version": 16}'
```

8. Start Compute
- `config.json` must have connstrings and Tenant information

9. Start Compute Hook Ctrl Plane service
- Set `compute_dsn` and `storcon_dsn` from steps 
```
cd ctrl_plane_self_hosted && cargo run
```

---

See Neon's [self-hosted](https://discord.com/channels/1176467419317940276/1184894814769127464) channel.

