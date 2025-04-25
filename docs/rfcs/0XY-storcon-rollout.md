# Feature Rollout on Storage Controller

This RFC describes the rollout interface from a user's perspective. I do not have a concreate implementation idea
yet, but the operations described below should be intuitively feasible to implement -- most of them map to a single
SQL inside the storcon database and then some reconcile operations.

The rollout RFC makes it possible for the storcon to gradually modify tenant configs based on filters and percentages.

What will it look like if we want to rollout gc-compaction gradually?

Create a feature called gc-compaction.

```
$ storcon-cli feature create --job gc-compaction
```

Add two config sets to the rollout: gc-compaction without verification, and gc-compaction with read path verification.

```
$ storcon-cli feature config-set add --job gc-compaction --id enable_wo_verification --config '{ "gc_compaction_enabled": true }'
$ storcon-cli feature config-set add --job gc-compaction --id enable --config '{ "gc_compaction_enabled": true, "gc_compaction_verification": true }'
$ storcon-cli feature config-set list --job gc-compaction
default {}
enable_wo_verification { "gc_compaction_enabled": true }
enable { "gc_compaction_enabled": true, "gc_compaction_verification": true }
```

Week 1: rollout to 1% of active small tenants with the verification mode enabled.

```
$ storcon-cli feature rollout --job gc-compaction --config-set enable --filter "remote_size < 10GB" --coverage-percentage 1
20000 attached tenants satisfying the filter and randomly picked 200 tenants to apply `enable`, use `feature status` to view the rollout status
```

```
$ storcon-cli feature status --job gc-compaction
enable_wo_verification 0
enable 200
default 100000

$ storcon-cli feature status --job gc-compaction --filter "remote_size < 10GB"
enable_wo_verification 0
enable 200
default 19800

$ storcon-cli feature history --job gc-compaction
id,time,config-set,filter,percentage,count
1,2025-04-25 14:00:00,enable,"remote_size < 10GB",1,200

$ storcon-cli feature history --job gc-compaction --id 1
<tenant_ids that are involved in this rollout>
```

Week 2: rollout to all active small tenants with the verification mode enabled, previously rolled-out tenants switch to full rollout that disables verification mode.

```
$ storcon-cli feature rollout --job gc-compaction --config-set enable_wo_verification --filter "config_set=enable" --coverage-percentage all
$ storcon-cli feature rollout --job gc-compaction --config-set enable --filter "remote_size < 10GB" --coverage-percentage all
```

Week 3: rollout gradually to 50% larger tenants before Jun 1. The storage controller will randomly select some tenants every day at 12am to rollout the change, and will finish the rollout to at least 50% of the tenants by Jun 1.

```
$ storcon-cli feature scheduled-rollout --job gc-compaction --config-set enable --filter "remote_size < 100GB" --coverage-percentage 50 --cron "0 0 * * *" --before 2025-06-01 00:00:00
```

Week 4: we discover a bug over a specific tenant and want to disable gc-compaction on it,

```
$ storcon-cli feature rollout --job gc-compaction --config-set default --filter "tenant-id=<id>" --coverage-percentage all
rollout succeeded, operation_id=11
```

Then we realize that this bug might affect all tenants and decide to disable it for all tenants:

```
$ storcon-cli feature rollout --job gc-compaction --config-set default --coverage-percentage all
rollout succeeded, operation_id=10
```

We get a fix and can re-enable it on those tenants which had the feature enabled previously.

```
$ storcon-cli feature rollout --job gc-compaction --revert 11
$ storcon-cli feature rollout --job gc-compaction --revert 10
```

Week 5: enable by default

For newly-attached tenants, we want to enable gc-compaction by default.

```
$ storcon-cli feature set-default-when-attached --job gc-compaction --config-set enable
```

Week 6: full rollout

```
$ storcon-cli feature rollout --job gc-compaction --config-set enable --coverage-percentage all
```

Then we make a tenant default config change in the infra repo, get it deployed, and we can delete the feature rollout record in the storcon database.

```
$ storcon-cli feature delete --job gc-compaction
```
