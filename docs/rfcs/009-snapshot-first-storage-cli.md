While working on export/import commands, I understood that they fit really well into "snapshot-first design".

We may think about backups as snapshots in a different format (i.e plain pgdata format, basebackup tar format, WAL-G format (if they want to support it) and so on). They use same storage API, the only difference is the code that packs/unpacks files.

Even if zenith aims to maintains durability using it's own snapshots, backups will be useful for uploading data from postges to zenith.

So here is an attemt to design consistent CLI for diferent usage scenarios:

#### 1. Start empty pageserver.
That is what we have now.
Init empty pageserver using `initdb` in temporary directory.

`--storage_dest=FILE_PREFIX | S3_PREFIX |...` option defines object storage type, all other parameters are passed via env variables. Inspired by WAL-G style naming : https://wal-g.readthedocs.io/STORAGES/.

Save`storage_dest` and other parameters in config. 
Push snapshots to `storage_dest` in background.

```
zenith init --storage_dest=S3_PREFIX
zenith start
```

#### 2. Restart pageserver (manually or crash-recovery).
Take `storage_dest` from pageserver config, start pageserver from latest snapshot in `storage_dest`. 
Push snapshots to `storage_dest` in background.

```
zenith start
```

#### 3. Import.
Start pageserver from existing snapshot.
Path to snapshot provided via `--snapshot_path=FILE_PREFIX | S3_PREFIX | ...`
Do not save `snapshot_path` and `snapshot_format` in config, as it is a one-time operation.
Save`storage_dest` parameters in config. 
Push snapshots to `storage_dest` in background.
```
//I.e. we want to start zenith on top of existing $PGDATA and use s3 as a persistent storage.
zenith init --snapshot_path=FILE_PREFIX --snapshot_format=pgdata --storage_dest=S3_PREFIX
zenith start
```
How to pass credentials needed for `snapshot_path`?

#### 4. Export.
Manually push snapshot to `snapshot_path` which differs from `storage_dest` 
Optionally set `snapshot_format`, which can be plain pgdata format or zenith format.
```
zenith export --snapshot_path=FILE_PREFIX --snapshot_format=pgdata
```

#### Notes and questions
- walkeeper s3_offload should use same (similar) syntax for storage. How to set it in UI?
- Why do we need `zenith init` as a separate command? Can't we init everything at first start?
- We can think of better names for all options.
- Export to plain postgres format will be useless, if we are not 100% compatible on page level.
I can recall at least one such difference - PD_WAL_LOGGED flag in pages.