# Neon Storage Scrubber

This tool directly accesses the S3 buckets used by the Neon `pageserver`
and `safekeeper`, and does housekeeping such as cleaning up objects for tenants & timelines that no longer exist.

## Usage

### Generic Parameters

#### S3

Do `aws sso login --profile dev` to get the SSO access to the bucket to clean.
Also, set the following environment variables:

- `AWS_PROFILE`: Profile name to use for accessing S3 buckets (e.g. `dev`)
- `REGION`: A region where the bucket is located at.
- `BUCKET`: Bucket name
- `BUCKET_PREFIX` (optional): Prefix inside the bucket

#### Console API

_This section is only relevant if using a command that requires access to Neon's internal control plane_

- `CLOUD_ADMIN_API_URL`: The URL base to use for checking tenant/timeline for existence via the Cloud API.  e.g. `https://<admin host>/admin`

- `CLOUD_ADMIN_API_TOKEN`: The token to provide when querying the admin API. Get one on the corresponding console page, e.g. `https://<admin host>/app/settings/api-keys`

### Commands

#### `find-garbage`

Walk an S3 bucket and cross-reference the contents with the Console API to identify data for
tenants or timelines that should no longer exist.

- `--node-kind`: whether to inspect safekeeper or pageserver bucket prefix
- `--depth`: whether to only search for deletable tenants, or also search for
  deletable timelines within active tenants. Default: `tenant`
- `--output-path`: filename to write garbage list to.  Default `garbage.json`

This command outputs a JSON file describing tenants and timelines to remove, for subsequent
processing by the `purge-garbage` subcommand.

**Note that the garbage list format is not stable.  The output of `find-garbage` is only
  intended for use by the exact same version of the tool running `purge-garbage`**

Example:

`env AWS_PROFILE=dev REGION=eu-west-1 BUCKET=my-dev-bucket CLOUD_ADMIN_API_TOKEN=[client_key] CLOUD_ADMIN_API_URL=[url] cargo run --release -- find-garbage --node-kind=pageserver --depth=tenant --output-path=eu-west-1-garbage.json`

Note that `CLOUD_ADMIN_API_TOKEN` can be obtained from https://console-stage.neon.build/app/settings/api-keys (for staging) or https://console.neon.tech/app/settings/api-keys for production. This is not the control plane admin JWT key. The env var name is confusing. Though anyone can generate that API key, you still need admin permission in order to access all projects in the region.

And note that `CLOUD_ADMIN_API_URL` should include the region in the admin URL due to the control plane / console split. For example, `https://console-stage.neon.build/regions/aws-us-east-2/api/v1/admin` for the staging us-east-2 region.

#### `purge-garbage`

Consume a garbage list from `find-garbage`, and delete the related objects in the S3 bucket.

- `--input-path`: filename to read garbage list from.  Default `garbage.json`.
- `--mode`: controls whether to purge only garbage that was specifically marked
            deleted in the control plane (`deletedonly`), or also to purge tenants/timelines
            that were not present in the control plane at all (`deletedandmissing`)

This command learns region/bucket details from the garbage file, so it is not necessary
to pass them on the command line

Example:

`env AWS_PROFILE=dev cargo run --release -- purge-garbage --input-path=eu-west-1-garbage.json`

Add the `--delete` argument before `purge-garbage` to enable deletion.  This is intentionally
not provided inline in the example above to avoid accidents.  Without the `--delete` flag
the purge command will log all the keys that it would have deleted.

#### `scan-metadata`

Walk objects in a pageserver or safekeeper S3 bucket, and report statistics on the contents and checking consistency.
Errors are logged to stderr and summary to stdout.

For pageserver:
```
env AWS_PROFILE=dev REGION=eu-west-1 BUCKET=my-dev-bucket CLOUD_ADMIN_API_TOKEN=${NEON_CLOUD_ADMIN_API_STAGING_KEY} CLOUD_ADMIN_API_URL=[url] cargo run --release -- scan-metadata --node-kind pageserver

Timelines: 31106
With errors: 3
With warnings: 13942
With garbage: 0
Index versions: 2: 13942, 4: 17162
Timeline size bytes: min 22413312, 1% 52133887, 10% 56459263, 50% 101711871, 90% 191561727, 99% 280887295, max 167535558656
Layer size bytes: min 24576, 1% 36879, 10% 36879, 50% 61471, 90% 44695551, 99% 201457663, max 275324928
Timeline layer count: min 1, 1% 3, 10% 6, 50% 16, 90% 25, 99% 39, max 1053
```

For safekeepers, dump_db_connstr and dump_db_table must be
specified; they should point to table with debug dump which will be used
to list timelines and find their backup and start LSNs.

## Cleaning up running pageservers

If S3 state is altered first manually, pageserver in-memory state will contain wrong data about S3 state, and tenants/timelines may get recreated on S3 (due to any layer upload due to compaction, pageserver restart, etc.). So before proceeding, for tenants/timelines which are already deleted in the console, we must remove these from pageservers.

First, we need to group pageservers by buckets, `https://<admin host>/admin/pageservers` can be used for all env nodes, then `cat /storage/pageserver/data/pageserver.toml` on every node will show the bucket names and regions needed.

Per bucket, for every pageserver id related, find deleted tenants:

`curl -X POST "https://<admin_host>/admin/check_pageserver/{id}" -H "Accept: application/json" -H "Authorization: Bearer ${NEON_CLOUD_ADMIN_API_STAGING_KEY}" | jq`

use `?check_timelines=true` to find deleted timelines, but the check runs a separate query on every alive tenant, so that could be long and time out for big pageservers.

Note that some tenants/timelines could be marked as deleted in console, but console might continue querying the node later to fully remove the tenant/timeline: wait for some time before ensuring that the "extra" tenant/timeline is not going away by itself.

When all IDs are collected, manually go to every pageserver and detach/delete the tenant/timeline.
In future, the cleanup tool may access pageservers directly, but now it's only console and S3 it has access to.
