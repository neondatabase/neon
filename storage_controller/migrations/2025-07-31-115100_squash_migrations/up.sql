/*
 * This migration squashes all previous migrations into a single one.
 * It will be applied in two cases:
 * 1. Spinning up a new region. There's no previous migrations apart from the diesel setup in this case
 * 2. For existing regions.
 *
 * Hence, this migration does nothing if the schema is already up to date.
 */

CREATE TABLE IF NOT EXISTS controllers (
    address character varying NOT NULL,
    started_at timestamp with time zone NOT NULL,
    PRIMARY KEY(address, started_at)
);

CREATE TABLE IF NOT EXISTS hadron_safekeepers (
    sk_node_id bigint PRIMARY KEY NOT NULL,
    listen_http_addr character varying NOT NULL,
    listen_http_port integer NOT NULL,
    listen_pg_addr character varying NOT NULL,
    listen_pg_port integer NOT NULL
);

CREATE TABLE IF NOT EXISTS hadron_timeline_safekeepers (
    timeline_id character varying NOT NULL,
    sk_node_id bigint NOT NULL,
    legacy_endpoint_id uuid,
    PRIMARY KEY(timeline_id, sk_node_id)
);

CREATE TABLE IF NOT EXISTS tenant_shards (
    tenant_id character varying NOT NULL,
    shard_number integer NOT NULL,
    shard_count integer NOT NULL,
    PRIMARY KEY(tenant_id, shard_number, shard_count),
    shard_stripe_size integer NOT NULL,
    generation integer,
    generation_pageserver bigint,
    placement_policy character varying NOT NULL,
    splitting smallint NOT NULL,
    config text NOT NULL,
    scheduling_policy character varying DEFAULT '"Active"'::character varying NOT NULL,
    preferred_az_id character varying
);

CREATE INDEX IF NOT EXISTS tenant_shards_tenant_id ON tenant_shards USING btree (tenant_id);

CREATE TABLE IF NOT EXISTS metadata_health (
    tenant_id character varying NOT NULL,
    shard_number integer NOT NULL,
    shard_count integer NOT NULL,
    PRIMARY KEY(tenant_id, shard_number, shard_count),
    -- Rely on cascade behavior for delete
    FOREIGN KEY(tenant_id, shard_number, shard_count) REFERENCES tenant_shards ON DELETE CASCADE,
    healthy boolean DEFAULT true NOT NULL,
    last_scrubbed_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE IF NOT EXISTS nodes (
    node_id bigint PRIMARY KEY NOT NULL,
    scheduling_policy character varying NOT NULL,
    listen_http_addr character varying NOT NULL,
    listen_http_port integer NOT NULL,
    listen_pg_addr character varying NOT NULL,
    listen_pg_port integer NOT NULL,
    availability_zone_id character varying NOT NULL,
    listen_https_port integer,
    lifecycle character varying DEFAULT 'active'::character varying NOT NULL,
    listen_grpc_addr character varying,
    listen_grpc_port integer
);

CREATE TABLE IF NOT EXISTS safekeeper_timeline_pending_ops (
    sk_id bigint NOT NULL,
    tenant_id character varying NOT NULL,
    timeline_id character varying NOT NULL,
    generation integer NOT NULL,
    op_kind character varying NOT NULL,
    PRIMARY KEY(tenant_id, timeline_id, sk_id)
);

CREATE TABLE IF NOT EXISTS safekeepers (
    id bigint PRIMARY KEY NOT NULL,
    region_id text NOT NULL,
    version bigint NOT NULL,
    host text NOT NULL,
    port integer NOT NULL,
    http_port integer NOT NULL,
    availability_zone_id text NOT NULL,
    scheduling_policy character varying DEFAULT 'activating'::character varying NOT NULL,
    https_port integer
);

CREATE TABLE IF NOT EXISTS timeline_imports (
    tenant_id character varying NOT NULL,
    timeline_id character varying NOT NULL,
    shard_statuses jsonb NOT NULL,
    PRIMARY KEY(tenant_id, timeline_id)
);

CREATE TABLE IF NOT EXISTS timelines (
    tenant_id character varying NOT NULL,
    timeline_id character varying NOT NULL,
    start_lsn pg_lsn NOT NULL,
    generation integer NOT NULL,
    sk_set bigint[] NOT NULL,
    new_sk_set bigint[],
    cplane_notified_generation integer NOT NULL,
    deleted_at timestamp with time zone,
    sk_set_notified_generation integer DEFAULT 1 NOT NULL,
    PRIMARY KEY(tenant_id, timeline_id)
);
