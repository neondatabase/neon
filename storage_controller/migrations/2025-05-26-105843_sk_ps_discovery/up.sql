CREATE TABLE "sk_ps_discovery"(
	"tenant_id" VARCHAR NOT NULL,
	"shard_number" INT4 NOT NULL,
	"shard_count" INT4 NOT NULL,
	"ps_generation" INT4 NOT NULL,
	"sk_id" INT8 NOT NULL,
	"ps_id" INT8 NOT NULL,
	"created_at" TIMESTAMPTZ NOT NULL,
	"last_attempt_at" TIMESTAMPTZ,
	PRIMARY KEY("tenant_id", "shard_number", "shard_count", "ps_generation", "sk_id")
);

CREATE OR REPLACE FUNCTION sk_ps_discovery_enqueue_tenant(ARG_TENANT_ID VARCHAR)
RETURNS VOID AS $$
BEGIN

	DELETE FROM sk_ps_discovery
	WHERE tenant_id = ARG_TENANT_ID;

	INSERT INTO sk_ps_discovery (tenant_id, shard_number, shard_count, ps_generation, sk_id, ps_id,created_at,last_attempt_at)
		WITH sk_timeline_attachments AS (
			SELECT DISTINCT tenant_id,timeline_id,unnest(array_cat(sk_set, new_sk_set)) as sk_id FROM timelines
			WHERE tenant_id = ARG_TENANT_ID
		)
		SELECT tenant_shards.tenant_id, tenant_shards.shard_number, tenant_shards.shard_count, tenant_shards.generation, sk_timeline_attachments.sk_id, tenant_shards.generation_pageserver, NOW(), NULL
		FROM tenant_shards
		INNER JOIN sk_timeline_attachments ON tenant_shards.tenant_id = sk_timeline_attachments.tenant_id;

	PERFORM pg_notify('sk_ps_discovery', json_build_object(
		'tenant_id', ARG_TENANT_ID
	)::text);
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION on_ps_tenant_shard_INSERT_enqueue_sk_ps_discovery_triggerfn()
RETURNS TRIGGER AS $$
BEGIN
	PERFORM sk_ps_discovery_enqueue_tenant(NEW.tenant_id);
	RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE OR REPLACE TRIGGER on_ps_tenant_shard_INSERT_enqueue_sk_ps_discovery
AFTER INSERT
ON "tenant_shards"
FOR EACH ROW
EXECUTE FUNCTION on_ps_tenant_shard_INSERT_enqueue_sk_ps_discovery_triggerfn();


CREATE OR REPLACE FUNCTION on_ps_tenant_shard_DELETE_enqueue_sk_ps_discovery_triggerfn()
RETURNS TRIGGER AS $$
BEGIN
	PERFORM sk_ps_discovery_enqueue_tenant(OLD.tenant_id);
	RETURN OLD;
END;
$$ LANGUAGE plpgsql;
CREATE OR REPLACE TRIGGER on_ps_tenant_shard_DELETE_enqueue_sk_ps_discovery
AFTER DELETE
ON "tenant_shards"
FOR EACH ROW
EXECUTE FUNCTION on_ps_tenant_shard_DELETE_enqueue_sk_ps_discovery_triggerfn();


CREATE OR REPLACE FUNCTION on_ps_tenant_shard_UPDATE_enqueue_sk_ps_discovery_triggerfn()
RETURNS TRIGGER AS $$
BEGIN
	PERFORM sk_ps_discovery_enqueue_tenant(NEW.tenant_id);
	RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE OR REPLACE TRIGGER on_ps_tenant_shard_UPDATE_enqueue_sk_ps_discovery
AFTER UPDATE
ON "tenant_shards"
FOR EACH ROW
EXECUTE FUNCTION on_ps_tenant_shard_UPDATE_enqueue_sk_ps_discovery_triggerfn();

-- TODO: same for `timelines` table
