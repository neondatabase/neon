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

CREATE OR REPLACE FUNCTION sk_ps_discovery_enqueue_tenant()
RETURNS TRIGGER AS $$
BEGIN

	DELETE FROM sk_ps_discovery
	WHERE tenant_id = NEW.tenant_id;

	INSERT INTO sk_ps_discovery (tenant_id, shard_number, shard_count, ps_generation, sk_id, ps_id,created_at,last_attempt_at)
	SELECT (
		WITH sk_timeline_attachments AS (
			SELECT DISTINCT tenant_id,timeline_id,unnest(array_cat(sk_set, new_sk_set)) as sk_id FROM timelines
			WHERE tenant_id = NEW.tenant_id
		)
		SELECT NEW.tenant_id, NEW.tenant_id, NEW.tenant_id, tenant_shards.generation, sk_timeline_attachments.sk_id, tenant_shards.generation_pageserver, NOW(), NULL
		FROM tenant_shards
		INNER JOIN sk_timeline_attachments ON tenant_shards.tenant_id = sk_timeline_attachments.tenant_id
	);

	NOTIFY neon_storcon_sk_ps_discovery, json_build_object(
		'tenant_id', NEW.tenant_id
	)::text;

	RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER on_ps_tenant_shard_change_enqueue_sk_ps_discovery
AFTER INSERT OR UPDATE OR DELETE
ON "tenant_shards"
WHEN OLD.generation IS DISTINCT FROM NEW.generation
FOR EACH STATEMENT
EXECUTE FUNCTION sk_ps_discovery_enqueue_tenant();


CREATE OR REPLACE TRIGGER on_sk_timeline_update_enqueue_sk_ps_discovery
AFTER INSERT OR UPDATE OR DELETE
ON "timelines"
WHEN
	OLD.sk_set IS DISTINCT FROM NEW.sk_set
	OR
	OLD.new_sk_set IS DISTINCT FROM NEW.new_sk_set
FOR EACH STATEMENT
EXECUTE FUNCTION sk_ps_discovery_enqueue_tenant();
