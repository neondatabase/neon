-- Your SQL goes here
-- very much a copy of cplane version, except that the projects_count is not included
CREATE TABLE safekeepers (
	id BIGSERIAL PRIMARY KEY,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	region_id TEXT NOT NULL,
	version BIGINT NOT NULL,
	instance_id TEXT NOT NULL,
	host TEXT NOT NULL,
	port INTEGER NOT NULL,
	active BOOLEAN NOT NULL DEFAULT false,
	-- projects_count INTEGER NOT NULL DEFAULT 0,
	http_port TEXT NOT NULL,
	availability_zone_id TEXT NOT NULL,
);
