-- very much a copy of cplane version, except that the projects_count is not included
CREATE TABLE safekeepers (
	-- the surrogate identifier defined by control plane database sequence
	id BIGINT PRIMARY KEY,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	region_id TEXT NOT NULL,
	version BIGINT NOT NULL,
	-- the natural id on whatever cloud platform
	instance_id TEXT UNIQUE NOT NULL,
	host TEXT NOT NULL,
	port INTEGER NOT NULL,
	active BOOLEAN NOT NULL DEFAULT false,
	-- projects_count INTEGER NOT NULL DEFAULT 0,
	http_port INTEGER NOT NULL,
	availability_zone_id TEXT NOT NULL
);
