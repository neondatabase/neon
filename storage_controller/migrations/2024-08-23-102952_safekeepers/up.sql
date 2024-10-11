-- started out as a copy of cplane schema, removed the unnecessary columns.
CREATE TABLE safekeepers (
	-- the surrogate identifier defined by control plane database sequence
	id BIGINT PRIMARY KEY,
	region_id TEXT NOT NULL,
	version BIGINT NOT NULL,
	-- the natural id on whatever cloud platform, not needed in storage controller
	-- instance_id TEXT UNIQUE NOT NULL,
	host TEXT NOT NULL,
	port INTEGER NOT NULL,
	active BOOLEAN NOT NULL DEFAULT false,
	-- projects_count INTEGER NOT NULL DEFAULT 0,
	http_port INTEGER NOT NULL,
	availability_zone_id TEXT NOT NULL
);
