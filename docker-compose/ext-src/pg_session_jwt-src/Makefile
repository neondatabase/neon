EXTENSION = pg_session_jwt

REGRESS = basic_functions
REGRESS_OPTS = --load-extension=$(EXTENSION)
export PGOPTIONS = -c pg_session_jwt.jwk={"crv":"Ed25519","kty":"OKP","x":"R_Abz-63zJ00l-IraL5fQhwkhGVZCSooQFV5ntC3C7M"}

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
PG_REGRESS := $(dir $(PGXS))../../src/test/regress/pg_regress
.PHONY installcheck:
installcheck:
	dropdb --if-exists contrib_regression
	createdb contrib_regression
	psql -d contrib_regression -c "CREATE EXTENSION $(EXTENSION)"
	$(PG_REGRESS) --use-existing --dbname=contrib_regression $(REGRESS)
