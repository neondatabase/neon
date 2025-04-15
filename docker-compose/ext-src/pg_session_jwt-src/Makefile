EXTENSION = pg_session_jwt

REGRESS = basic_functions
REGRESS_OPTS = --load-extension=$(EXTENSION)
export PGOPTIONS = -c pg_session_jwt.jwk={"crv":"Ed25519","kty":"OKP","x":"R_Abz-63zJ00l-IraL5fQhwkhGVZCSooQFV5ntC3C7M"}

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)