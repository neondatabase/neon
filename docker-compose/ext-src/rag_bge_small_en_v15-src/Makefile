EXTENSION = rag_bge_small_en_v15
MODULE_big = rag_bge_small_en_v15
OBJS = $(patsubst %.rs,%.o,$(wildcard src/*.rs))

REGRESS = basic_functions embedding_functions basic_functions_enhanced embedding_functions_enhanced

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
PG_REGRESS := $(dir $(PGXS))../../src/test/regress/pg_regress
.PHONY installcheck:
installcheck:
	dropdb --if-exists contrib_regression
	createdb contrib_regression
	../alter_db.sh
	psql -d contrib_regression -c "CREATE EXTENSION vector" -c "CREATE EXTENSION rag_bge_small_en_v15"
	$(PG_REGRESS) --use-existing --dbname=contrib_regression $(REGRESS)