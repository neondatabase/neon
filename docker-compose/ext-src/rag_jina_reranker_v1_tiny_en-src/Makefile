EXTENSION = rag_jina_reranker_v1_tiny_en
MODULE_big = rag_jina_reranker_v1_tiny_en
OBJS = $(patsubst %.rs,%.o,$(wildcard src/*.rs))

REGRESS = reranking_functions reranking_functions_enhanced

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
PG_REGRESS := $(dir $(PGXS))../../src/test/regress/pg_regress
.PHONY installcheck:
installcheck:
	dropdb --if-exists contrib_regression
	createdb contrib_regression
	../alter_db.sh
	psql -d contrib_regression -c "CREATE EXTENSION vector" -c "CREATE EXTENSION rag_jina_reranker_v1_tiny_en"
	$(PG_REGRESS) --use-existing --dbname=contrib_regression $(REGRESS)
