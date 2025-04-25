EXTENSION = rag_jina_reranker_v1_tiny_en
MODULE_big = rag_jina_reranker_v1_tiny_en
OBJS = $(patsubst %.rs,%.o,$(wildcard src/*.rs))

REGRESS = reranking_functions reranking_functions_enhanced
REGRESS_OPTS = --load-extension=vector --load-extension=rag_jina_reranker_v1_tiny_en

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
