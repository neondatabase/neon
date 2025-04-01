EXTENSION = rag_bge_small_en_v15
MODULE_big = rag_bge_small_en_v15
OBJS = $(patsubst %.rs,%.o,$(wildcard src/*.rs))

REGRESS = basic_functions embedding_functions basic_functions_enhanced embedding_functions_enhanced
REGRESS_OPTS = --load-extension=vector --load-extension=rag_bge_small_en_v15

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
