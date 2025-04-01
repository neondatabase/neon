EXTENSION = rag
MODULE_big = rag
OBJS = $(patsubst %.rs,%.o,$(wildcard src/*.rs))

REGRESS = basic_functions text_processing api_keys chunking_functions document_processing embedding_api_functions voyageai_functions
REGRESS_OPTS = --load-extension=vector --load-extension=rag

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
