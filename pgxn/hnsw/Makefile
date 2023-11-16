EXTENSION = hnsw
EXTVERSION = 0.1.0

MODULE_big = hnsw
DATA = $(wildcard *--*.sql)
OBJS = hnsw.o hnswalg.o

TESTS = $(wildcard test/sql/*.sql)
REGRESS = $(patsubst test/sql/%.sql,%,$(TESTS))
REGRESS_OPTS = --inputdir=test --load-extension=hnsw

# For auto-vectorization:
# - GCC (needs -ftree-vectorize OR -O3) - https://gcc.gnu.org/projects/tree-ssa/vectorization.html
PG_CFLAGS += -O3
PG_CXXFLAGS +=  -O3 -std=c++11
PG_LDFLAGS += -lstdc++

all: $(EXTENSION)--$(EXTVERSION).sql

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

dist:
	mkdir -p dist
	git archive --format zip --prefix=$(EXTENSION)-$(EXTVERSION)/ --output dist/$(EXTENSION)-$(EXTVERSION).zip master
