PG_CONFIG ?= pg_config
PG_REGRESS = $(shell dirname $$($(PG_CONFIG) --pgxs))/../../src/test/regress/pg_regress
REGRESS = pg_tiktoken

installcheck: regression-test

regression-test:
	$(PG_REGRESS) --inputdir=. --outputdir=. --dbname=contrib_regression $(REGRESS)