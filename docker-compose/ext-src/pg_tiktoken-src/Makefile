PG_CONFIG ?= pg_config
PG_REGRESS = $(shell dirname $$($(PG_CONFIG) --pgxs))/../../src/test/regress/pg_regress
REGRESS = pg_tiktoken

installcheck: regression-test

regression-test:
	dropdb --if-exists contrib_regression
	createdb contrib_regression
	$(PG_REGRESS) --inputdir=. --outputdir=. --use-existing --dbname=contrib_regression $(REGRESS)