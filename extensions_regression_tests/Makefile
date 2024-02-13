#initdb -D /usr/local/data

.PHONY: extensions-test-all
extensions-test-all:
	mkdir ../data
	#initdb -d -D ../data
	#postgres -D ../data >logfile 2>&1 &
	pg_ctl -D ../data initdb
	pg_ctl start -D ../data -l serverlog


	 cd extensions/pgvector/ && $(MAKE) installcheck && $(MAKE) prove_installcheck
	( cd extensions/pg_graphql/ && ./bin/installcheck )
