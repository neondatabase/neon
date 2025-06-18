jsonnet_files = $(wildcard \
	etc/*.jsonnet \
	etc/sql_exporter/*.libsonnet)

.PHONY: all
all: neon_collector.yml neon_collector_autoscaling.yml sql_exporter.yml sql_exporter_autoscaling.yml

neon_collector.yml: $(jsonnet_files)
	JSONNET_PATH=jsonnet:etc jsonnet \
		--output-file etc/$@ \
		--ext-str pg_version=$(PG_VERSION) \
		etc/neon_collector.jsonnet

neon_collector_autoscaling.yml: $(jsonnet_files)
	JSONNET_PATH=jsonnet:etc jsonnet \
		--output-file etc/$@ \
		--ext-str pg_version=$(PG_VERSION) \
		etc/neon_collector_autoscaling.jsonnet

sql_exporter.yml: $(jsonnet_files)
	JSONNET_PATH=etc jsonnet \
		--output-file etc/$@ \
		--tla-str collector_name=neon_collector \
		--tla-str collector_file=neon_collector.yml \
		--tla-str 'connection_string=postgresql://cloud_admin@127.0.0.1:5432/postgres?sslmode=disable&application_name=sql_exporter&pgaudit.log=none' \
		etc/sql_exporter.jsonnet

sql_exporter_autoscaling.yml: $(jsonnet_files)
	JSONNET_PATH=etc jsonnet \
		--output-file etc/$@ \
		--tla-str collector_name=neon_collector_autoscaling \
		--tla-str collector_file=neon_collector_autoscaling.yml \
		--tla-str 'connection_string=postgresql://cloud_admin@127.0.0.1:5432/postgres?sslmode=disable&application_name=sql_exporter_autoscaling&pgaudit.log=none' \
		etc/sql_exporter.jsonnet

.PHONY: clean
clean:
	$(RM) \
		etc/neon_collector.yml \
		etc/neon_collector_autoscaling.yml \
		etc/sql_exporter.yml \
		etc/sql_exporter_autoscaling.yml

.PHONY: jsonnetfmt-test
jsonnetfmt-test:
	jsonnetfmt --test $(jsonnet_files)

.PHONY: jsonnetfmt-format
jsonnetfmt-format:
	jsonnetfmt --in-place $(jsonnet_files)

.PHONY: manifest-schema-validation
manifest-schema-validation: node_modules
	node_modules/.bin/jsonschema validate -d https://json-schema.org/draft/2020-12/schema manifest.schema.json manifest.yaml

node_modules: package.json
	npm install
	touch node_modules
