jsonnet_files = $(wildcard etc/*.jsonnet etc/*.libsonnet)

.PHONY: all
all: neon_collector.yml neon_collector_autoscaling.yml sql_exporter.yml sql_exporter_autoscaling.yml

neon_collector.yml: $(jsonnet_files)
	JSONNET_PATH=etc jsonnet \
		--output-file etc/$@ \
		etc/neon_collector.jsonnet

neon_collector_autoscaling.yml: $(jsonnet_files)
	JSONNET_PATH=etc jsonnet \
		--output-file etc/$@ \
		etc/neon_collector_autoscaling.jsonnet

sql_exporter.yml: $(jsonnet_files)
	JSONNET_PATH=etc jsonnet \
		--output-file etc/$@ \
		--tla-str collector_file=neon_collector.yml \
		etc/sql_exporter.jsonnet

sql_exporter_autoscaling.yml: $(jsonnet_files)
	JSONNET_PATH=etc jsonnet \
		--output-file etc/$@ \
		--tla-str collector_file=neon_collector_autoscaling.yml \
		--tla-str application_name=sql_exporter_autoscaling \
		etc/sql_exporter.jsonnet

.PHONY: clean
clean:
	rm --force \
		etc/neon_collector.yml \
		etc/neon_collector_autoscaling.yml \
		etc/sql_exporter.yml \
		etc/sql_exporter_autoscaling.yml
