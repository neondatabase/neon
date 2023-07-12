
BINARY := sloth.git/bin/sloth-linux-amd64

.PHONY: all
all: pageserver-slo.dev.sloth-output.yaml pageserver-slo.prod.sloth-output.yaml

%.sloth-output.yaml: %.sloth.yaml $(BINARY)
	$(BINARY) generate  \
		--disable-promExpr-validation \
		--input $< \
		--out $@

CLOUD_GIT_CHECKOUT := /nonexistent

.PHONY: sync-to-cloud-git
sync-to-cloud-git: pageserver-slo.dev.sloth-output.yaml pageserver-slo.prod.sloth-output.yaml
	cp pageserver-slo.dev.sloth-output.yaml \
		$(CLOUD_GIT_CHECKOUT)/ops/infra/workloads/values/dev-eu-central-1-alpha/neon-vm/pageserver-slo.dev.sloth-output.yaml
	cp pageserver-slo.prod.sloth-output.yaml \
		$(CLOUD_GIT_CHECKOUT)/ops/infra/workloads/values/prod-eu-central-1-gamma/neon-vm/pageserver-slo.prod.sloth-output.yaml
