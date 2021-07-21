all:
	@echo "No default command"

.PHONY: fmt
fmt: pipenv-install
	pipenv run yapf -r -i $(CURDIR)

.PHONY: mypy
mypy: pipenv-install
	pipenv run mypy $(CURDIR)

.PHONY: test
test: pipenv-install
	pipenv run pytest $(CURDIR)

.PHONY: pipenv-install
pipenv-install:
	pipenv install --dev --ignore-pipfile
