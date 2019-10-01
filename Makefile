.PHONY: default test dist install dev
default: help

test: ## launch package tests
	@pytest

dist: ## run tests and build a package distribution
	@pytest && python setup.py sdist bdist_wheel

install: ## install package
	@pip install .

dev: ## install package in development mode
	@pip install --upgrade -e .[testing]

start:
	@docker run --name elastic \
	    -d -p 9200:9200 \
	    -p 9300:9300 \
	    -e "discovery.type=single-node" \
	    docker.elastic.co/elasticsearch/elasticsearch:7.4.0

stop:
	@docker stop elastic
	@docker rm elastic


.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
