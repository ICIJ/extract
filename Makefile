VERSION = $(shell head pom.xml | grep '<version>[0-9.]\+' | sed -E 's/<version>([0-9a-z.-]+)<\/version>/\1/' | tr -d '[:space:]')
MVN = mvn

.PHONY: help build dist install test clean release

help:
	@echo "Extract Makefile - Available targets:"
	@echo ""
	@echo "  Development:"
	@echo "    install   - Install all modules to the local Maven repository"
	@echo "    build     - Build distribution JARs (alias for 'dist')"
	@echo "    dist      - Package the distribution (skips tests)"
	@echo "    test      - Run all tests (requires a running Redis)"
	@echo "    clean     - Clean all build artifacts"
	@echo ""
	@echo "  Release:"
	@echo "    release   - Create a new release (requires NEW_VERSION=x.y.z)"
	@echo ""

## Install all modules to the local Maven repository
install:
	$(MVN) install

## Build distribution JARs (alias for dist)
build: dist

## Package the distribution (skips tests)
dist:
	$(MVN) validate package -Dmaven.test.skip=true

## Run all tests
test:
	$(MVN) test

## Clean all build artifacts
clean:
	$(MVN) clean

## Create a new release (usage: make release NEW_VERSION=x.y.z)
release:
ifndef NEW_VERSION
	$(error NEW_VERSION is required. Usage: make release NEW_VERSION=x.y.z)
endif
	$(MVN) versions:set -DnewVersion=$(NEW_VERSION)
	git commit -am "[release] $(NEW_VERSION) [skip ci]"
	git tag $(NEW_VERSION)
	@echo "Release $(NEW_VERSION) created. Push with: git push origin master --tags"
