UV ?= uv


.PHONY: all
all:
	@echo "Run my targets individually!"

.PHONY: develop
develop:
	$(UV) sync --locked

.PHONY: test
test: develop
	$(UV) run --locked python -m unittest

.PHONY: typecheck
typecheck:
	$(UV) run --locked ty check test

.PHONY: dist
dist: dist-pyrage dist-pyrage-stubs

.PHONY: dist-pyrage
dist-pyrage:
	docker run --rm -v $(shell pwd):/io ghcr.io/pyo3/maturin build --release --sdist --strip --out dist

.PHONY: dist-pyrage-stubs
dist-pyrage-stubs:
	$(UV) build ./pyrage-stubs --out-dir dist

BENCH = $(UV) run --locked --with uvloop python bench/bench_io.py

.PHONY: bench-data
bench-data: develop
	$(BENCH) prepare

.PHONY: bench
bench: bench-data
	$(BENCH) run $(BENCH_ARGS)
