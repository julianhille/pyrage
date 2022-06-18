.PHONY: all
all:
	@echo "Run my targets individually!"

.PHONY: env
env: env/pyvenv.cfg

env/pyvenv.cfg:
	python -m venv env
	./env/bin/python -m pip install --upgrade pip
	./env/bin/python -m pip install maturin

.PHONY: develop
develop: env/pyvenv.cfg
	./env/bin/maturin develop

.PHONY: build
build: env/pyvenv.cfg
	./env/bin/maturin build
