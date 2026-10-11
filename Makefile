# Read only this non-secret opt-in flag; never source/evaluate the whole .env.
# Use an unquoted WORKER_API_ENABLED=1 line, or a Make/environment override.
WORKER_API_ENABLED ?= $(shell sed -n 's/^WORKER_API_ENABLED=//p' .env 2>/dev/null)
ifneq ($(filter-out 0 1,$(strip $(WORKER_API_ENABLED))),)
$(error WORKER_API_ENABLED must be an unquoted 0 or 1)
endif
COMPOSE_FILES = -f docker-compose.yml
ifeq ($(strip $(WORKER_API_ENABLED)),1)
COMPOSE_FILES += -f compose.worker-api.yml
endif
DOCKER_COMPOSE = docker compose $(COMPOSE_FILES)
include versions.mk

.PHONY: install
install:
	poetry install

install-poetry:
	python -m pip install poetry==$(POETRY_VERSION)

# Validate proto (buf must be installed: brew install bufbuild/buf/buf).
.PHONY: proto-lint
proto-lint:
	buf lint proto

# Regenerate Python gRPC stubs from proto/whisper.proto.
# proto/whisper.proto is the canonical source — edit it here, then run make proto.
.PHONY: proto
proto: proto-lint
	poetry run python -m grpc_tools.protoc \
		-I . \
		--python_out=. \
		--grpc_python_out=. \
		--pyi_out=. \
		proto/whisper.proto

.PHONY: up
up: config-check
	$(DOCKER_COMPOSE) up -d --build

.PHONY: down
down:
	$(DOCKER_COMPOSE) down

.PHONY: logs
logs:
	$(DOCKER_COMPOSE) logs -f

.PHONY: deploy
deploy:
	$(DOCKER_COMPOSE) up -d --build --no-cache

.PHONY: restart
restart:
	$(DOCKER_COMPOSE) restart whisper-ingress whisper-worker

.PHONY: format
format:
	poetry run ruff check whisper tests scripts --fix
	poetry run ruff format whisper tests scripts
	buf format -w proto

.PHONY: clean
clean:
	find . -type f -name '*.pyc' -delete
	find . -type d -name '__pycache__' -exec rm -rf {} +

.PHONY: versions build
versions:
	@$(foreach v,$(VERSION_VARS),printf '%s=%s\n' '$(v)' '$($(v))';)
build:
	$(DOCKER_COMPOSE) build

# Generated Python files are ignored; generation never fetches a sibling repo.
proto-gen: proto

PROTO_BREAKING_AGAINST ?= .git\#ref=$(shell git rev-parse HEAD^),subdir=proto
proto-check:
	buf format --diff --exit-code proto
	buf build proto -o /dev/null
	buf lint proto
	buf breaking proto --against '$(PROTO_BREAKING_AGAINST)'

test:
	poetry run python -m unittest discover -s tests -v

test-integration:
	@test -n "$(TRANSCRIBER_TEST_DATABASE_URL)" || (echo 'Set TRANSCRIBER_TEST_DATABASE_URL to an isolated test database'; exit 1)
	poetry run python -m unittest discover -s tests -p test_worker_api.py -v

test-db-up:
	docker run --rm -d --name transcriber-ci-test-db --label purpose=transcriber-ci-test-db \
		-e POSTGRES_USER=pilot -e POSTGRES_PASSWORD=synthetic-ci-password -e POSTGRES_DB=transcriber_pilot \
		-p 127.0.0.1:55439:5432 --tmpfs /var/lib/postgresql/data:rw postgres:$(POSTGRES_VERSION)
	@for attempt in $$(seq 1 30); do \
		docker exec transcriber-ci-test-db pg_isready -h 127.0.0.1 -U pilot -d transcriber_pilot >/dev/null 2>&1 && exit 0; \
		sleep 1; \
	done; exit 1

test-db-down:
	@if [ "$$(docker inspect --format '{{ index .Config.Labels "purpose" }}' transcriber-ci-test-db 2>/dev/null)" = 'transcriber-ci-test-db' ]; then \
		docker stop transcriber-ci-test-db; \
	fi

config-check:
	$(DOCKER_COMPOSE) config --quiet

worker-api-config-check:
	docker compose -f docker-compose.yml -f compose.worker-api.yml config --quiet

up-worker-api: worker-api-config-check
	$(MAKE) WORKER_API_ENABLED=1 up

check: proto-gen proto-check config-check
	poetry run ruff check whisper tests scripts
	poetry run ruff format --check whisper tests scripts
	$(MAKE) test

run:
	poetry run python -m whisper.main

migrate:
	poetry run python -m whisper.migrate

run-remote-worker: proto-gen
	poetry run python -m whisper.remote_worker

# Fully isolated synthetic experiment; no existing stack or database is used.
pilot: proto-gen
	@test -n "$(PILOT_AUDIO_FILE)" || (echo 'Set PILOT_AUDIO_FILE to synthetic speech audio'; exit 1)
	poetry run python scripts/pilot_worker.py --audio '$(PILOT_AUDIO_FILE)'

ci-versions:
	@printf 'python=%s\npoetry=%s\nbuf=%s\npostgres=%s\n' '$(PYTHON_VERSION)' '$(POETRY_VERSION)' '$(BUF_VERSION)' '$(POSTGRES_VERSION)'

.PHONY: install-poetry proto-gen proto-check test test-integration test-db-up test-db-down config-check worker-api-config-check up-worker-api check run migrate run-remote-worker pilot ci-versions
