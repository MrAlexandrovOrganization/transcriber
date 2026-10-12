# Read only non-secret selectors; never source/evaluate the whole .env.
# Use unquoted assignments, or a Make/environment override.
ENV_FILE ?= .env
TRANSCRIBER_ROLE ?= $(or $(shell sed -n 's/^TRANSCRIBER_ROLE=//p' $(ENV_FILE) 2>/dev/null),server)
WORKER_API_ENABLED ?= $(shell sed -n 's/^WORKER_API_ENABLED=//p' $(ENV_FILE) 2>/dev/null)
ifneq ($(filter-out server workers,$(strip $(TRANSCRIBER_ROLE))),)
$(error TRANSCRIBER_ROLE must be an unquoted server or workers)
endif
ifneq ($(filter-out 0 1,$(strip $(WORKER_API_ENABLED))),)
$(error WORKER_API_ENABLED must be an unquoted 0 or 1)
endif
COMPOSE_FILES = -f docker-compose.yml
ifeq ($(strip $(TRANSCRIBER_ROLE)),workers)
COMPOSE_FILES = -f compose.workers.yml
else
ifeq ($(strip $(WORKER_API_ENABLED)),1)
COMPOSE_FILES += -f compose.worker-api.yml
endif
endif
DOCKER_COMPOSE = docker compose $(if $(wildcard $(ENV_FILE)),--env-file "$(ENV_FILE)") $(COMPOSE_FILES)
# Timestamped attestations change local image IDs even for a cached build,
# causing an unnecessary Compose restart on every make up.
BUILDX_NO_DEFAULT_ATTESTATIONS ?= 1
export BUILDX_NO_DEFAULT_ATTESTATIONS
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
ifeq ($(strip $(TRANSCRIBER_ROLE)),workers)
	$(DOCKER_COMPOSE) up -d --build --remove-orphans --wait --wait-timeout 180
else
	$(DOCKER_COMPOSE) up -d --build --remove-orphans
endif
ifeq ($(strip $(TRANSCRIBER_ROLE)),workers)
	@set -e; for service in worker-cpu worker-gpu; do \
		if ! $(DOCKER_COMPOSE) config --services | grep -qx "$$service"; then \
			$(DOCKER_COMPOSE) --profile '*' rm --stop --force "$$service"; \
		fi; \
	done
endif

.PHONY: down
down:
ifeq ($(strip $(TRANSCRIBER_ROLE)),workers)
	$(DOCKER_COMPOSE) --profile '*' down
else
	$(DOCKER_COMPOSE) down
endif

.PHONY: logs
logs:
	$(DOCKER_COMPOSE) logs -f

.PHONY: deploy
deploy:
	$(MAKE) up

.PHONY: restart
restart:
	$(DOCKER_COMPOSE) restart

ps:
	$(DOCKER_COMPOSE) ps -a

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

compose-build: build

build-workers:
	$(MAKE) ENV_FILE=.env.wsl.example TRANSCRIBER_ROLE=workers build

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
ifeq ($(strip $(TRANSCRIBER_ROLE)),workers)
	@$(DOCKER_COMPOSE) config --services | grep -Eq '^worker-(cpu|gpu)$$' || (echo 'Enable cpu and/or gpu in COMPOSE_PROFILES'; exit 1)
endif

worker-api-config-check:
	docker compose -f docker-compose.yml -f compose.worker-api.yml config --quiet

up-worker-api: worker-api-config-check
	$(MAKE) WORKER_API_ENABLED=1 up

config-check-examples:
	$(MAKE) ENV_FILE=.env.mac.example TRANSCRIBER_ROLE=workers config-check
	$(MAKE) ENV_FILE=.env.wsl.example TRANSCRIBER_ROLE=workers config-check
	$(MAKE) ENV_FILE=.env.server.example TRANSCRIBER_ROLE=server WORKER_API_ENABLED=1 config-check

check: proto-gen proto-check config-check config-check-examples
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

.PHONY: compose-build build-workers config-check-examples ps install-poetry proto-gen proto-check test test-integration test-db-up test-db-down config-check worker-api-config-check up-worker-api check run migrate run-remote-worker pilot ci-versions
