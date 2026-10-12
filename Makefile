# Read only non-secret selectors; never source/evaluate the whole .env.
# Use unquoted assignments, or a Make/environment override.
ENV_FILE ?= .env
MONITORING_DATA_DIR ?= $(HOME)/.monitoring
export MONITORING_DATA_DIR
TRANSCRIBER_ROLE ?= $(or $(shell sed -n 's/^TRANSCRIBER_ROLE=//p' $(ENV_FILE) 2>/dev/null),server)
WORKER_AUTODEPLOY ?= $(or $(shell sed -n 's/^WORKER_AUTODEPLOY=//p' $(ENV_FILE) 2>/dev/null),0)
WORKER_REGISTRY ?= ghcr.io/mralexandrovorganization/transcriber
export WORKER_REGISTRY
WORKER_RELEASE_FILE ?= .worker-release.env
WORKER_RELEASE_ENV = $(if $(wildcard $(WORKER_RELEASE_FILE)),--env-file "$(WORKER_RELEASE_FILE)")
WORKER_TRANSPORT ?= $(or $(shell sed -n 's/^WORKER_TRANSPORT=//p' $(ENV_FILE) 2>/dev/null),tls)
WORKER_TLS_ENABLED ?= $(shell sed -n 's/^WORKER_TLS_ENABLED=//p' $(ENV_FILE) 2>/dev/null)
WORKER_API_ENABLED ?= $(shell sed -n 's/^WORKER_API_ENABLED=//p' $(ENV_FILE) 2>/dev/null)
ifneq ($(filter-out server workers,$(strip $(TRANSCRIBER_ROLE))),)
$(error TRANSCRIBER_ROLE must be an unquoted server or workers)
endif
ifneq ($(filter-out 0 1,$(strip $(WORKER_API_ENABLED))),)
$(error WORKER_API_ENABLED must be an unquoted 0 or 1)
endif
ifneq ($(filter-out tls ssh,$(strip $(WORKER_TRANSPORT))),)
$(error WORKER_TRANSPORT must be an unquoted tls or ssh)
endif
ifneq ($(filter-out 0 1,$(strip $(WORKER_TLS_ENABLED))),)
$(error WORKER_TLS_ENABLED must be an unquoted 0 or 1)
endif
COMPOSE_FILES = -f docker-compose.yml
ifeq ($(strip $(TRANSCRIBER_ROLE)),workers)
COMPOSE_FILES = -f compose.workers.yml
ifeq ($(strip $(WORKER_TRANSPORT)),ssh)
COMPOSE_FILES += -f compose.workers.ssh.yml
endif
ifeq ($(strip $(WORKER_AUTODEPLOY)),1)
COMPOSE_FILES += -f compose.workers.autodeploy.yml
else
WORKER_RELEASE_ENV =
endif
else
ifeq ($(strip $(WORKER_API_ENABLED)),1)
COMPOSE_FILES += -f compose.worker-api.yml
endif
ifeq ($(strip $(WORKER_TLS_ENABLED)),1)
COMPOSE_FILES += -f compose.worker-tls.yml
endif
endif
DOCKER_COMPOSE = docker compose $(WORKER_RELEASE_ENV) $(if $(wildcard $(ENV_FILE)),--env-file "$(ENV_FILE)") $(COMPOSE_FILES)
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
up:
ifeq ($(strip $(TRANSCRIBER_ROLE)):$(strip $(WORKER_AUTODEPLOY)),workers:1)
	$(MAKE) worker-release
endif
	$(MAKE) up-selected

up-selected: config-check
ifeq ($(strip $(TRANSCRIBER_ROLE)):$(strip $(WORKER_AUTODEPLOY)),workers:1)
	$(DOCKER_COMPOSE) stop worker-updater
endif
ifeq ($(strip $(TRANSCRIBER_ROLE)),workers)
	$(DOCKER_COMPOSE) up -d --build --remove-orphans --wait --wait-timeout 180
else
	$(DOCKER_COMPOSE) up -d --build --remove-orphans
	$(MAKE) monitoring-install
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
ifeq ($(strip $(WORKER_AUTODEPLOY)),1)
	$(DOCKER_COMPOSE) stop worker-updater
endif
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
	$(DOCKER_COMPOSE) up -d --wait --wait-timeout 240

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
	$(DOCKER_COMPOSE) config --format json | python3 scripts/validate_workers.py
endif

worker-api-config-check:
	docker compose -f docker-compose.yml -f compose.worker-api.yml config --quiet

up-worker-api: worker-api-config-check
	$(MAKE) WORKER_API_ENABLED=1 up

config-check-examples:
	$(MAKE) ENV_FILE=.env.mac.example TRANSCRIBER_ROLE=workers config-check
	$(MAKE) ENV_FILE=.env.wsl.example TRANSCRIBER_ROLE=workers config-check
	$(MAKE) ENV_FILE=.env.server.example TRANSCRIBER_ROLE=server WORKER_API_ENABLED=0 WORKER_TLS_ENABLED=1 config-check

check: proto-gen proto-check config-check config-check-examples monitoring-check
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
	@printf 'python=%s\npoetry=%s\nbuf=%s\npostgres=%s\nbuildx=%s\n' '$(PYTHON_VERSION)' '$(POETRY_VERSION)' '$(BUF_VERSION)' '$(POSTGRES_VERSION)' '$(BUILDX_VERSION)'

.PHONY: compose-build build-workers config-check-examples ps install-poetry proto-gen proto-check test test-integration test-db-up test-db-down config-check worker-api-config-check up-worker-api check run migrate run-remote-worker pilot ci-versions

# CI and local bootstrap use the same publishing/resolution code.
SOURCE_REVISION ?= $(shell git rev-parse HEAD)
export SOURCE_REVISION
worker-release:
	python3 scripts/worker_release.py resolve

publish-workers:
	python3 scripts/worker_release.py publish --arch $(PUBLISH_ARCH)

promote-workers:
	python3 scripts/worker_release.py promote

.PHONY: up-selected worker-release publish-workers promote-workers

test-updater-docker:
	python3 scripts/smoke_updater.py
	SMOKE_NETWORK=shared python3 scripts/smoke_updater.py

.PHONY: test-updater-docker

# Update only the bootstrap controller, preserving running inference containers.
up-updater: config-check
	@test '$(TRANSCRIBER_ROLE):$(WORKER_AUTODEPLOY)' = workers:1 || (echo 'Requires WORKER_AUTODEPLOY=1 in worker mode'; exit 1)
	$(DOCKER_COMPOSE) up -d --build --no-deps --wait --wait-timeout 180 worker-updater

.PHONY: up-updater

# Real TLS/HTTP2 proxy check; synthetic data, no production queue or credentials.
test-worker-tls-docker:
	poetry run python -m scripts.smoke_worker_tls

restart-worker-api:
	@test '$(TRANSCRIBER_ROLE)' = server || (echo 'Requires server role'; exit 1)
	$(DOCKER_COMPOSE) up -d --no-deps --force-recreate --wait --wait-timeout 240 whisper-ingress

.PHONY: test-worker-tls-docker restart-worker-api

dashboards:
	python3 scripts/render_dashboards.py

monitoring-check:
	python3 scripts/render_dashboards.py --check
	python3 scripts/render_dashboards.py --rules | docker run --rm -i --entrypoint promtool prom/prometheus:$(PROMETHEUS_CHECK_VERSION) check rules /dev/stdin

monitoring-install:
	@test '$(TRANSCRIBER_ROLE)' = server || (echo 'Requires server role'; exit 1)
	python3 scripts/install_monitoring.py

.PHONY: dashboards monitoring-check monitoring-install
