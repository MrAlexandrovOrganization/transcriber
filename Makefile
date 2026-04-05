DOCKER_COMPOSE = docker compose

.PHONY: install
install:
	poetry install

# Validate proto (buf must be installed: brew install bufbuild/buf/buf).
.PHONY: proto-lint
proto-lint:
	buf lint proto

# Regenerate Python gRPC stubs from proto/whisper.proto.
# proto/whisper.proto is the canonical source — edit it here, then run make proto.
.PHONY: proto
proto: proto-lint
	.venv/bin/python -m grpc_tools.protoc \
		-I . \
		--python_out=. \
		--grpc_python_out=. \
		proto/whisper.proto

.PHONY: up
up:
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
	$(DOCKER_COMPOSE) restart whisper

.PHONY: clean
clean:
	find . -type f -name '*.pyc' -delete
	find . -type d -name '__pycache__' -exec rm -rf {} +
