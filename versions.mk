PYTHON_VERSION := 3.11
POETRY_VERSION := 2.2.1
POSTGRES_VERSION := 16-alpine
BUF_VERSION := 1.67.0
# Use the exact generator selected by Poetry, not a second independent pin.
GRPCIO_TOOLS_VERSION := $(shell awk '/^name = "grpcio-tools"$$/ {found=1; next} found && /^version = / {gsub(/"/, "", $$3); print $$3; exit}' poetry.lock)
VERSION_VARS := PYTHON_VERSION POETRY_VERSION POSTGRES_VERSION BUF_VERSION GRPCIO_TOOLS_VERSION
export $(VERSION_VARS)
