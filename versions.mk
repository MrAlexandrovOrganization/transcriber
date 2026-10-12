PYTHON_VERSION := 3.11
POETRY_VERSION := 2.2.1
POSTGRES_VERSION := 16-alpine
BUILDX_VERSION := v0.38.0
BUF_VERSION := 1.67.0
CUDNN_VERSION := 9.8.0.87
CUBLAS_VERSION := 12.8.4.1
# Use the exact generator selected by Poetry, not a second independent pin.
GRPCIO_TOOLS_VERSION := $(shell awk '/^name = "grpcio-tools"$$/ {found=1; next} found && /^version = / {gsub(/"/, "", $$3); print $$3; exit}' poetry.lock)
VERSION_VARS := BUILDX_VERSION PYTHON_VERSION POETRY_VERSION POSTGRES_VERSION BUF_VERSION CUDNN_VERSION CUBLAS_VERSION GRPCIO_TOOLS_VERSION
export $(VERSION_VARS)
