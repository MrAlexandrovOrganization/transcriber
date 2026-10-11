# check=skip=InvalidDefaultArgInFrom

# Required version arguments are supplied by Make/Compose, without fallback pins.
ARG PYTHON_VERSION
FROM python:${PYTHON_VERSION}-slim AS proto-builder

ARG GRPCIO_TOOLS_VERSION
RUN pip install --no-cache-dir grpcio-tools==${GRPCIO_TOOLS_VERSION}

WORKDIR /proto-build
COPY proto/ ./proto/

RUN python -m grpc_tools.protoc \
        -I . \
        --python_out=. \
        --grpc_python_out=. \
        proto/whisper.proto


FROM python:${PYTHON_VERSION}-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg libsndfile1 \
    && rm -rf /var/lib/apt/lists/*

ARG POETRY_VERSION
RUN pip install --no-cache-dir poetry==${POETRY_VERSION}

WORKDIR /app

COPY pyproject.toml poetry.lock* ./

RUN poetry config virtualenvs.create false && \
    poetry install --only main --no-interaction --no-ansi --no-root

COPY whisper/ ./whisper/
COPY --from=proto-builder /proto-build/proto/ ./proto/

CMD ["python", "-m", "whisper.main"]
