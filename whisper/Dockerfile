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


FROM python:${PYTHON_VERSION}-slim AS runtime

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

FROM runtime AS remote-cpu
ENV PYTHONUNBUFFERED=1
USER 1000:1000
CMD ["python", "-m", "whisper.remote_worker"]

FROM remote-cpu AS remote-gpu
USER root
ARG PYTHON_VERSION
ARG CUDNN_VERSION
ARG CUBLAS_VERSION
RUN pip install --no-cache-dir --no-deps \
    nvidia-cublas-cu12==${CUBLAS_VERSION} nvidia-cudnn-cu12==${CUDNN_VERSION}
ENV LD_LIBRARY_PATH=/usr/local/lib/python${PYTHON_VERSION}/site-packages/nvidia/cublas/lib:/usr/local/lib/python${PYTHON_VERSION}/site-packages/nvidia/cudnn/lib
USER 1000:1000

FROM python:${PYTHON_VERSION}-slim AS tunnel
ARG TUNNEL_UID
ARG TUNNEL_GID
RUN apt-get update && apt-get install -y --no-install-recommends openssh-client \
    && rm -rf /var/lib/apt/lists/* \
    && (getent group ${TUNNEL_GID} || groupadd -g ${TUNNEL_GID} tunnel) \
    && useradd -m -u ${TUNNEL_UID} -g ${TUNNEL_GID} tunnel
WORKDIR /app
COPY whisper/ssh_tunnel.py ./ssh_tunnel.py
USER tunnel
CMD ["python", "ssh_tunnel.py"]
