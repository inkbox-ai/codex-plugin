# syntax=docker/dockerfile:1.7

# Local/manual test image only. It preinstalls Codex and this plugin; credentials
# are supplied at runtime and are never copied into the image.
FROM python:3.12-slim-bookworm

ARG CODEX_VERSION="0.146.0"

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl git \
    && curl -fsSL https://deb.nodesource.com/setup_22.x | bash - \
    && apt-get install -y --no-install-recommends nodejs \
    && npm install --global "@openai/codex@${CODEX_VERSION}" \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/inkbox-codex-src
COPY . .
RUN python -m pip install --no-cache-dir --editable .

ENV INKBOX_CODEX_HOME="/root/.inkbox-codex"
RUN mkdir -p /root/.codex /root/.inkbox-codex /workspace
WORKDIR /workspace

CMD ["bash"]
