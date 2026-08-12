# ---- frontend build ----
FROM node:22-alpine AS frontend
WORKDIR /build/frontend
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci --ignore-scripts
COPY frontend/ ./
COPY src/embyx_manager/__init__.py /build/src/embyx_manager/__init__.py
RUN npm run build

# ---- python build ----
FROM python:3.13-slim AS backend
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/
WORKDIR /build
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
COPY --from=frontend /build/src/embyx_manager/static ./src/embyx_manager/static
RUN uv build --wheel && uv venv /opt/embyx-manager \
    && uv pip install --python /opt/embyx-manager/bin/python --no-cache dist/*.whl

# ---- runtime ----
FROM python:3.13-slim
RUN useradd --create-home --uid 1000 embyx
COPY --from=backend /opt/embyx-manager /opt/embyx-manager
ENV PATH="/opt/embyx-manager/bin:$PATH" \
    EMBYX_MANAGER_HOST=127.0.0.1 \
    EMBYX_MANAGER_PORT=8000
USER embyx
EXPOSE 8000
ENTRYPOINT ["embyx-manager"]
CMD ["serve"]
