# Application image: simulator, topic/schema bootstrap, loader, DLQ and reconciliation CLIs.
FROM python:3.14-slim-bookworm AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    PATH=/opt/venv/bin:$PATH

RUN pip install --no-cache-dir uv==0.12.20 \
 && useradd --uid 10001 --create-home --shell /usr/sbin/nologin app

WORKDIR /app

# Dependencies first (cached layer), then the project itself.
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project --extra snowflake
COPY src ./src
RUN uv sync --frozen --no-dev --extra snowflake

# Language-neutral contracts and SQL are runtime inputs.
COPY kafka ./kafka
COPY snowflake ./snowflake

RUN mkdir -p /app/data && chown -R app:app /app/data
USER app
