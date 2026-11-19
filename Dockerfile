FROM python:3.14-slim

COPY --from=ghcr.io/astral-sh/uv:0.13.9 /uv /bin/uv

ENV PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app

# Сначала только зависимости: слой кэшируется, пока не меняется lock-файл
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY src/ src/
COPY migrations/ migrations/
COPY alembic.ini ./
COPY devtools/ devtools/
RUN uv sync --frozen --no-dev

RUN useradd --system --no-create-home --uid 1000 app
USER app
