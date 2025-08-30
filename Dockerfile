FROM python:3.12-slim-bookworm

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=0 \
    PATH="/app/.venv/bin:$PATH"

RUN useradd -m appuser
USER appuser
WORKDIR /home/appuser/app

RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project --no-editable

COPY --chown=appuser:appuser . .

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-editable

ENV PYTHONPATH="${PYTHONPATH}:/home/appuser/app"

ENTRYPOINT ["uv", "run", "anomaly_detection_service/main.py"]
