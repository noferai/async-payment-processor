"""Приёмник webhook'ов для демонстрации: `/ok` отвечает 200, `/fail` — всегда 500.

Запуск: `uvicorn devtools.webhook_mock:app --host 0.0.0.0 --port 8080`.
"""

import logging

from fastapi import FastAPI, Request, Response

# Под uvicorn выводится лог "uvicorn.error"; обычный logging.info не виден
logger = logging.getLogger("uvicorn.error")

app = FastAPI(title="webhook mock")


async def _log_body(request: Request, path: str) -> None:
    body = await request.body()
    logger.info("webhook %s: %s", path, body.decode(errors="replace"))


@app.post("/ok")
async def ok(request: Request) -> Response:
    await _log_body(request, "/ok")
    return Response(status_code=200)


@app.post("/fail")
async def fail(request: Request) -> Response:
    await _log_body(request, "/fail")
    return Response(status_code=500)
