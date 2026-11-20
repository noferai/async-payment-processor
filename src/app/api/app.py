import importlib.metadata
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import APIRouter, Depends, FastAPI, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.exception_handlers import request_validation_exception_handler
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from app.api import payments
from app.api.deps import require_api_key
from app.api.schemas import ErrorResponse, WebhookPayload
from app.config import Settings, get_settings
from app.db.session import create_engine, create_session_factory

API_DESCRIPTION = """\
Приём платежей с асинхронной обработкой. `POST /api/v1/payments` сразу отвечает `202`,
платёж обрабатывается в фоне, итог приходит webhook'ом (раздел **Webhooks** ниже).

Все запросы — с заголовком `X-API-Key` (кнопка **Authorize**).
"""


async def _payment_processed_webhook(body: WebhookPayload) -> None:
    """Только описание webhook в OpenAPI: сервис такой запрос отправляет, а не принимает."""


async def validation_error_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    try:
        return await request_validation_exception_handler(request, exc)
    except ValueError:
        # Отвергнутый ввод с NaN или непарным суррогатом не сериализуется в JSON-ответ:
        # ошибки возвращаются без поля input
        errors = [{k: v for k, v in error.items() if k != "input"} for error in exc.errors()]
        return JSONResponse(status_code=422, content={"detail": jsonable_encoder(errors)})


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        engine = create_engine(settings)
        app.state.settings = settings
        app.state.session_factory = create_session_factory(engine)
        try:
            yield
        finally:
            await engine.dispose()

    app = FastAPI(
        title="Async Payment Processor",
        version=importlib.metadata.version("async-payment-processor"),
        description=API_DESCRIPTION,
        lifespan=lifespan,
    )
    app.exception_handler(RequestValidationError)(validation_error_handler)
    app.webhooks.post(
        "payment-processed",
        summary="Итог платежа",
        description="Сервис отправляет POST на `webhook_url` после обработки платежа. Доставленным "
        "считается любой ответ 2xx, редиректы не выполняются. Доставка at-least-once: "
        "дубль отсеивается по `payment_id`.",
        operation_id="paymentProcessed",
        response_description="Любой ответ 2xx: webhook доставлен.",
        responses={
            "default": {
                "description": "Любой другой ответ, таймаут или редирект — сбой доставки, "
                "webhook будет отправлен повторно."
            }
        },
    )(_payment_processed_webhook)
    api = APIRouter(
        prefix="/api/v1",
        dependencies=[Depends(require_api_key)],
        responses={
            status.HTTP_401_UNAUTHORIZED: {
                "model": ErrorResponse,
                "description": "Нет заголовка X-API-Key или ключ неверный.",
            }
        },
    )
    api.include_router(payments.router)
    app.include_router(api)
    return app
