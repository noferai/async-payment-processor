"""Фабрика FastAPI-приложения."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import APIRouter, Depends, FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exception_handlers import request_validation_exception_handler
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from app.api import payments
from app.api.deps import require_api_key
from app.config import Settings, get_settings
from app.db.session import create_engine, create_session_factory


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

    app = FastAPI(title="Async Payment Processor", lifespan=lifespan)
    app.exception_handler(RequestValidationError)(validation_error_handler)
    api = APIRouter(prefix="/api/v1", dependencies=[Depends(require_api_key)])
    api.include_router(payments.router)
    app.include_router(api)
    return app
