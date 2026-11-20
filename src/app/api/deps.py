import secrets
from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import Depends, Header, HTTPException, Request, status
from fastapi.security import APIKeyHeader
from sqlalchemy.ext.asyncio import AsyncSession

_api_key_header = APIKeyHeader(
    name="X-API-Key", auto_error=False, description="Ключ API сервиса, задаётся переменной API_KEY."
)


async def require_api_key(request: Request, api_key: Annotated[str | None, Depends(_api_key_header)]) -> None:
    expected: str = request.app.state.settings.api_key
    if api_key is None or not secrets.compare_digest(api_key.encode(), expected.encode()):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="Неверный или отсутствующий X-API-Key")


async def get_session(request: Request) -> AsyncIterator[AsyncSession]:
    async with request.app.state.session_factory() as session:
        yield session


SessionDep = Annotated[AsyncSession, Depends(get_session)]
IdempotencyKey = Annotated[
    str,
    Header(
        alias="Idempotency-Key",
        min_length=1,
        max_length=255,
        description="Ключ идемпотентности, 1–255 символов. Повтор с тем же ключом и телом "
        "возвращает тот же платёж, с другим телом — 409.",
    ),
]
