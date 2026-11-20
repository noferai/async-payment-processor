from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.config import Settings


def create_engine(settings: Settings) -> AsyncEngine:
    # pre_ping: после перезапуска PostgreSQL закрытое соединение из пула заменяется,
    # а не роняет первый запрос
    return create_async_engine(settings.database_url, pool_size=settings.db_pool_size, pool_pre_ping=True)


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)
