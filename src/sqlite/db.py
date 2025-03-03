from typing import Optional

from sqlalchemy.ext.asyncio import (
    async_sessionmaker, create_async_engine, AsyncEngine
)
from sqlalchemy.pool import AsyncAdaptedQueuePool


class DatabaseManageException(Exception): ...


class AsyncSQLiteManage:
    def __init__(self):
        self.engine: Optional[AsyncEngine] = None
        self.async_session_maker: Optional[async_sessionmaker] = None

    def init(self, url: str, echo: bool = False):
        # NOTE: For SQLite, we need to use aiosqlite as the async driver
        #   - Using check_same_thread=False to allow multiple threads to access
        #     the db
        #   - Using pool-class to handle connection pooling for concurrent
        #     access
        self.engine = create_async_engine(
            url,
            echo=echo,
            connect_args={"check_same_thread": False},
            poolclass=AsyncAdaptedQueuePool,
            pool_pre_ping=False,
            # NOTE: Maximum number of connections in the pool
            pool_size=20,
            # NOTE: Maximum number of connections that can be created
            #   beyond pool_size.
            max_overflow=30,
        )
        self.async_session_maker = async_sessionmaker(
            autocommit=False,
            expire_on_commit=False,
            bind=self.engine,
        )
        print("Init database manage success")

    async def initialize(self):
        """Create all tables defined in the models"""
        from .models import Base

        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def close(self):
        """Close all connections in the engine"""
        if self.engine is None:
            raise DatabaseManageException(
                "DatabaseSessionManager is not initialized"
            )
        await self.engine.dispose()
        self.engine = None
        self.async_session_maker = None

    def is_opened(self) -> bool:
        return self.engine is not None
