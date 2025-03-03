import asyncio
import os
import pytest
import time
from typing import List, AsyncGenerator

from sqlalchemy import Column, Integer, String, select, func
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, \
    AsyncSession
from sqlalchemy.orm import declarative_base

# Define the SQLite database file path
DB_PATH = "test_database.db"
DB_URL = f"sqlite+aiosqlite:///{DB_PATH}"

# Create the base model
Base = declarative_base()


# Define a sample model
class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    name = Column(String(50), nullable=False)
    email = Column(String(100), unique=True, nullable=False)

    def __repr__(self):
        return f"<User(id={self.id}, name='{self.name}', email='{self.email}')>"


# Fixture to set up and tear down the database
@pytest.fixture(scope="function")
async def db_session() -> AsyncGenerator[AsyncSession, None]:
    # Remove the database file if it exists
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    # Create async engine
    engine = create_async_engine(
        DB_URL,
        # Enable SQLite to handle concurrent connections
        connect_args={"check_same_thread": False},
        # Set a larger pool size for concurrent operations
        pool_size=20,
        max_overflow=30,
        echo=True,
    )

    # Create async session factory
    async_session = async_sessionmaker(engine, expire_on_commit=False,
                                       class_=AsyncSession)

    # Create all tables
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Yield session for testing
    async with async_session() as session:
        yield session

    # Teardown - close engine and remove DB file
    await engine.dispose()
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)


# Utility functions for testing
async def add_users(session: AsyncSession, start_id: int, count: int) -> List[
    User]:
    """Add multiple users to the database."""
    users = []
    for i in range(start_id, start_id + count):
        user = User(name=f"User {i}", email=f"user{i}@example.com")
        session.add(user)
        users.append(user)

    await session.commit()
    return users


async def read_users(session: AsyncSession) -> List[User]:
    """Read all users from the database."""
    result = await session.execute(select(User).order_by(User.id))
    return result.scalars().all()


async def count_users(session: AsyncSession) -> int:
    """Count users in the database."""
    result = await session.execute(select(func.count()).select_from(User))
    return result.scalar_one()


@pytest.mark.asyncio
async def test_concurrent_read_write(db_session: AsyncSession):
    # Add some initial data
    await add_users(db_session, 1, 10)

    # Verify initial data
    initial_count = await count_users(db_session)
    assert initial_count == 10

    # Create tasks for concurrent operations
    write_tasks = []
    read_tasks = []

    # Create 10 write tasks (each adding 5 users)
    for i in range(10):
        write_tasks.append(
            asyncio.create_task(
                add_users(db_session, (i + 1) * 100, 5)
            )
        )

    # Create 20 read tasks
    for _ in range(20):
        read_tasks.append(
            asyncio.create_task(
                read_users(db_session)
            )
        )
        read_tasks.append(
            asyncio.create_task(
                count_users(db_session)
            )
        )

    # Execute all tasks concurrently
    all_tasks = write_tasks + read_tasks
    results = await asyncio.gather(*all_tasks, return_exceptions=True)

    # Check for exceptions
    exceptions = [r for r in results if isinstance(r, Exception)]
    assert len(exceptions) == 0, f"Exceptions occurred: {exceptions}"

    # Verify final count (initial 10 + 10 writes of 5 users each = 60)
    final_count = await count_users(db_session)
    assert final_count == 60, f"Expected 60 users, got {final_count}"


@pytest.mark.asyncio
async def test_intensive_concurrent_operations(db_session: AsyncSession):
    """Test more intensive concurrent read/write operations with timing."""
    start_time = time.time()

    # Create a large number of concurrent tasks
    tasks = []

    # Writers (50 tasks, each adding 10 users)
    for i in range(50):
        tasks.append(
            asyncio.create_task(
                add_users(db_session, i * 1000, 10)
            )
        )

    # Readers (100 tasks)
    for _ in range(100):
        # Mix of different read operations
        tasks.append(asyncio.create_task(count_users(db_session)))
        tasks.append(asyncio.create_task(read_users(db_session)))

    # Execute all tasks concurrently
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Check for exceptions
    exceptions = [r for r in results if isinstance(r, Exception)]
    assert len(exceptions) == 0, f"Exceptions occurred: {exceptions}"

    # Verify final user count (50 writers * 10 users = 500)
    final_count = await count_users(db_session)
    assert final_count == 500, f"Expected 500 users, got {final_count}"

    # Report execution time
    execution_time = time.time() - start_time
    print(f"Executed 150 concurrent operations in {execution_time:.2f} seconds")
