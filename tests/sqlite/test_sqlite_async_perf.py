import asyncio
import pytest
import time

from sqlalchemy.ext.asyncio import AsyncSession
from src.sqlite.models import User
from src.sqlite.db import AsyncSQLiteManage


async def add_users(
    db_manage: AsyncSQLiteManage, start_id: int, count: int
) -> list[User]:
    """Add multiple users to the database."""
    async with db_manage.async_session_maker() as session:
        try:
            users: list[User] = []
            for i in range(start_id, start_id + count):
                user: User = User(name=f"User {i}",
                                  email=f"user{i}@example.com")
                users.append(user)

            session.add_all(users)
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


@pytest.mark.asyncio
async def test_sqlite_concurrent_read_write(
    db_session: AsyncSession, db_manage: AsyncSQLiteManage
):
    await add_users(db_manage, 1, 10)

    initial_count = await User.count_users(db_session)
    assert initial_count == 10

    # Create tasks for concurrent operations
    write_tasks = []
    read_tasks = []

    # Create 10 write tasks (each adding 5 users)
    for i in range(10):
        write_tasks.append(
            asyncio.create_task(add_users(db_manage, (i + 1) * 100, 5))
        )

    # Create 20 read tasks
    for _ in range(20):
        read_tasks.append(asyncio.create_task(User.read_users(db_session)))
        read_tasks.append(asyncio.create_task(User.count_users(db_session)))

    # Execute all tasks concurrently
    all_tasks = write_tasks + read_tasks
    results = await asyncio.gather(*all_tasks, return_exceptions=True)

    # Check for exceptions
    exceptions = [r for r in results if isinstance(r, Exception)]
    assert len(exceptions) == 0, f"Exceptions occurred: {exceptions}"

    # Verify final count (initial 10 + 10 writes of 5 users each = 60)
    final_count = await User.count_users(db_session)
    assert final_count == 60, f"Expected 60 users, got {final_count}"


@pytest.mark.asyncio
async def test_sqlite_intensive_concurrent_operations(
    db_session: AsyncSession, db_manage: AsyncSQLiteManage
):
    """Test more intensive concurrent read/write operations with timing."""
    start_time = time.time()

    # Create a large number of concurrent tasks
    tasks = []

    # Writers (50 tasks, each adding 10 users)
    for i in range(50):
        tasks.append(
            asyncio.create_task(add_users(db_manage, i * 1000, 10))
        )

    # Readers (100 tasks)
    # for _ in range(50):
    #     # Mix of different read operations
    #     tasks.append(asyncio.create_task(User.count_users(db_session)))
    #     tasks.append(asyncio.create_task(User.read_users(db_session)))

    # Execute all tasks concurrently
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Check for exceptions
    exceptions = [r for r in results if isinstance(r, Exception)]
    assert len(exceptions) == 0, f"Exceptions occurred: {exceptions}"

    # Verify final user count (50 writers * 10 users = 500)
    final_count = await User.count_users(db_session)
    assert final_count == 500, f"Expected 500 users, got {final_count}"

    # Report execution time
    execution_time = time.time() - start_time
    print(f"Executed 150 concurrent operations in {execution_time:.2f} seconds")
