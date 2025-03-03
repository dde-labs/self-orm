from sqlalchemy import Integer, String, select, func
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.ext.asyncio import AsyncSession

from . import Base


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(50), nullable=False)
    email: Mapped[str] = mapped_column(String(100), nullable=False, unique=True)

    @classmethod
    async def read_users(cls, session: AsyncSession) -> list["User"]:
        """Read all users from the database."""
        result = await session.execute(select(cls).order_by(cls.id))
        return result.scalars().all()

    @classmethod
    async def count_users(cls, session: AsyncSession) -> int:
        """Count users in the database."""
        result = await session.execute(select(func.count()).select_from(cls))
        return result.scalar_one()
