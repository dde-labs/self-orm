from typing import Any

from sqlalchemy import Integer, String, Float, select, func
from sqlalchemy.orm import Mapped, mapped_column

from . import Base


class Product(Base):
    __tablename__ = "products"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False, index=True)
    price: Mapped[float] = mapped_column(Float, nullable=False)
    sku: Mapped[str] = mapped_column(String, nullable=False, unique=True)
    description: Mapped[str] = mapped_column(String)
    inventory: Mapped[int] = mapped_column(Integer, default=0)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "price": self.price,
            "sku": self.sku,
            "description": self.description,
            "inventory": self.inventory
        }

    @classmethod
    async def add_product(
        cls,
        session,
        name: str,
        price: float,
        sku: str,
        description: str = "",
        inventory: int = 0,
    ) -> "Product":
        """Add a new product to the database"""
        async with session() as session:
            product = cls(
                name=name,
                price=price,
                sku=sku,
                description=description,
                inventory=inventory
            )
            session.add(product)
            await session.commit()
            return product

    @classmethod
    async def get_product_by_id(cls, session, product_id: int) -> "Product":
        """Get a product by ID"""
        async with session() as session:
            result = await session.execute(
                select(Product).where(Product.id == product_id))
            return result.scalars().first()

    @classmethod
    async def get_product_by_sku(cls, session, sku: str) -> "Product":
        """Get a product by SKU"""
        async with session() as session:
            result = await session.execute(
                select(Product).where(Product.sku == sku))
            return result.scalars().first()

    @classmethod
    async def update_inventory(cls, session, product_id: int,
                               new_inventory: int) -> "Product":
        """Update product inventory"""
        async with session() as session:
            async with session.begin():
                product = await session.get(Product, product_id)
                if product:
                    product.inventory = new_inventory
                await session.commit()
                return product

    @classmethod
    async def get_all_products(cls, session) -> list["Product"]:
        """Get all products"""
        async with session() as session:
            result = await session.execute(select(Product))
            return result.scalars().all()

    @classmethod
    async def get_total_inventory_value(cls, session) -> float:
        """Get total inventory value (price * inventory) for all products"""
        async with session() as session:
            result = await session.execute(
                select(func.sum(Product.price * Product.inventory))
            )
            return result.scalar() or 0.0
