from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase): ...


# NOTE: Import models after the Base object.
from .product import Product
