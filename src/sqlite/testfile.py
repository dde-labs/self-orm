import asyncio
import os
import time
from typing import List
import uuid

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import AsyncAdaptedQueuePool


async def run_concurrent_write_test(db: AsyncSQLiteDatabase,
                                    num_products: int = 1000) -> float:
    """Test concurrent writes by adding multiple products simultaneously"""
    start_time = time.time()

    async def add_random_product(i: int):
        sku = f"SKU-{uuid.uuid4()}"
        name = f"Product {i}"
        price = round(10 + (i % 90), 2)
        await db.add_product(name=name, price=price, sku=sku, inventory=i % 100)

    # Create a list of tasks for concurrent execution
    tasks = [add_random_product(i) for i in range(num_products)]
    await asyncio.gather(*tasks)

    end_time = time.time()
    return end_time - start_time


async def run_concurrent_read_test(db: AsyncSQLiteDatabase,
                                   num_reads: int = 1000) -> float:
    """Test concurrent reads by performing multiple queries simultaneously"""
    # First, get all product IDs
    products = await db.get_all_products()
    product_ids = [p.id for p in products]

    if not product_ids:
        raise ValueError("No products found for read test")

    # Use modulo to cycle through product IDs if we have more reads than products
    async def read_product(i: int):
        product_id = product_ids[i % len(product_ids)]
        return await db.get_product_by_id(product_id)

    start_time = time.time()

    # Create a list of tasks for concurrent execution
    tasks = [read_product(i) for i in range(num_reads)]
    results = await asyncio.gather(*tasks)

    end_time = time.time()
    return end_time - start_time


async def run_concurrent_mixed_test(
    db: AsyncSQLiteDatabase,
    num_operations: int = 1000,
    read_ratio: float = 0.8
) -> dict[str, float]:
    """Test concurrent mixed operations (reads and writes)"""
    products = await db.get_all_products()
    product_ids = [p.id for p in products] if products else []

    read_count = int(num_operations * read_ratio)
    write_count = num_operations - read_count

    async def random_operation(i: int):
        if i < read_count and product_ids:
            # Read operation
            product_id = product_ids[i % len(product_ids)]
            return await db.get_product_by_id(product_id)
        else:
            # Write operation (update or create)
            if product_ids and i % 3 == 0 and len(product_ids) > 0:
                # Update inventory
                product_id = product_ids[i % len(product_ids)]
                return await db.update_inventory(product_id, (i * 7) % 200)
            else:
                # Create new product
                sku = f"SKU-MIXED-{uuid.uuid4()}"
                name = f"Mixed Product {i}"
                price = round(15 + (i % 85), 2)
                return await db.add_product(name=name, price=price, sku=sku,
                                            inventory=i % 150)

    start_time = time.time()

    # Create a list of tasks for concurrent execution
    tasks = [random_operation(i) for i in range(num_operations)]
    await asyncio.gather(*tasks)

    end_time = time.time()

    # Report performance
    total_time = end_time - start_time
    ops_per_second = num_operations / total_time if total_time > 0 else 0

    return {
        "total_time": total_time,
        "operations": num_operations,
        "read_operations": read_count,
        "write_operations": write_count,
        "operations_per_second": ops_per_second
    }


async def main():
    """Main test function"""
    # Remove existing test database if it exists
    if os.path.exists("async_sqlite_test.db"):
        os.remove("async_sqlite_test.db")

    print("Initializing SQLite database with SQLAlchemy 2.0 async engine...")
    db = AsyncSQLiteDatabase(echo=False)
    await db.initialize()

    try:
        # Test 1: Concurrent writes
        print("\nTest 1: Concurrent writes - Adding 1000 products")
        write_time = await run_concurrent_write_test(db, 1000)
        print(
            f"Completed in {write_time:.2f} seconds ({1000 / write_time:.2f} operations/second)")

        # Test 2: Concurrent reads
        print("\nTest 2: Concurrent reads - Reading 2000 products")
        read_time = await run_concurrent_read_test(db, 2000)
        print(
            f"Completed in {read_time:.2f} seconds ({2000 / read_time:.2f} operations/second)")

        # Test 3: Mixed concurrent operations
        print(
            "\nTest 3: Mixed concurrent operations - 80% reads, 20% writes, 3000 operations")
        mixed_results = await run_concurrent_mixed_test(db, 3000, 0.8)
        print(
            f"Completed {mixed_results['operations']} mixed operations in {mixed_results['total_time']:.2f} seconds")
        print(
            f"Operations per second: {mixed_results['operations_per_second']:.2f}")
        print(f"Read operations: {mixed_results['read_operations']}")
        print(f"Write operations: {mixed_results['write_operations']}")

        # Get some statistics
        total_products = len(await db.get_all_products())
        total_value = await db.get_total_inventory_value()
        print(
            f"\nDatabase contains {total_products} products with total inventory value of ${total_value:.2f}")

    finally:
        # Clean up
        await db.close()
        print("\nDatabase connection closed")


if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main())
