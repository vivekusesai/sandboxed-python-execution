"""
Seed database with test data.

Usage: python scripts/seed_database.py
"""

import asyncio
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from sqlalchemy import text

from app.config import get_settings
from app.core.security import hash_password
from app.database import AsyncSessionLocal, engine, sync_engine
from app.models import Base, User, Script
from app.models.user import UserRole


async def create_tables():
    """Create all database tables."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    print("Database tables created")


async def create_test_user():
    """Create a test user."""
    async with AsyncSessionLocal() as db:
        # Check if user exists
        result = await db.execute(
            text("SELECT id FROM users WHERE email = 'admin@example.com'")
        )
        if result.scalar_one_or_none():
            print("Test user already exists")
            return

        user = User(
            email="admin@example.com",
            password_hash=hash_password("admin123"),
            role=UserRole.ADMIN,
        )
        db.add(user)
        await db.commit()
        print(f"Created test user: admin@example.com / admin123")


async def create_test_script():
    """Create a test transformation script."""
    async with AsyncSessionLocal() as db:
        # Get admin user
        result = await db.execute(
            text("SELECT id FROM users WHERE email = 'admin@example.com'")
        )
        user_id = result.scalar_one_or_none()
        if not user_id:
            print("Test user not found, skipping script creation")
            return

        # Check if script exists
        result = await db.execute(
            text("SELECT id FROM scripts WHERE name = 'Add Total Column'")
        )
        if result.scalar_one_or_none():
            print("Test script already exists")
            return

        script = Script(
            user_id=user_id,
            name="Add Total Column",
            description="Multiplies price by quantity to create a total column",
            code_text="""def transform(df):
    # Calculate total from price and quantity
    df["total"] = df["price"] * df["qty"]
    return df
""",
        )
        db.add(script)
        await db.commit()
        print("Created test script: Add Total Column")


def create_sample_tables():
    """Create sample data tables for testing."""
    settings = get_settings()

    # Sample sales data
    sales_data = pd.DataFrame({
        "id": range(1, 101),
        "product": [f"Product_{i % 10}" for i in range(1, 101)],
        "price": [round(10 + (i * 0.5), 2) for i in range(1, 101)],
        "qty": [(i % 20) + 1 for i in range(1, 101)],
        "region": ["North", "South", "East", "West"] * 25,
    })

    # Sample customers data
    customers_data = pd.DataFrame({
        "id": range(1, 51),
        "name": [f"Customer_{i}" for i in range(1, 51)],
        "email": [f"customer{i}@example.com" for i in range(1, 51)],
        "city": ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"] * 10,
        "signup_date": pd.date_range("2024-01-01", periods=50, freq="D"),
    })

    # Write to database
    sales_data.to_sql("sales", sync_engine, if_exists="replace", index=False)
    print(f"Created 'sales' table with {len(sales_data)} rows")

    customers_data.to_sql("customers", sync_engine, if_exists="replace", index=False)
    print(f"Created 'customers' table with {len(customers_data)} rows")


async def main():
    """Run all seed operations."""
    print("=" * 50)
    print("Seeding database...")
    print("=" * 50)

    # Create tables
    await create_tables()

    # Create test user
    await create_test_user()

    # Create test script
    await create_test_script()

    # Create sample tables
    create_sample_tables()

    print("=" * 50)
    print("Database seeding complete!")
    print("")
    print("Test credentials:")
    print("  Email: admin@example.com")
    print("  Password: admin123")
    print("=" * 50)


if __name__ == "__main__":
    asyncio.run(main())
