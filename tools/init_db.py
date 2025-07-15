"""Initialize database with sample data."""
import argparse
import asyncio
import logging
from typing import List

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from services.context_provider import supplier_table

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def init_db(db_url: str, sample_data: bool = True):
    """Initialize database and optionally add sample data."""
    logger.info("Connecting to database at %s", db_url)
    engine = create_async_engine(db_url)
    
    # Create session factory
    async_session = sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False
    )
    
    if sample_data:
        sample_suppliers = [
            {
                "id": "SUP-000045",
                "name": "Metal-Can Co",
                "annual_revenue": 50000000.0,
                "employee_count": 250,
                "founded_year": 2010,
                "hq_location": "San Francisco, CA",
                "industry": "Manufacturing"
            },
            {
                "id": "SUP-000046",
                "name": "Eco Packaging Ltd",
                "annual_revenue": 75000000.0,
                "employee_count": 350,
                "founded_year": 2008,
                "hq_location": "Austin, TX",
                "industry": "Manufacturing"
            },
            {
                "id": "SUP-000047",
                "name": "Global Logistics Inc",
                "annual_revenue": 120000000.0,
                "employee_count": 500,
                "founded_year": 2005,
                "hq_location": "Chicago, IL",
                "industry": "Logistics"
            }
        ]
        
        async with async_session() as session:
            logger.info("Adding sample suppliers")
            for supplier in sample_suppliers:
                await session.execute(
                    supplier_table.insert().values(supplier)
                )
            await session.commit()
            
        logger.info("Sample data loaded successfully!")
    
    await engine.dispose()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Initialize database")
    parser.add_argument(
        "--db-url",
        default="postgresql+asyncpg://docker:docker@db:5432/airweave",
        help="Database URL"
    )
    parser.add_argument(
        "--no-sample-data",
        action="store_true",
        help="Skip adding sample data"
    )
    
    args = parser.parse_args()
    asyncio.run(init_db(args.db_url, not args.no_sample_data))
