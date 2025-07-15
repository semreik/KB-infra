from typing import Dict, Optional

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models.supplier_risk import SupplierRiskProfile

router = APIRouter()


@router.get("/{supplier_id}/risk")
async def get_supplier_risk(
    supplier_id: str,
    db: AsyncSession = Depends(get_db)
) -> Dict:
    """Get the latest risk profile for a supplier."""
    query = (
        select(SupplierRiskProfile)
        .where(SupplierRiskProfile.supplier_id == supplier_id)
        .order_by(SupplierRiskProfile.timestamp.desc())
        .limit(1)
    )
    
    try:
        result = await db.execute(query)
        profile = result.scalar_one_or_none()
        
        if not profile:
            raise HTTPException(
                status_code=404,
                detail=f"No risk profile found for supplier {supplier_id}"
            )
        
        return profile.risk_json
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching risk profile: {str(e)}"
        )
