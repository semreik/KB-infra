"""Routes package."""

from fastapi import APIRouter

from app.routes import supplier_risk
from app.services import llm_scorer

router = APIRouter()
router.include_router(supplier_risk.router, prefix="/suppliers")
router.include_router(llm_scorer.router, prefix="/llm")
