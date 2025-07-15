"""Health check routes."""

from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter()

class HealthResponse(BaseModel):
    """Health check response model."""
    status: str
    message: str

@router.get("/", response_model=HealthResponse)
async def health_check():
    """Check server health."""
    return HealthResponse(status="healthy", message="Server is running")
