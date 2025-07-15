"""Supplier risk assessment UI endpoints."""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Dict, List
import requests

from services.airweave_client import AirweaveClient

router = APIRouter()

class SupplierRequest(BaseModel):
    """Request model for supplier risk assessment."""
    supplier_email: str
    supplier_name: str

class RiskAssessment(BaseModel):
    """Risk assessment response model."""
    supplier_id: str
    risk_score: float
    key_findings: List[Dict]
    recent_news: List[Dict]
    email_analysis: List[Dict]

def get_supplier_id(email: str) -> str:
    """Generate a supplier ID from email."""
    return f"SUP-{email.split('@')[0].upper()}"

@router.post("/assess", response_model=RiskAssessment)
async def assess_supplier_risk(request: SupplierRequest):
    """Assess supplier risk based on emails and news."""
    try:
        # Initialize Airweave client
        client = AirweaveClient()
        
        # Get supplier ID
        supplier_id = get_supplier_id(request.supplier_email)
        
        # Search for relevant documents
        email_results = client.search(
            collection="emails",
            filter={"supplier_id": supplier_id},
            limit=100
        )
        
        news_results = client.search(
            collection="news",
            filter={"supplier_name": request.supplier_name},
            limit=50
        )
        
        # Analyze results
        key_findings = []
        risk_score = 0.0
        
        # Analyze emails
        email_analysis = []
        for email in email_results.get("results", []):
            email_analysis.append({
                "subject": email.get("subject"),
                "date": email.get("created_at"),
                "risk_level": email.get("risk_level", "low")
            })
            
            # Update risk score based on email content
            if email.get("risk_level") == "high":
                risk_score += 0.3
            elif email.get("risk_level") == "medium":
                risk_score += 0.1
        
        # Analyze news
        recent_news = []
        for article in news_results.get("results", []):
            recent_news.append({
                "title": article.get("title"),
                "date": article.get("created_at"),
                "sentiment": article.get("sentiment", "neutral")
            })
            
            # Update risk score based on news sentiment
            if article.get("sentiment") == "negative":
                risk_score += 0.4
            elif article.get("sentiment") == "neutral":
                risk_score += 0.1
        
        # Normalize risk score
        risk_score = min(1.0, risk_score)
        
        return RiskAssessment(
            supplier_id=supplier_id,
            risk_score=risk_score,
            key_findings=key_findings,
            recent_news=recent_news,
            email_analysis=email_analysis
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
