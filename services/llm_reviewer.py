"""LLM-based supplier 360° review service."""
from datetime import datetime, timezone
from typing import Dict, List, Optional
from pydantic import BaseModel, Field

from fastapi import FastAPI, HTTPException
import openai
from openai import OpenAI

# Risk schema models
class DimensionScore(BaseModel):
    score: float = Field(..., ge=0.0, le=1.0, description="Risk score between 0-1")
    reason: str = Field(..., description="Explanation with evidence citations")

class OverallRisk(BaseModel):
    grade: str = Field(..., description="low/medium/high risk grade")
    score: float = Field(..., ge=0.0, le=1.0, description="Weighted average risk score")
    reason: str = Field(..., description="Summary explanation")

class RiskReview(BaseModel):
    supplier: str
    overall_risk: OverallRisk
    dimensions: Dict[str, DimensionScore]
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    evidence_hash: str = Field(..., description="Hash of evidence set used")

# Configuration
DIMENSION_WEIGHTS = {
    "financial": 0.25,
    "supply": 0.30,
    "reputation": 0.20,
    "quality": 0.15,
    "geo": 0.10
}

FEW_SHOT_EXAMPLES = [
    # Low risk example
    {
        "supplier": "Example Corp",
        "overall_risk": {
            "grade": "low",
            "score": 0.15,
            "reason": "Strong financials [E1], no supply disruptions [E2], positive ESG record"
        },
        "dimensions": {
            "financial": {"score": 0.1, "reason": "Stable revenue growth [E1]"},
            "supply": {"score": 0.2, "reason": "99.8% on-time delivery [E2]"},
            "reputation": {"score": 0.1, "reason": "No negative press [E3]"},
            "quality": {"score": 0.2, "reason": "Low defect rate [E4]"},
            "geo": {"score": 0.1, "reason": "Diversified locations [E5]"}
        }
    }
]

SYSTEM_PROMPT = """You are a senior supply-risk analyst. Your task is to analyze supplier risk across multiple dimensions using only the provided evidence."""

REVIEW_PROMPT_TEMPLATE = """Using only the evidence below, fill the JSON template for a comprehensive supplier risk review.

EVIDENCE:
{context}

TEMPLATE:
{{
 "supplier": "{supplier_name}",
 "overall_risk": {{"grade":"", "score":0.0, "reason":""}},
 "dimensions": {{
   "financial": {{"score":0.0, "reason":""}},
   "supply": {{"score":0.0, "reason":""}},
   "reputation": {{"score":0.0, "reason":""}},
   "quality": {{"score":0.0, "reason":""}},
   "geo": {{"score":0.0, "reason":""}}
 }}
}}

Instructions:
- Use 0.0 = no risk, 1.0 = extreme risk
- Cite evidence IDs in each reason like [E3]
- If information is missing, note "insufficient data" and score 0.3
- Be specific about dates and metrics in reasons
"""

class LLMReviewer:
    def __init__(
        self,
        api_key: Optional[str] = None,
        qdrant_url: Optional[str] = None,
        postgres_url: Optional[str] = None,
        redis_url: Optional[str] = None
    ):
        """Initialize the LLM reviewer with optional API key and data sources."""
        self.client = OpenAI(api_key=api_key) if api_key else None
        
        # Initialize context provider if URLs provided
        if qdrant_url and postgres_url:
            from services.context_provider import ContextProvider
            self.context_provider = ContextProvider(
                qdrant_url=qdrant_url,
                postgres_url=postgres_url,
                redis_url=redis_url
            )
        else:
            self.context_provider = None
        
    async def get_context_bundle(self, supplier_id: str) -> Dict:
        """Retrieve relevant context for the supplier from various sources."""
        if self.context_provider:
            return await self.context_provider.get_context_bundle(supplier_id)
            
        # Return mock data if no context provider
        return {
            "supplier_core": {
                "name": "Metal-Can Co",
                "id": supplier_id,
                "annual_revenue": "$50M",
                "founded": 2010
            },
            "evidence": """
            ### INTERNAL
            [E1] SAP Data (2025-06): On-time delivery rate 94%, Quality rating B+
            [E2] Email (2025-07-01): Production delay reported at main facility
            
            ### EXTERNAL
            [E3] News (2025-07-10): Metal-Can Co reports 15% revenue growth
            [E4] ESG Report (2025-Q2): No major incidents, carbon reduction on track
            """
        }
        
    async def review_supplier(self, supplier_id: str) -> RiskReview:
        """Generate a comprehensive risk review for a supplier."""
        # Get context
        context = await self.get_context_bundle(supplier_id)
        
        # Check cache if context provider available
        if self.context_provider:
            evidence_hash = self.context_provider._compute_evidence_hash(context)
            cached_review = await self.context_provider.get_cached_review(
                supplier_id, evidence_hash
            )
            if cached_review:
                return RiskReview(**cached_review)
        
        # Build prompt
        prompt = REVIEW_PROMPT_TEMPLATE.format(
            context=context["evidence"],
            supplier_name=context["supplier_core"]["name"]
        )
        
        # Get LLM response
        if self.client:
            response = await self._call_openai(prompt)
        else:
            response = self._mock_response(context["supplier_core"]["name"])
            
        # Post-process scores
        review = self._postprocess_review(response)
        
        # Cache result if context provider available
        if self.context_provider:
            await self.context_provider.cache_review(
                supplier_id,
                evidence_hash,
                review.dict()
            )
            
        return review
    
    async def _call_openai(self, prompt: str) -> Dict:
        """Call OpenAI API with the review prompt."""
        try:
            response = await self.client.chat.completions.create(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1
            )
            return response.choices[0].message.content
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"LLM API error: {str(e)}")
    
    def _mock_response(self, supplier_name: str) -> Dict:
        """Return mock response for testing."""
        return {
            "supplier": supplier_name,
            "overall_risk": {
                "grade": "medium",
                "score": 0.45,
                "reason": "Mixed signals: strong financials [E3] but supply concerns [E2]"
            },
            "dimensions": {
                "financial": {"score": 0.2, "reason": "15% revenue growth [E3]"},
                "supply": {"score": 0.6, "reason": "Production delays reported [E2]"},
                "reputation": {"score": 0.3, "reason": "No major incidents [E4]"},
                "quality": {"score": 0.4, "reason": "B+ quality rating [E1]"},
                "geo": {"score": 0.3, "reason": "Insufficient data"}
            }
        }
    
    def _postprocess_review(self, llm_response: Dict) -> RiskReview:
        """Post-process LLM response and compute final scores."""
        # Calculate weighted average score
        scores = llm_response["dimensions"]
        weighted_score = sum(
            scores[dim]["score"] * DIMENSION_WEIGHTS[dim]
            for dim in DIMENSION_WEIGHTS
        )
        
        # Determine overall grade
        grade = (
            "high" if weighted_score > 0.66
            else "medium" if weighted_score > 0.33
            else "low"
        )
        
        # Update overall risk
        llm_response["overall_risk"]["score"] = round(weighted_score, 2)
        llm_response["overall_risk"]["grade"] = grade
        
        # Add metadata
        llm_response["timestamp"] = datetime.now(timezone.utc)
        llm_response["evidence_hash"] = "mock_hash"  # TODO: Implement proper hashing
        
        return RiskReview(**llm_response)

# FastAPI app
app = FastAPI(title="Supplier Risk Review Service")
reviewer = LLMReviewer()

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}

@app.get("/review/{supplier_id}")
async def get_supplier_review(supplier_id: str) -> RiskReview:
    """Generate a comprehensive risk review for a supplier."""
    return await reviewer.review_supplier(supplier_id)
