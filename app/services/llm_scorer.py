"""LLM Scorer service for risk assessment."""
import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import openai
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from redis import Redis

router = APIRouter()

# Initialize Redis client
redis_client = Redis(
    host=os.getenv('REDIS_HOST', 'redis'),
    port=int(os.getenv('REDIS_PORT', 6379)),
    db=0
)

# Initialize OpenAI client
openai.api_key = os.getenv('OPENAI_API_KEY')

# Models
class Document(BaseModel):
    """Document to be scored."""
    content: str
    metadata: Dict
    
class RiskScore(BaseModel):
    """Risk score response."""
    score: float
    explanation: str
    categories: Dict[str, Dict[str, float | str]]
    
# Constants
CACHE_TTL = 3600  # 1 hour cache TTL
SYSTEM_PROMPT = """You are a risk assessment expert. Analyze the given document and assess supplier risk in these categories:
- Financial Risk: Late payments, financial instability, etc.
- Compliance Risk: Regulatory violations, legal issues, etc.
- Reputation Risk: Negative press, customer complaints, etc.

For each category and overall, provide:
1. A risk score between 0.0 (lowest risk) and 1.0 (highest risk)
2. A brief explanation of the score

Format your response as a JSON object with this structure:
{
    "categories": {
        "financial": {"score": float, "explanation": string},
        "compliance": {"score": float, "explanation": string},
        "reputation": {"score": float, "explanation": string}
    },
    "overall_risk": {
        "score": float,
        "explanation": string
    }
}"""

def get_cache_key(content: str) -> str:
    """Generate cache key for document content."""
    return f"risk_score:{hash(content)}"

async def get_cached_score(content: str) -> Optional[RiskScore]:
    """Get cached risk score if available."""
    cache_key = get_cache_key(content)
    cached = redis_client.get(cache_key)
    if cached:
        return RiskScore(**json.loads(cached))
    return None

async def cache_score(content: str, score: RiskScore):
    """Cache risk score for future use."""
    cache_key = get_cache_key(content)
    redis_client.setex(
        cache_key,
        CACHE_TTL,
        json.dumps(score.dict())
    )

async def analyze_document(content: str) -> RiskScore:
    """Analyze document content using OpenAI."""
    try:
        response = await openai.ChatCompletion.acreate(
            model="gpt-4",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": content}
            ],
            temperature=0.2,
            max_tokens=500
        )
        
        # Parse the response
        result = json.loads(response.choices[0].message.content)
        return RiskScore(**result)
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error analyzing document: {str(e)}"
        )

@router.post("/score", response_model=RiskScore)
async def score_document(document: Document) -> RiskScore:
    """Score a document for supplier risk."""
    # Check cache first
    cached_score = await get_cached_score(document.content)
    if cached_score:
        return cached_score
    
    # Get fresh score from LLM
    score = await analyze_document(document.content)
    
    # Cache the result
    await cache_score(document.content, score)
    
    return score

@router.post("/batch_score")
async def batch_score_documents(documents: List[Document]) -> List[RiskScore]:
    """Score multiple documents in batch."""
    scores = []
    for doc in documents:
        # Try cache first
        cached_score = await get_cached_score(doc.content)
        if cached_score:
            scores.append(cached_score)
            continue
            
        # Get fresh score
        score = await analyze_document(doc.content)
        await cache_score(doc.content, score)
        scores.append(score)
    
    return scores
