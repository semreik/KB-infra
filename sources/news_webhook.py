from typing import Dict, Any, Iterator
from datetime import datetime
from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel
from airweave.sources import BaseSource
from airweave.types import TextChunk

class NewsItem(BaseModel):
    title: str
    url: str
    body: str
    published: datetime

class NewsWebhookSource(BaseSource):
    """Custom source for ingesting news via webhooks."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.api_key = config["api_key"]
        self.app = FastAPI()
        self.news_items = []
        
        @self.app.post("/webhook/news")
        async def receive_news(
            item: NewsItem,
            x_api_key: str = Header(None)
        ):
            if x_api_key != self.api_key:
                raise HTTPException(
                    status_code=403,
                    detail="Invalid API key"
                )
                
            self.news_items.append(item)
            # Trigger Airweave ingestion
            chunk = TextChunk(
                content=f"{item.title}\n\n{item.body}",
                metadata={
                    "source": "news_webhook",
                    "url": item.url,
                    "published": item.published.isoformat(),
                    "type": "news"
                }
            )
            self.ingest_chunk(chunk)
            return {"status": "success"}

    def list_entities(self) -> Iterator[str]:
        """List available news items."""
        return ["news"]

    def iter_content(self, entity: str) -> Iterator[TextChunk]:
        """Yield news items as TextChunks."""
        for item in self.news_items:
            yield TextChunk(
                content=f"{item.title}\n\n{item.body}",
                metadata={
                    "source": "news_webhook",
                    "url": item.url,
                    "published": item.published.isoformat(),
                    "type": "news"
                }
            )

    def get_metadata(self) -> Dict[str, Any]:
        """Return source metadata."""
        return {
            "name": "news_webhook",
            "type": "webhook",
            "description": "News ingestion via webhooks"
        }
