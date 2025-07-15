from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, BackgroundTasks, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import os
from dotenv import load_dotenv
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from app.database import Base, get_db
from app.routes import supplier_risk
from sources.gmail_source import GmailSource
from sources.gdrive_source import GDriveSource
from google.oauth2.credentials import Credentials
from processors.text import TextProcessor
from vectorstore.qdrant import QdrantStore

# Load environment variables
load_dotenv()

# Create async engine
engine = create_async_engine("postgresql+asyncpg://docker:docker@db:5432/airweave")
async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Create tables on startup
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield

app = FastAPI(
    title="Airweave Knowledge Base",
    description="Semantic search over Gmail and Google Drive content",
    lifespan=lifespan
)

@app.middleware("http")
async def log_requests(request: Request, call_next):
    logger.info(f"Request: {request.method} {request.url}")
    response = await call_next(request)
    logger.info(f"Response status: {response.status_code}")
    return response

# Mount static files
app.mount("/static", StaticFiles(directory="static"), name="static")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, replace with specific origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add supplier risk router
app.include_router(supplier_risk.router, prefix="/suppliers")

# Initialize components
text_processor = TextProcessor(
    chunk_size=int(os.getenv('CHUNK_SIZE', '1000')),
    chunk_overlap=int(os.getenv('CHUNK_OVERLAP', '200'))
)

vector_store = QdrantStore(
    url=os.getenv('QDRANT_URL', 'http://localhost:6333'),
    collection_name=os.getenv('QDRANT_COLLECTION', 'kb_vectors')
)

# Initialize sources with debug logging
print("Initializing Gmail source...")
print(f"Gmail Client ID: {os.getenv('GMAIL_CLIENT_ID')}")
print(f"Gmail Refresh Token: {os.getenv('GMAIL_REFRESH_TOKEN')[:10]}...")

gmail_source = GmailSource({'credentials_json': {
    'refresh_token': os.getenv('GMAIL_REFRESH_TOKEN'),
    'client_id': os.getenv('GMAIL_CLIENT_ID'),
    'client_secret': os.getenv('GMAIL_CLIENT_SECRET')
}})

print("Gmail source initialized")

gdrive_source = GDriveSource({'credentials_json': {
    'refresh_token': os.getenv('GDRIVE_REFRESH_TOKEN'),
    'client_id': os.getenv('GDRIVE_CLIENT_ID'),
    'client_secret': os.getenv('GDRIVE_CLIENT_SECRET')
}})

# API Models
class SearchQuery(BaseModel):
    query: str
    limit: int = 5
    source_type: Optional[str] = None  # "gmail" or "gdrive"
    source_id: Optional[str] = None    # specific email or file ID

class IndexRequest(BaseModel):
    source_type: str  # "gmail" or "gdrive"
    entity_id: str    # email or file ID

# API Routes
@app.get("/")
async def root():
    return FileResponse('static/index.html')

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

@app.get("/emails")
async def list_emails():
    """List all emails from Gmail."""
    try:
        print("Attempting to list emails...")
        messages = []
        for msg_id in gmail_source.list_entities():
            try:
                msg = gmail_source.service.users().messages().get(userId='me', id=msg_id).execute()
                subject = next((h['value'] for h in msg['payload']['headers'] if h['name'].lower() == 'subject'), 'No Subject')
                messages.append({
                    'id': msg_id,
                    'subject': subject,
                    'snippet': msg.get('snippet', '')
                })
            except Exception as msg_error:
                print(f"Error fetching message {msg_id}: {str(msg_error)}")
        print(f"Found {len(messages)} messages")
        return {"message_count": len(messages), "messages": messages}
    except Exception as e:
        print(f"Error listing emails: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/emails/{message_id}")
async def get_email(message_id: str):
    """Get a specific email by its ID."""
    try:
        logger.info(f"Fetching email content for message ID: {message_id}")
        chunks = list(gmail_source.iter_content(message_id))
        logger.info(f"Retrieved {len(chunks)} chunks from Gmail")
        
        for chunk in chunks:
            logger.info(f"Raw chunk metadata: {chunk.metadata}")
            if isinstance(chunk.content, bytes):
                logger.info(f"Content is bytes, length: {len(chunk.content)}")
            else:
                logger.info(f"Content preview: {str(chunk.content)[:100]}...")
        
        processed_chunks = []
        for chunk in chunks:
            for processed in text_processor.process_chunk(chunk):
                processed_chunks.append({
                    "text": processed.text,
                    "metadata": processed.metadata
                })
        logger.info(f"Processed into {len(processed_chunks)} chunks")
        return {"chunks": processed_chunks}
    except Exception as e:
        logger.error(f"Error fetching email: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/drive")
async def list_files():
    """List all files from Google Drive."""
    try:
        files = list(gdrive_source.list_entities())
        return {"file_count": len(files), "files": files}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/drive/{file_id}")
async def get_file(file_id: str):
    """Get a specific file by its ID."""
    try:
        chunks = list(gdrive_source.iter_content(file_id))
        processed_chunks = []
        for chunk in chunks:
            for processed in text_processor.process_chunk(chunk):
                processed_chunks.append({
                    "text": processed.text,
                    "metadata": processed.metadata
                })
        return {"chunks": processed_chunks}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/index")
async def index_content(request: IndexRequest, background_tasks: BackgroundTasks):
    """Index content from a specific source."""
    try:
        # Get the appropriate source
        source = gmail_source if request.source_type == "gmail" else gdrive_source
        
        # Get content chunks
        chunks = list(source.iter_content(request.entity_id))
        
        # Process and index chunks in the background
        def process_and_index():
            for chunk in chunks:
                for processed in text_processor.process_chunk(chunk):
                    vector_store.index_chunk(processed)
        
        background_tasks.add_task(process_and_index)
        return {"status": "indexing", "chunk_count": len(chunks)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/search")
async def search(query: SearchQuery):
    """Search for content using semantic search."""
    try:
        results = vector_store.search(
            query=query.query,
            limit=query.limit,
            source_type=query.source_type,
            source_id=query.source_id
        )
        return {"results": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
