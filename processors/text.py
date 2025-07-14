"""Text processing utilities for Airweave."""
from typing import List, Dict, Iterator
import re
from dataclasses import dataclass
from sources.base_source import Chunk

@dataclass
class ProcessedChunk:
    """A processed chunk ready for embedding."""
    text: str
    metadata: Dict
    source_chunk: Chunk

class TextProcessor:
    """Process text content into chunks suitable for embedding."""
    
    def __init__(self, chunk_size: int = 1000, chunk_overlap: int = 200):
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
    
    def clean_text(self, text: str) -> str:
        """Clean text content."""
        # Remove multiple newlines
        text = re.sub(r'\n\s*\n', '\n\n', text)
        # Remove multiple spaces
        text = re.sub(r'\s+', ' ', text)
        return text.strip()
    
    def split_text(self, text: str) -> List[str]:
        """Split text into overlapping chunks."""
        if not text:
            return []
            
        chunks = []
        start = 0
        text_len = len(text)
        
        while start < text_len:
            end = start + self.chunk_size
            chunk = text[start:end]
            
            # Try to break at sentence boundary
            if end < text_len:
                last_period = chunk.rfind('.')
                if last_period > 0:
                    end = start + last_period + 1
                    chunk = text[start:end]
            
            chunks.append(chunk)
            start = end - self.chunk_overlap
            
        return chunks
    
    def process_chunk(self, chunk: Chunk) -> Iterator[ProcessedChunk]:
        """Process a single chunk into embedable chunks."""
        if not chunk.content:
            return
            
        # Handle different content types
        content = chunk.content
        if isinstance(content, bytes):
            try:
                content = content.decode('utf-8', errors='replace')
            except:
                return
                
        # Clean the text
        cleaned_text = self.clean_text(content)
        
        # Split into smaller chunks
        text_chunks = self.split_text(cleaned_text)
        
        # Create processed chunks
        for i, text in enumerate(text_chunks):
            metadata = chunk.metadata.copy()
            metadata.update({
                'chunk_index': i,
                'total_chunks': len(text_chunks),
                'is_processed': True
            })
            
            yield ProcessedChunk(
                text=text,
                metadata=metadata,
                source_chunk=chunk
            )
