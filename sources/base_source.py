from dataclasses import dataclass
from typing import Dict, Iterator

@dataclass
class Chunk:
    """A chunk of content with metadata."""
    content: str
    metadata: Dict

class BaseSource:
    """Base class for all Airweave connectors."""
    
    def __init__(self, config: Dict):
        """Initialize the source with config."""
        self.config = config
    
    def list_entities(self) -> Iterator[str]:
        """List all available entity IDs."""
        raise NotImplementedError
    
    def iter_content(self, entity_id: str) -> Iterator[Chunk]:
        """Get content chunks for a specific entity."""
        raise NotImplementedError
