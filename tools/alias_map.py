"""Supplier alias mapping for external news matching."""
import re
from typing import Dict, Set

class AliasMap:
    """Maps supplier IDs to their name aliases for news matching."""
    
    def __init__(self):
        self._aliases: Dict[str, Set[str]] = {}
        
    def add_supplier(self, supplier_id: str, name: str, aliases: Set[str] = None):
        """
        Add a supplier with its name and optional aliases.
        
        Args:
            supplier_id: Unique supplier ID
            name: Primary supplier name
            aliases: Optional set of alternative names
        """
        if aliases is None:
            aliases = set()
        aliases.add(name)  # Add primary name to aliases
        self._aliases[supplier_id] = {self._normalize(a) for a in aliases}
        
    def find_matches(self, text: str) -> Set[str]:
        """
        Find supplier IDs whose aliases match the text.
        
        Args:
            text: Text to search for supplier aliases
            
        Returns:
            Set of matching supplier IDs
        """
        text = self._normalize(text)
        matches = set()
        
        for supplier_id, aliases in self._aliases.items():
            if any(alias in text for alias in aliases):
                matches.add(supplier_id)
        
        return matches
    
    @staticmethod
    def _normalize(text: str) -> str:
        """Normalize text for matching."""
        # Convert to lowercase
        text = text.lower()
        # Remove special characters
        text = re.sub(r'[^\w\s]', ' ', text)
        # Normalize whitespace
        text = ' '.join(text.split())
        return text
