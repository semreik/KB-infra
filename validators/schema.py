"""Schema validation for data sources."""
from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field, ValidationError
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class EmailSchema(BaseModel):
    """Schema for email data."""
    subject: str
    from_: str = Field(alias='from')
    to: str
    date: datetime
    content: str = ''

class DriveFileSchema(BaseModel):
    """Schema for Google Drive file metadata."""
    id: str
    name: str
    mime_type: str
    created_time: datetime
    modified_time: datetime
    size: Optional[int] = None
    web_view_link: Optional[str] = None

class PurchaseOrderSchema(BaseModel):
    """Schema for purchase order data."""
    po_number: str
    vendor: str
    date: datetime
    total_amount: float
    status: str
    items: str

class SchemaValidator:
    """Validates data against predefined schemas."""
    
    SCHEMAS = {
        'email': EmailSchema,
        'drive': DriveFileSchema,
        'purchase_order': PurchaseOrderSchema
    }
    
    @classmethod
    def validate(cls, data: Dict[str, Any], schema_type: str) -> Dict[str, Any]:
        """
        Validate data against a schema.
        
        Args:
            data: Dictionary containing the data to validate
            schema_type: Type of schema to validate against
            
        Returns:
            Validated and normalized data
            
        Raises:
            ValueError: If schema_type is not supported
            ValidationError: If data does not match schema
        """
        if schema_type not in cls.SCHEMAS:
            raise ValueError(f"Unsupported schema type: {schema_type}")
        
        try:
            schema_class = cls.SCHEMAS[schema_type]
            validated = schema_class(**data)
            return validated.model_dump()
        except ValidationError as e:
            logger.error(f"Validation error for {schema_type}: {str(e)}")
            raise

    @classmethod
    def validate_batch(cls, data_list: List[Dict[str, Any]], schema_type: str) -> List[Dict[str, Any]]:
        """
        Validate a batch of data items against a schema.
        
        Args:
            data_list: List of dictionaries to validate
            schema_type: Type of schema to validate against
            
        Returns:
            List of validated and normalized data items
            
        Note:
            Invalid items are logged and skipped
        """
        validated_data = []
        for item in data_list:
            try:
                validated = cls.validate(item, schema_type)
                validated_data.append(validated)
            except ValidationError as e:
                logger.warning(f"Skipping invalid {schema_type} item: {str(e)}")
                continue
        return validated_data
