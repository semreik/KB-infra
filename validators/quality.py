"""Data quality checks for ingested data."""
from typing import Dict, Any, List, Optional
from datetime import datetime
import re
import logging

logger = logging.getLogger(__name__)

class QualityChecker:
    """Performs data quality checks on ingested data."""
    
    @staticmethod
    def check_email_quality(data: Dict[str, Any]) -> List[str]:
        """
        Check quality of email data.
        
        Args:
            data: Dictionary containing email data
            
        Returns:
            List of quality issues found
        """
        issues = []
        
        # Check email format
        email_pattern = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
        if 'from' in data and not email_pattern.match(data['from']):
            issues.append(f"Invalid 'from' email format: {data['from']}")
        
        # Check content length
        if len(data.get('content', '')) < 10:
            issues.append("Content too short (< 10 chars)")
        
        # Check date
        try:
            if isinstance(data.get('date'), str):
                datetime.fromisoformat(data['date'].replace('Z', '+00:00'))
        except (ValueError, TypeError):
            issues.append(f"Invalid date format: {data.get('date')}")
        
        return issues

    @staticmethod
    def check_drive_quality(data: Dict[str, Any]) -> List[str]:
        """
        Check quality of Drive file metadata.
        
        Args:
            data: Dictionary containing Drive file metadata
            
        Returns:
            List of quality issues found
        """
        issues = []
        
        # Check required fields
        if not data.get('id'):
            issues.append("Missing file ID")
        
        # Check mime type format
        mime_type = data.get('mime_type', '')
        if not re.match(r'^[\w-]+/[\w-]+$', mime_type):
            issues.append(f"Invalid mime type format: {mime_type}")
        
        # Check timestamps
        for field in ['created_time', 'modified_time']:
            try:
                if isinstance(data.get(field), str):
                    datetime.fromisoformat(data[field].replace('Z', '+00:00'))
            except (ValueError, TypeError, KeyError):
                issues.append(f"Invalid {field} format: {data.get(field)}")
        
        return issues

    @staticmethod
    def check_po_quality(data: Dict[str, Any]) -> List[str]:
        """
        Check quality of purchase order data.
        
        Args:
            data: Dictionary containing purchase order data
            
        Returns:
            List of quality issues found
        """
        issues = []
        
        # Check PO number format
        if not re.match(r'^PO\d{3,}$', data.get('po_number', '')):
            issues.append(f"Invalid PO number format: {data.get('po_number')}")
        
        # Check amount
        amount = data.get('total_amount')
        if not isinstance(amount, (int, float)) or amount <= 0:
            issues.append(f"Invalid total amount: {amount}")
        
        # Check status
        valid_statuses = {'pending', 'approved', 'rejected'}
        if data.get('status') not in valid_statuses:
            issues.append(f"Invalid status: {data.get('status')}")
        
        return issues

    @classmethod
    def check_quality(cls, data: Dict[str, Any], data_type: str) -> List[str]:
        """
        Check quality of data based on its type.
        
        Args:
            data: Dictionary containing the data
            data_type: Type of data to check
            
        Returns:
            List of quality issues found
            
        Raises:
            ValueError: If data_type is not supported
        """
        checkers = {
            'email': cls.check_email_quality,
            'drive': cls.check_drive_quality,
            'purchase_order': cls.check_po_quality
        }
        
        if data_type not in checkers:
            raise ValueError(f"Unsupported data type: {data_type}")
        
        return checkers[data_type](data)

    @classmethod
    def check_batch_quality(cls, data_list: List[Dict[str, Any]], data_type: str) -> Dict[int, List[str]]:
        """
        Check quality of a batch of data items.
        
        Args:
            data_list: List of dictionaries to check
            data_type: Type of data to check
            
        Returns:
            Dictionary mapping item indices to their quality issues
        """
        issues = {}
        for i, item in enumerate(data_list):
            item_issues = cls.check_quality(item, data_type)
            if item_issues:
                issues[i] = item_issues
        return issues
