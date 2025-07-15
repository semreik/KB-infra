"""Unit tests for data validators."""
import pytest
from datetime import datetime
from validators.schema import SchemaValidator
from validators.quality import QualityChecker

@pytest.fixture
def valid_email_data():
    return {
        'subject': 'Test Email',
        'from': 'sender@example.com',
        'to': 'recipient@example.com',
        'date': '2025-01-15T10:00:00Z',
        'content': 'This is a test email content.'
    }

@pytest.fixture
def valid_drive_data():
    return {
        'id': 'file123',
        'name': 'test.pdf',
        'mime_type': 'application/pdf',
        'created_time': '2025-01-15T10:00:00Z',
        'modified_time': '2025-01-15T11:00:00Z',
        'size': 1024,
        'web_view_link': 'https://drive.google.com/file/d/123'
    }

@pytest.fixture
def valid_po_data():
    return {
        'po_number': 'PO001',
        'vendor': 'Test Vendor',
        'date': '2025-01-15T10:00:00Z',
        'total_amount': 1500.50,
        'status': 'approved',
        'items': 'Item1, Item2'
    }

class TestSchemaValidator:
    def test_validate_email(self, valid_email_data):
        """Test email schema validation."""
        validated = SchemaValidator.validate(valid_email_data, 'email')
        assert validated['subject'] == 'Test Email'
        assert validated['from_'] == 'sender@example.com'

    def test_validate_drive(self, valid_drive_data):
        """Test Drive file schema validation."""
        validated = SchemaValidator.validate(valid_drive_data, 'drive')
        assert validated['id'] == 'file123'
        assert validated['mime_type'] == 'application/pdf'

    def test_validate_po(self, valid_po_data):
        """Test purchase order schema validation."""
        validated = SchemaValidator.validate(valid_po_data, 'purchase_order')
        assert validated['po_number'] == 'PO001'
        assert validated['total_amount'] == 1500.50

    def test_invalid_schema_type(self):
        """Test error on invalid schema type."""
        with pytest.raises(ValueError):
            SchemaValidator.validate({}, 'invalid_type')

    def test_validate_batch(self, valid_email_data):
        """Test batch validation."""
        data_list = [valid_email_data, valid_email_data]
        validated = SchemaValidator.validate_batch(data_list, 'email')
        assert len(validated) == 2

class TestQualityChecker:
    def test_check_email_quality(self, valid_email_data):
        """Test email quality checks."""
        issues = QualityChecker.check_quality(valid_email_data, 'email')
        assert not issues  # No issues for valid data

        # Test invalid email
        invalid_data = valid_email_data.copy()
        invalid_data['from'] = 'invalid-email'
        issues = QualityChecker.check_quality(invalid_data, 'email')
        assert len(issues) == 1
        assert 'Invalid \'from\' email format' in issues[0]

    def test_check_drive_quality(self, valid_drive_data):
        """Test Drive file quality checks."""
        issues = QualityChecker.check_quality(valid_drive_data, 'drive')
        assert not issues  # No issues for valid data

        # Test invalid mime type
        invalid_data = valid_drive_data.copy()
        invalid_data['mime_type'] = 'invalid-mime-type'
        issues = QualityChecker.check_quality(invalid_data, 'drive')
        assert len(issues) == 1
        assert 'Invalid mime type format' in issues[0]

    def test_check_po_quality(self, valid_po_data):
        """Test purchase order quality checks."""
        issues = QualityChecker.check_quality(valid_po_data, 'purchase_order')
        assert not issues  # No issues for valid data

        # Test invalid amount
        invalid_data = valid_po_data.copy()
        invalid_data['total_amount'] = -100
        issues = QualityChecker.check_quality(invalid_data, 'purchase_order')
        assert len(issues) == 1
        assert 'Invalid total amount' in issues[0]

    def test_check_batch_quality(self, valid_po_data):
        """Test batch quality checks."""
        # Create a batch with one valid and one invalid item
        invalid_data = valid_po_data.copy()
        invalid_data['total_amount'] = -100
        
        data_list = [valid_po_data, invalid_data]
        issues = QualityChecker.check_batch_quality(data_list, 'purchase_order')
        
        assert len(issues) == 1  # Only invalid item should have issues
        assert 1 in issues  # Issue should be reported for second item
