# tests/unit/test_validators.py
"""
Unit tests for event validators.
"""
import pytest
from datetime import datetime, timedelta
from src.ingestion.validators import EventValidator, ValidationResult


class TestEventValidator:
    """Test cases for EventValidator"""
    
    @pytest.fixture
    def validator(self):
        return EventValidator(strict_mode=True)
    
    @pytest.fixture
    def valid_event(self):
        return {
            'event_id': '123e4567-e89b-12d3-a456-426614174000',
            'event_type': 'page_view',
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'user_id': 'user_12345',
            'session_id': 'session_67890',
            'page_url': '/products',
            'device_type': 'desktop',
            'country': 'US',
            'ip_address': '192.168.1.1'
        }
    
    def test_valid_event(self, validator, valid_event):
        """Test that a valid event passes validation"""
        result = validator.validate_event(valid_event)
        assert result.is_valid
        assert len(result.errors) == 0
    
    def test_missing_required_fields(self, validator):
        """Test that missing required fields are caught"""
        invalid_event = {
            'event_type': 'page_view'
        }
        result = validator.validate_event(invalid_event)
        assert not result.is_valid
        assert any('event_id' in error for error in result.errors)
        assert any('timestamp' in error for error in result.errors)
        assert any('session_id' in error for error in result.errors)
    
    def test_invalid_event_type(self, validator, valid_event):
        """Test that invalid event types are rejected"""
        valid_event['event_type'] = 'invalid_type'
        result = validator.validate_event(valid_event)
        assert not result.is_valid
        assert any('event_type' in error for error in result.errors)
    
    def test_invalid_uuid_format(self, validator, valid_event):
        """Test that invalid UUID format is caught"""
        valid_event['event_id'] = 'not-a-valid-uuid'
        result = validator.validate_event(valid_event)
        assert not result.is_valid
        assert any('UUID' in error for error in result.errors)
    
    def test_future_timestamp(self, validator, valid_event):
        """Test that future timestamps are flagged"""
        future_time = datetime.utcnow() + timedelta(hours=2)
        valid_event['timestamp'] = future_time.isoformat() + 'Z'
        result = validator.validate_event(valid_event)
        assert not result.is_valid
        assert any('future' in error.lower() for error in result.errors)
    
    def test_invalid_price(self, validator, valid_event):
        """Test that invalid prices are caught"""
        valid_event['event_type'] = 'purchase'
        valid_event['product_id'] = 'prod_001'
        valid_event['price'] = -10.50
        result = validator.validate_event(valid_event)
        assert not result.is_valid
        assert any('price' in error.lower() for error in result.errors)
    
    def test_purchase_without_product_info(self, validator, valid_event):
        """Test that purchase events require product information"""
        valid_event['event_type'] = 'purchase'
        result = validator.validate_event(valid_event)
        assert not result.is_valid
        assert any('product_id' in error for error in result.errors)
    
    def test_invalid_country_code(self, validator, valid_event):
        """Test that invalid country codes are caught"""
        valid_event['country'] = 'USA'  # Should be 2-letter code
        result = validator.validate_event(valid_event)
        assert not result.is_valid
        assert any('country' in error.lower() for error in result.errors)
    
    def test_invalid_ip_address(self, validator, valid_event):
        """Test that invalid IP addresses are caught"""
        valid_event['ip_address'] = '999.999.999.999'
        result = validator.validate_event(valid_event)
        assert not result.is_valid
        assert any('ip_address' in error.lower() for error in result.errors)
    
    def test_warnings_generation(self, validator, valid_event):
        """Test that warnings are generated appropriately"""
        valid_event['user_id'] = None  # Anonymous user
        result = validator.validate_event(valid_event)
        assert result.is_valid  # Still valid
        assert len(result.warnings) > 0
        assert any('user_id' in warning.lower() for warning in result.warnings)
    
    def test_batch_validation(self, validator, valid_event):
        """Test batch validation functionality"""
        events = [valid_event.copy() for _ in range(5)]
        events[2]['event_id'] = 'invalid'  # Make one invalid
        
        batch_result = validator.validate_batch(events)
        assert batch_result['total_events'] == 5
        assert batch_result['valid_events'] == 4
        assert batch_result['invalid_events'] == 1