# tests/unit/test_transformers.py
"""
Unit tests for data transformers.
"""
import pytest
from datetime import datetime
from src.processing.transformers import EventTransformer


class TestEventTransformer:
    """Test cases for EventTransformer"""
    
    @pytest.fixture
    def transformer(self):
        return EventTransformer()

    @pytest.fixture
    def sample_event(self):
        return {
            'event_id': '123e4567-e89b-12d3-a456-426614174000',
            'event_type': 'page_view',
            'timestamp': '2024-01-15T10:30:00Z',
            'user_id': 'user_12345',
            'session_id': 'session_67890',
            'page_url': '/products/laptop',
            'device_type': 'mobile',
            'country': 'US',
            'price': 299.99
        }
    
    def test_enrich_event(self, transformer, sample_event):
        """Test event enrichment adds derived fields"""
        enriched = transformer.enrich_event(sample_event)
        
        # Check that original fields are preserved
        assert enriched['event_id'] == sample_event['event_id']
        assert enriched['event_type'] == sample_event['event_type']
        
        # Check that derived fields are added
        assert 'hour' in enriched
        assert 'date' in enriched
        assert 'day_of_week' in enriched
        assert 'hour_of_day' in enriched
        assert 'is_weekend' in enriched
        assert 'device_category' in enriched
        assert 'price_tier' in enriched
    
    def test_price_tier_categorization(self, transformer, sample_event):
        """Test price tier categorization"""
        # Budget
        sample_event['price'] = 15.00
        enriched = transformer.enrich_event(sample_event)
        assert enriched['price_tier'] == 'budget'
        
        # Mid-range
        sample_event['price'] = 75.00
        enriched = transformer.enrich_event(sample_event)
        assert enriched['price_tier'] == 'mid-range'
        
        # Premium
        sample_event['price'] = 350.00
        enriched = transformer.enrich_event(sample_event)
        assert enriched['price_tier'] == 'premium'
        
        # Luxury
        sample_event['price'] = 1500.00
        enriched = transformer.enrich_event(sample_event)
        assert enriched['price_tier'] == 'luxury'
    
    def test_device_categorization(self, transformer, sample_event):
        """Test device categorization"""
        sample_event['device_type'] = 'mobile'
        enriched = transformer.enrich_event(sample_event)
        assert enriched['device_category'] == 'mobile'
        
        sample_event['device_type'] = 'desktop'
        enriched = transformer.enrich_event(sample_event)
        assert enriched['device_category'] == 'desktop'
    
    def test_extract_user_sessions(self, transformer):
        """Test user session extraction"""
        events = [
            {
                'session_id': 'session_1',
                'user_id': 'user_1',
                'event_type': 'page_view',
                'timestamp': '2024-01-15T10:00:00Z',
                'device_type': 'desktop',
                'country': 'US'
            },
            {
                'session_id': 'session_1',
                'user_id': 'user_1',
                'event_type': 'purchase',
                'timestamp': '2024-01-15T10:05:00Z',
                'product_id': 'prod_1',
                'price': 99.99,
                'quantity': 1
            }
        ]
        
        sessions = transformer._extract_user_sessions(events)
        
        assert len(sessions) == 1
        session = sessions[0]
        assert session['session_id'] == 'session_1'
        assert session['event_count'] == 2
        assert session['purchases'] == 1
        assert session['total_revenue'] == 99.99
    
    def test_extract_product_interactions(self, transformer):
        """Test product interaction extraction"""
        events = [
            {
                'event_type': 'page_view',
                'product_id': 'prod_1',
                'category': 'Electronics',
                'user_id': 'user_1'
            },
            {
                'event_type': 'add_to_cart',
                'product_id': 'prod_1',
                'category': 'Electronics',
                'user_id': 'user_1'
            },
            {
                'event_type': 'purchase',
                'product_id': 'prod_1',
                'category': 'Electronics',
                'price': 299.99,
                'quantity': 1,
                'user_id': 'user_2'
            }
        ]
        
        products = transformer._extract_product_interactions(events)
        
        assert len(products) == 1
        product = products[0]
        assert product['product_id'] == 'prod_1'
        assert product['views'] == 1
        assert product['cart_adds'] == 1
        assert product['purchases'] == 1
        assert product['unique_users'] == 2