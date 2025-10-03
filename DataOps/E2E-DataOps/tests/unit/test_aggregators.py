# tests/unit/test_aggregators.py
"""
Unit tests for data aggregators.
"""
import pytest
from datetime import datetime, timedelta
from decimal import Decimal
from unittest.mock import Mock, MagicMock
from src.processing.aggregators import MetricsAggregator


class TestMetricsAggregator:
    """Test cases for MetricsAggregator"""
    
    @pytest.fixture
    def aggregator(self):
        return MetricsAggregator()
    
    def test_calculate_hour_metrics(self, aggregator, mocker):
        """Test hourly metrics calculation"""
        # Mock database session
        mock_session = MagicMock()
        mock_query = MagicMock()
        
        # Setup mock returns
        mock_query.count.return_value = 100
        mock_query.with_entities.return_value.scalar.return_value = 50
        
        mock_session.query.return_value = mock_query
        
        # Mock the get_session context manager
        mocker.patch.object(
            aggregator.db,
            'get_session',
            return_value=MagicMock(__enter__=lambda x: mock_session, __exit__=lambda *args: None)
        )
        
        # Test
        start_time = datetime(2024, 1, 15, 10, 0, 0)
        end_time = datetime(2024, 1, 15, 11, 0, 0)
        
        metrics = aggregator._calculate_hour_metrics(start_time, end_time)
        
        # Verify metrics structure
        assert 'total_events' in metrics
        assert 'unique_users' in metrics
        assert 'page_views' in metrics
        assert 'purchases' in metrics
        assert 'total_revenue' in metrics
    
    def test_aggregate_hourly_metrics(self, aggregator, mocker):
        """Test hourly aggregation across multiple hours"""
        # Mock the calculate and upsert methods
        mocker.patch.object(
            aggregator,
            '_calculate_hour_metrics',
            return_value={
                'total_events': 100,
                'unique_users': 50,
                'page_views': 75,
                'purchases': 10,
                'total_revenue': 500.0
            }
        )
        mocker.patch.object(aggregator, '_upsert_hourly_metrics')
        
        start_hour = datetime(2024, 1, 15, 10, 0, 0)
        end_hour = datetime(2024, 1, 15, 13, 0, 0)
        
        hours_processed = aggregator.aggregate_hourly_metrics(start_hour, end_hour)
        
        # Should process 3 hours
        assert hours_processed == 3
    
    def test_calculate_product_metrics(self, aggregator, mocker):
        """Test product metrics calculation"""
        mock_session = MagicMock()
        mock_query = MagicMock()
        
        # Setup mock for category
        mock_event = MagicMock()
        mock_event.category = 'Electronics'
        mock_query.first.return_value = mock_event
        
        # Setup mock for counts
        mock_query.count.side_effect = [10, 5, 2]  # views, cart_adds, purchases
        
        # Setup mock for revenue
        mock_session.query.return_value.filter.return_value.scalar.return_value = Decimal('199.98')
        
        mock_session.query.return_value = mock_query
        
        start_time = datetime(2024, 1, 15, 0, 0, 0)
        end_time = datetime(2024, 1, 16, 0, 0, 0)
        
        metrics = aggregator._calculate_product_metrics(
            mock_session, 'prod_1', start_time, end_time
        )
        
        assert metrics['category'] == 'Electronics'
        assert 'views' in metrics
        assert 'cart_adds' in metrics
        assert 'purchases' in metrics
        assert 'conversion_rate' in metrics
        
        # tests/unit/test_validators.py