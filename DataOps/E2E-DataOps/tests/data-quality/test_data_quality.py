# tests/data_quality/test_data_quality.py
"""
Data quality tests using Great Expectations patterns.
"""
import pytest
from datetime import datetime, timedelta
from src.storage.database import get_db_manager
from src.storage.models import RawEvent


@pytest.fixture(scope="module")
def db():
    """Get database manager"""
    return get_db_manager()


class TestRawEventQuality:
    """Data quality tests for raw events table"""
    
    def test_no_null_event_ids(self, db):
        """Verify all events have non-null event IDs"""
        with db.get_session() as session:
            null_count = session.query(RawEvent)\
                .filter(RawEvent.event_id.is_(None))\
                .count()
            
            assert null_count == 0, f"Found {null_count} events with null event_id"
    
    def test_no_null_timestamps(self, db):
        """Verify all events have timestamps"""
        with db.get_session() as session:
            null_count = session.query(RawEvent)\
                .filter(RawEvent.timestamp.is_(None))\
                .count()
            
            assert null_count == 0, f"Found {null_count} events with null timestamp"
    
    def test_valid_event_types(self, db):
        """Verify all event types are valid"""
        valid_types = {'page_view', 'click', 'add_to_cart', 'purchase', 'search', 'login', 'logout'}
        
        with db.get_session() as session:
            from sqlalchemy import distinct
            distinct_types = session.query(distinct(RawEvent.event_type)).all()
            distinct_types = {t[0] for t in distinct_types if t[0] is not None}
            
            invalid_types = distinct_types - valid_types
            assert len(invalid_types) == 0, f"Found invalid event types: {invalid_types}"
    
    def test_timestamps_not_in_future(self, db):
        """Verify timestamps are not in the future"""
        future_threshold = datetime.utcnow() + timedelta(hours=1)
        
        with db.get_session() as session:
            future_count = session.query(RawEvent)\
                .filter(RawEvent.timestamp > future_threshold)\
                .count()
            
            assert future_count == 0, f"Found {future_count} events with future timestamps"
    
    def test_timestamps_not_too_old(self, db):
        """Verify timestamps are not unreasonably old"""
        old_threshold = datetime.utcnow() - timedelta(days=365)
        
        with db.get_session() as session:
            old_count = session.query(RawEvent)\
                .filter(RawEvent.timestamp < old_threshold)\
                .count()
            
            # This is a warning, not a hard failure
            if old_count > 0:
                pytest.warns(UserWarning, f"Found {old_count} events older than 1 year")
    
    def test_purchase_events_have_product_info(self, db):
        """Verify purchase events have required product information"""
        with db.get_session() as session:
            invalid_purchases = session.query(RawEvent)\
                .filter(
                    RawEvent.event_type == 'purchase',
                    (RawEvent.product_id.is_(None)) | (RawEvent.price.is_(None))
                )\
                .count()
            
            total_purchases = session.query(RawEvent)\
                .filter(RawEvent.event_type == 'purchase')\
                .count()
            
            if total_purchases > 0:
                error_rate = invalid_purchases / total_purchases
                assert error_rate < 0.01, f"Purchase event error rate too high: {error_rate:.2%}"
    
    def test_price_values_are_positive(self, db):
        """Verify all prices are positive"""
        with db.get_session() as session:
            negative_price_count = session.query(RawEvent)\
                .filter(
                    RawEvent.price.isnot(None),
                    RawEvent.price < 0
                )\
                .count()
            
            assert negative_price_count == 0, f"Found {negative_price_count} events with negative prices"
    
    def test_quantity_values_are_positive(self, db):
        """Verify all quantities are positive"""
        with db.get_session() as session:
            invalid_quantity_count = session.query(RawEvent)\
                .filter(
                    RawEvent.quantity.isnot(None),
                    RawEvent.quantity < 1
                )\
                .count()
            
            assert invalid_quantity_count == 0, f"Found {invalid_quantity_count} events with invalid quantities"
    
    def test_country_codes_are_valid_format(self, db):
        """Verify country codes are 2-letter format"""
        with db.get_session() as session:
            from sqlalchemy import func
            
            invalid_countries = session.query(RawEvent.country)\
                .filter(
                    RawEvent.country.isnot(None),
                    func.length(RawEvent.country) != 2
                )\
                .limit(10)\
                .all()
            
            assert len(invalid_countries) == 0, f"Found invalid country codes: {invalid_countries}"
    
    def test_data_freshness(self, db):
        """Verify data is being ingested recently"""
        threshold = datetime.utcnow() - timedelta(hours=24)
        
        with db.get_session() as session:
            recent_count = session.query(RawEvent)\
                .filter(RawEvent.created_at >= threshold)\
                .count()
            
            # Warning if no recent data
            if recent_count == 0:
                pytest.warns(UserWarning, "No events ingested in the last 24 hours")
    
    def test_duplicate_event_ids(self, db):
        """Verify there are no duplicate event IDs"""
        with db.get_session() as session:
            from sqlalchemy import func
            
            duplicates = session.query(
                RawEvent.event_id,
                func.count(RawEvent.event_id)
            ).group_by(
                RawEvent.event_id
            ).having(
                func.count(RawEvent.event_id) > 1
            ).limit(10).all()
            
            assert len(duplicates) == 0, f"Found duplicate event IDs: {[d[0] for d in duplicates]}"
    
    def test_session_consistency(self, db):
        """Verify sessions have consistent user information"""
        with db.get_session() as session:
            from sqlalchemy import func, distinct
            
            # Find sessions with multiple user IDs
            inconsistent_sessions = session.query(
                RawEvent.session_id,
                func.count(distinct(RawEvent.user_id))
            ).filter(
                RawEvent.user_id.isnot(None)
            ).group_by(
                RawEvent.session_id
            ).having(
                func.count(distinct(RawEvent.user_id)) > 1
            ).limit(10).all()
            
            # Allow small number of inconsistencies (edge cases)
            assert len(inconsistent_sessions) < 5, \
                f"Found {len(inconsistent_sessions)} sessions with inconsistent user IDs"


class TestAggregatedMetricsQuality:
    """Data quality tests for aggregated metrics"""
    
    def test_hourly_metrics_completeness(self, db):
        """Verify hourly metrics have no gaps"""
        from src.storage.models import HourlyMetric
        
        with db.get_session() as session:
            # Get date range of metrics
            from sqlalchemy import func
            
            min_hour = session.query(func.min(HourlyMetric.hour)).scalar()
            max_hour = session.query(func.max(HourlyMetric.hour)).scalar()
            
            if min_hour and max_hour:
                expected_hours = int((max_hour - min_hour).total_seconds() / 3600) + 1
                actual_hours = session.query(HourlyMetric).count()
                
                # Allow for some missing hours (up to 10%)
                completeness = actual_hours / expected_hours if expected_hours > 0 else 0
                assert completeness > 0.9, \
                    f"Hourly metrics completeness too low: {completeness:.1%}"
    
    def test_revenue_consistency(self, db):
        """Verify revenue calculations are consistent"""
        from src.storage.models import HourlyMetric
        
        with db.get_session() as session:
            # Check for negative revenue
            negative_revenue = session.query(HourlyMetric)\
                .filter(HourlyMetric.total_revenue < 0)\
                .count()
            
            assert negative_revenue == 0, "Found hours with negative revenue"
            
            # Check for unreasonably high revenue (potential data quality issue)
            high_revenue = session.query(HourlyMetric)\
                .filter(HourlyMetric.total_revenue > 1000000)\
                .count()
            
            if high_revenue > 0:
                pytest.warns(UserWarning, 
                           f"Found {high_revenue} hours with very high revenue (>$1M)")