# tests/integration/test_ingestion_flow.py
"""
Integration tests for event ingestion flow.
"""
import pytest
from datetime import datetime
from src.ingestion.event_generator import EventGenerator
from src.ingestion.validators import EventValidator
from src.storage.database import get_db_manager
from src.storage.models import EventSchema


@pytest.fixture(scope="module")
def db():
    """Get database manager for tests"""
    db_manager = get_db_manager()
    db_manager.create_tables()
    yield db_manager
    # Cleanup after tests
    # In a real scenario, you might want to drop test tables here


@pytest.fixture
def event_generator():
    """Get event generator with fixed seed for reproducibility"""
    return EventGenerator(seed=42)


@pytest.fixture
def validator():
    """Get event validator"""
    return EventValidator(strict_mode=True)


class TestIngestionFlow:
    """Test end-to-end ingestion flow"""
    
    def test_generate_and_validate_events(self, event_generator, validator):
        """Test that generated events pass validation"""
        # Generate a batch of events
        events = event_generator.generate_events_batch(batch_size=10)
        
        assert len(events) == 10
        
        # Validate each event
        valid_count = 0
        for event in events:
            result = validator.validate_event(event)
            if result.is_valid:
                valid_count += 1
        
        # Most events should be valid (allow for some randomness)
        assert valid_count >= 8
    
    def test_insert_events_to_database(self, db, event_generator, validator):
        """Test inserting events into database"""
        # Generate and validate events
        events = event_generator.generate_events_batch(batch_size=5)
        
        valid_events = []
        for event in events:
            result = validator.validate_event(event)
            if result.is_valid:
                valid_events.append(EventSchema(**event))
        
        # Insert into database
        initial_count = db.get_event_count()
        inserted_count = db.insert_events(valid_events)
        
        assert inserted_count == len(valid_events)
        
        # Verify count increased
        final_count = db.get_event_count()
        assert final_count == initial_count + inserted_count
    
    def test_query_inserted_events(self, db, event_generator):
        """Test querying events from database"""
        # Generate and insert events
        events = event_generator.generate_events_batch(batch_size=5)
        valid_events = [EventSchema(**e) for e in events]
        db.insert_events(valid_events)
        
        # Query recent events
        recent_events = db.get_latest_events(limit=5)
        
        assert len(recent_events) >= 5
        assert all('event_id' in event for event in recent_events)
        assert all('event_type' in event for event in recent_events)
    
    def test_filter_events_by_type(self, db, event_generator):
        """Test filtering events by type"""
        # Generate events with specific types
        events = event_generator.generate_events_batch(batch_size=20)
        valid_events = [EventSchema(**e) for e in events]
        db.insert_events(valid_events)
        
        # Count page view events
        page_view_count = db.get_event_count(event_type='page_view')
        
        assert page_view_count > 0
    
    def test_concurrent_event_insertion(self, db, event_generator):
        """Test handling concurrent event insertions"""
        import threading
        
        def insert_batch():
            events = event_generator.generate_events_batch(batch_size=10)
            valid_events = [EventSchema(**e) for e in events]
            db.insert_events(valid_events)
        
        # Create multiple threads
        threads = [threading.Thread(target=insert_batch) for _ in range(3)]
        
        initial_count = db.get_event_count()
        
        # Start all threads
        for thread in threads:
            thread.start()
        
        # Wait for completion
        for thread in threads:
            thread.join()
        
        # Verify all events were inserted
        final_count = db.get_event_count()
        assert final_count >= initial_count + 30