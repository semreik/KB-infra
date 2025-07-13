import os
import pytest
from unittest.mock import Mock, patch
from datetime import datetime

@pytest.fixture
def postgres_config():
    return {
        'host': 'localhost',
        'port': 5432,
        'database': 'test_db',
        'user': 'test_user',
        'password': 'test_pass',
        'tables': ['users', 'orders']
    }

@pytest.fixture
def mock_pg_users():
    return [
        (1, 'user1', 'user1@example.com', '2023-07-13 10:00:00'),
        (2, 'user2', 'user2@example.com', '2023-07-13 11:00:00')
    ]

@pytest.fixture
def mock_pg_orders():
    return [
        (1001, 1, 'Product A', 100.00, '2023-07-13 10:30:00'),
        (1002, 2, 'Product B', 200.00, '2023-07-13 11:30:00')
    ]

@pytest.fixture
def mock_postgres_tables():
    return [('users',), ('orders',)]

@pytest.fixture
def mock_postgres_data():
    return [
        {'email': 'user1@example.com', 'name': 'John Doe', 'created_at': '2023-07-13'},
        {'email': 'user2@example.com', 'name': 'Jane Smith', 'created_at': '2023-07-13'}
    ]

def test_smoke_postgres(postgres_config, mock_postgres_tables, mock_postgres_data):
    """Smoke test for Postgres connector."""
    with patch('psycopg2.connect') as mock_connect:
        # Mock connection
        mock_conn = Mock()
        mock_connect.return_value = mock_conn
        
        # Mock connection context manager
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=None)
        
        # Mock cursor
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        
        # Mock cursor context manager
        mock_cursor.__enter__ = Mock(return_value=mock_cursor)
        mock_cursor.__exit__ = Mock(return_value=None)
        
        # Configure mock cursor responses
        mock_cursor.fetchall.side_effect = [mock_postgres_tables, mock_postgres_data]
        mock_cursor.description = [
            ('email',), ('name',), ('created_at',)
        ]
        mock_cursor.__enter__.return_value = mock_cursor
        mock_cursor.__exit__.return_value = None

        from sources.postgres_source import PostgresSource
        source = PostgresSource(postgres_config)
        
        # Should list tables
        tables = list(source.list_entities())
        assert len(tables) == 2
        assert 'users' in tables
        assert 'orders' in tables
        
        # Should get table content
        chunks = list(source.iter_content('users'))
        assert len(chunks) == 1
        chunk = chunks[0]
        assert 'user1@example.com' in chunk.content
        assert chunk.metadata['source'] == 'postgres'
        assert chunk.metadata['table'] == 'users'
        
        chunks = list(source.iter_content('orders'))
        assert len(chunks) == 1
        chunk = chunks[0]
        assert 'Product A' in chunk.content
        assert chunk.metadata['source'] == 'postgres'
        assert chunk.metadata['table'] == 'orders'
