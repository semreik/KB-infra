from typing import Dict, Iterator
import psycopg2
from psycopg2.extras import RealDictCursor
from .base_source import BaseSource, Chunk

class PostgresSource(BaseSource):
    """PostgreSQL connector for Airweave."""
    
    def __init__(self, config: Dict):
        super().__init__(config)
        self.conn_params = {
            'host': config['host'],
            'port': config['port'],
            'database': config['database'],
            'user': config['user'],
            'password': config['password']
        }
        if 'ssl_mode' in config:
            self.conn_params['sslmode'] = config['ssl_mode']
        self.tables = config.get('tables', [])
    
    def list_entities(self) -> Iterator[str]:
        """List all table names."""
        with psycopg2.connect(**self.conn_params) as conn:
            with conn.cursor() as cur:
                if self.tables:
                    # Use configured tables
                    for table in self.tables:
                        yield table['name'] if isinstance(table, dict) else table
                else:
                    # List all tables in public schema
                    cur.execute("""
                        SELECT table_name 
                        FROM information_schema.tables 
                        WHERE table_schema = 'public'
                    """)
                    for (table_name,) in cur.fetchall():
                        yield table_name
    
    def iter_content(self, table_name: str) -> Iterator[Chunk]:
        """Get content of a specific table."""
        with psycopg2.connect(**self.conn_params) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Find table config if it exists
                table_config = None
                if self.tables:
                    for table in self.tables:
                        if isinstance(table, dict) and table['name'] == table_name:
                            table_config = table
                            break
                
                # Execute query
                if table_config and 'query' in table_config:
                    cur.execute(table_config['query'])
                else:
                    cur.execute(f"SELECT * FROM {table_name} LIMIT 1000")
                
                rows = cur.fetchall()
                if not rows:
                    return
                
                # Format as readable text
                content = f"Table: {table_name}\n\n"
                for row in rows:
                    content += "---\n"
                    for key, value in row.items():
                        content += f"{key}: {value}\n"
                
                yield Chunk(
                    content=content,
                    metadata={
                        'source': 'postgres',
                        'table': table_name,
                        'row_count': len(rows)
                    }
                )
