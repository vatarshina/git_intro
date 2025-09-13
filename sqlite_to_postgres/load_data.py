import sqlite3
import logging
from contextlib import closing
from typing import Generator, List, Dict, Any

import psycopg
from psycopg import ClientCursor

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

BATCH_SIZE = 500
DSL = {
    'dbname': 'movies_database',
    'user': 'app', 
    'password': '123qwe',
    'host': '127.0.0.1',
    'port': 5432
}

TABLE_MAPPING = {
    'film_work': 'film_work',
    'genre': 'genre', 
    'person': 'person',
    'genre_film_work': 'genre_film_work',
    'person_film_work': 'person_film_work'
}

MIGRATION_ORDER = ['genre', 'person', 'film_work', 'genre_film_work', 'person_film_work']

NULLABLE_FIELDS = {
    'genre': ['description'],
    'film_work': ['description', 'creation_date', 'file_path', 'rating'],
    'person': [] 
}

FIELD_TYPES = {
    'film_work': {
        'creation_date': 'date',
        'rating': 'float',
        'description': 'text'
    },
    'genre': {
        'description': 'text'
    }
}


class SQLiteLoader:
    def __init__(self, connection: sqlite3.Connection):
        self.conn = connection
        self.conn.row_factory = sqlite3.Row

    def get_table_names(self) -> List[str]:
        """Получение списка всех таблиц в SQLite"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
            tables = [row[0] for row in cursor.fetchall()]
            return [table for table in tables if table in TABLE_MAPPING]
        except sqlite3.Error as e:
            logger.error(f"Error getting table names: {e}")
            return []

    def load_table_data(self, table_name: str, batch_size: int = BATCH_SIZE) -> Generator[List[Dict[str, Any]], None, None]:
        """Загрузка данных из SQLite пачками"""
        try:
            cursor = self.conn.cursor()
            cursor.execute(f'SELECT * FROM "{table_name}"')
            
            while batch := cursor.fetchmany(batch_size):
                yield [dict(row) for row in batch]
                
        except sqlite3.Error as e:
            logger.error(f"Error loading data from {table_name}: {e}")
            raise

    def get_table_count(self, table_name: str) -> int:
        """Получение количества записей в таблице"""
        try:
            cursor = self.conn.cursor()
            cursor.execute(f'SELECT COUNT(*) FROM "{table_name}"')
            return cursor.fetchone()[0]
        except sqlite3.Error as e:
            logger.error(f"Error getting count from {table_name}: {e}")
            return 0

    def get_table_columns(self, table_name: str) -> List[str]:
        """Получение списка колонок таблицы"""
        try:
            cursor = self.conn.cursor()
            cursor.execute(f'PRAGMA table_info("{table_name}")')
            return [row[1] for row in cursor.fetchall()]
        except sqlite3.Error as e:
            logger.error(f"Error getting columns from {table_name}: {e}")
            return []


class PostgresSaver:
    def __init__(self, connection: psycopg.Connection):
        self.conn = connection

    def convert_value(self, table_name: str, field_name: str, value: Any) -> Any:
        """Конвертация значения в соответствии с типом поля"""
        if value is None:
            field_types = FIELD_TYPES.get(table_name, {})
            field_type = field_types.get(field_name, 'text')
            
            if field_type == 'date':
                return None  
            elif field_type == 'float':
                return None  
            else:
                return ''  
        
        if isinstance(value, str) and value.strip() == '':
            field_types = FIELD_TYPES.get(table_name, {})
            field_type = field_types.get(field_name, 'text')
            
            if field_type == 'date':
                return None
            elif field_type == 'float':
                return None
        
        return value

    def convert_row_values(self, table_name: str, row: Dict[str, Any]) -> Dict[str, Any]:
        """Конвертация всех значений в строке"""
        converted_row = {}
        for key, value in row.items():
            converted_row[key] = self.convert_value(table_name, key, value)
        return converted_row

    def save_batch(self, table_name: str, columns: List[str], batch: List[Dict[str, Any]]):
        """Сохранение пачки данных в PostgreSQL"""
        try:
            cursor = self.conn.cursor()
            placeholders = ", ".join(["%s"] * len(columns))
            columns_str = ", ".join(columns)
            
            pg_table_name = TABLE_MAPPING.get(table_name, table_name)
            
            sql = f"""
                INSERT INTO content.{pg_table_name} ({columns_str})
                VALUES ({placeholders})
                ON CONFLICT (id) DO NOTHING
            """
            
            values = []
            for row in batch:
                converted_row = self.convert_row_values(table_name, row)
                row_values = [converted_row.get(col) for col in columns]
                values.append(row_values)
            
            cursor.executemany(sql, values)
            self.conn.commit()
            
            logger.info(f"Inserted {len(values)} rows into content.{pg_table_name}")
            
        except psycopg.Error as e:
            self.conn.rollback()
            logger.error(f"Error saving batch to {table_name}: {e}")
            raise

    def get_table_count(self, table_name: str) -> int:
        """Получение количества записей в таблице PostgreSQL"""
        try:
            cursor = self.conn.cursor()
            pg_table_name = TABLE_MAPPING.get(table_name, table_name)
            cursor.execute(f"SELECT COUNT(*) FROM content.{pg_table_name}")
            return cursor.fetchone()[0]
        except psycopg.Error as e:
            logger.error(f"Error getting count from {table_name}: {e}")
            return 0


def migrate_table_data(
    sqlite_loader: SQLiteLoader,
    postgres_saver: PostgresSaver,
    table_name: str
):
    """Миграция данных для конкретной таблицы"""
    logger.info(f"Starting migration for table: {table_name}")
    
    columns = sqlite_loader.get_table_columns(table_name)
    if not columns:
        logger.error(f"No columns found for {table_name}, skipping...")
        return
    
    sqlite_count = sqlite_loader.get_table_count(table_name)
    logger.info(f"Total records in SQLite {table_name}: {sqlite_count}")
    
    if sqlite_count == 0:
        logger.warning(f"No data found in {table_name}, skipping...")
        return
    
    migrated_count = 0
    
    try:
        for batch in sqlite_loader.load_table_data(table_name):
            postgres_saver.save_batch(table_name, columns, batch)
            migrated_count += len(batch)
            if migrated_count % 1000 == 0:  
                logger.info(f"Migrated {migrated_count}/{sqlite_count} records from {table_name}")
            
    except Exception as e:
        logger.error(f"Failed to migrate {table_name}: {e}")
        raise
    
    logger.info(f"Finished migrating {migrated_count} records from {table_name}")
    
    postgres_count = postgres_saver.get_table_count(table_name)
    if sqlite_count == postgres_count:
        logger.info(f"✓ Successfully migrated {table_name}: {postgres_count} records")
    else:
        logger.warning(f"⚠ Data inconsistency in {table_name}: SQLite={sqlite_count}, PostgreSQL={postgres_count}")


def test_data_consistency(sqlite_conn: sqlite3.Connection, pg_conn: psycopg.Connection):
    """Тестирование консистентности данных"""
    logger.info("Testing data consistency...")
    
    sqlite_loader = SQLiteLoader(sqlite_conn)
    sqlite_tables = sqlite_loader.get_table_names()
    postgres_saver = PostgresSaver(pg_conn)
    
    for table_name in sqlite_tables:
        sqlite_count = sqlite_loader.get_table_count(table_name)
        postgres_count = postgres_saver.get_table_count(table_name)
        
        if sqlite_count == postgres_count:
            logger.info(f"✓ {table_name}: {postgres_count} records (consistent)")
        else:
            logger.error(f"✗ {table_name}: SQLite={sqlite_count}, PostgreSQL={postgres_count} (inconsistent)")
    
    logger.info("Data consistency test completed")


def main():
    """Основная функция миграции"""
    try:
        logger.info("Starting data migration from SQLite to PostgreSQL...")
        
        with closing(sqlite3.connect('db.sqlite')) as sqlite_conn, \
             closing(psycopg.connect(**DSL, cursor_factory=ClientCursor)) as pg_conn:
            
            sqlite_loader = SQLiteLoader(sqlite_conn)
            postgres_saver = PostgresSaver(pg_conn)
            
            table_names = sqlite_loader.get_table_names()
            logger.info(f"Found tables in SQLite: {table_names}")
            
            for table_name in MIGRATION_ORDER:
                if table_name in table_names:
                    migrate_table_data(sqlite_loader, postgres_saver, table_name)
                else:
                    logger.warning(f"Table {table_name} not found in SQLite, skipping...")
            
            test_data_consistency(sqlite_conn, pg_conn)
            
            logger.info("Data migration completed successfully!")
            
    except Exception as e:
        logger.error(f"Data migration failed: {e}")
        raise


if __name__ == '__main__':
    main()