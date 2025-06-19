import psycopg2
import psycopg2.extras
from typing import List, Dict, Any, Optional
import logging
import json
from config import Config

logger = logging.getLogger(__name__)

class DatabaseManager:
    """Handles all database operations for file metadata storage"""

    def __init__(self):
        self.connection = None
        self.cursor = None

    def connect(self):
        """Establish connection to PostgreSQL database"""
        try:
            self.connection = psycopg2.connect(
                host=Config.DB_HOST,
                port=Config.DB_PORT,
                database=Config.DB_NAME,
                user=Config.DB_USER,
                password=Config.DB_PASSWORD
            )
            self.cursor = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
            logger.info("Successfully connected to PostgreSQL database")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            return False

    def disconnect(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info("Database connection closed")

    def create_schema(self):
        """Create database tables for file metadata and RAG functionality"""
        create_table_sql = """
        -- File metadata table (existing)
        CREATE TABLE IF NOT EXISTS file_metadata (
            id SERIAL PRIMARY KEY,
            file_path TEXT NOT NULL UNIQUE,
            file_name TEXT NOT NULL,
            file_extension TEXT,
            parent_directory TEXT,
            relative_path TEXT,
            is_directory BOOLEAN NOT NULL DEFAULT FALSE,
            file_size BIGINT,
            file_hash TEXT,
            mime_type TEXT,
            created_at TIMESTAMP,
            modified_at TIMESTAMP,
            accessed_at TIMESTAMP,
            depth_level INTEGER,
            permissions TEXT,
            owner_user TEXT,
            owner_group TEXT,
            scan_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- NEW: Document content table for storing extracted text
        CREATE TABLE IF NOT EXISTS document_content (
            id SERIAL PRIMARY KEY,
            file_metadata_id INTEGER REFERENCES file_metadata(id) ON DELETE CASCADE,
            content_text TEXT NOT NULL,
            content_type TEXT DEFAULT 'markdown',
            extraction_method TEXT DEFAULT 'markitdown',
            extraction_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            content_length INTEGER,
            language TEXT,
            encoding TEXT DEFAULT 'utf-8',
            title TEXT,
            document_type TEXT
        );

        -- NEW: Content chunks table for RAG
        CREATE TABLE IF NOT EXISTS content_chunks (
            id SERIAL PRIMARY KEY,
            document_content_id INTEGER REFERENCES document_content(id) ON DELETE CASCADE,
            file_metadata_id INTEGER REFERENCES file_metadata(id) ON DELETE CASCADE,
            chunk_index INTEGER NOT NULL,
            chunk_text TEXT NOT NULL,
            chunk_size INTEGER,
            chunk_method TEXT DEFAULT 'structure_based',
            chunk_type TEXT DEFAULT 'content',
            chunk_overlap INTEGER DEFAULT 0,
            start_position INTEGER,
            end_position INTEGER,
            file_directory TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Existing tables
        CREATE TABLE IF NOT EXISTS scan_sessions (
            id SERIAL PRIMARY KEY,
            session_id TEXT UNIQUE NOT NULL,
            start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            end_time TIMESTAMP,
            total_files INTEGER,
            total_directories INTEGER,
            total_size BIGINT,
            status TEXT DEFAULT 'running',
            errors_count INTEGER DEFAULT 0,
            content_files_processed INTEGER DEFAULT 0,
            chunks_created INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS file_duplicates (
            id SERIAL PRIMARY KEY,
            file_hash TEXT NOT NULL,
            file_paths TEXT[] NOT NULL,
            file_size BIGINT,
            duplicate_count INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Indexes for performance
        CREATE INDEX IF NOT EXISTS idx_file_metadata_path ON file_metadata(file_path);
        CREATE INDEX IF NOT EXISTS idx_file_metadata_parent ON file_metadata(parent_directory);
        CREATE INDEX IF NOT EXISTS idx_file_metadata_extension ON file_metadata(file_extension);
        CREATE INDEX IF NOT EXISTS idx_file_metadata_directory ON file_metadata(is_directory);
        CREATE INDEX IF NOT EXISTS idx_file_metadata_scan_time ON file_metadata(scan_timestamp);

        CREATE INDEX IF NOT EXISTS idx_document_content_file_id ON document_content(file_metadata_id);
        CREATE INDEX IF NOT EXISTS idx_document_content_type ON document_content(content_type);
        CREATE INDEX IF NOT EXISTS idx_document_content_doc_type ON document_content(document_type);

        CREATE INDEX IF NOT EXISTS idx_chunks_document_id ON content_chunks(document_content_id);
        CREATE INDEX IF NOT EXISTS idx_chunks_file_id ON content_chunks(file_metadata_id);
        CREATE INDEX IF NOT EXISTS idx_chunks_index ON content_chunks(chunk_index);
        CREATE INDEX IF NOT EXISTS idx_chunks_type ON content_chunks(chunk_type);
        CREATE INDEX IF NOT EXISTS idx_chunks_method ON content_chunks(chunk_method);
        CREATE INDEX IF NOT EXISTS idx_chunks_file_directory ON content_chunks(file_directory);
        """

        try:
            self.cursor.execute(create_table_sql)
            self.connection.commit()
            logger.info("Database schema created successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to create database schema: {e}")
            self.connection.rollback()
            return False

    def insert_file_metadata(self, metadata_list: List[Dict[str, Any]]):
        """Batch insert file metadata records"""
        if not metadata_list:
            return True

        insert_sql = """
        INSERT INTO file_metadata (
            file_path, file_name, file_extension, parent_directory, relative_path,
            is_directory, file_size, file_hash, mime_type, created_at, modified_at,
            accessed_at, depth_level, permissions, owner_user, owner_group
        ) VALUES (
            %(file_path)s, %(file_name)s, %(file_extension)s, %(parent_directory)s,
            %(relative_path)s, %(is_directory)s, %(file_size)s, %(file_hash)s,
            %(mime_type)s, %(created_at)s, %(modified_at)s, %(accessed_at)s,
            %(depth_level)s, %(permissions)s, %(owner_user)s, %(owner_group)s
        ) ON CONFLICT (file_path) DO UPDATE SET
            file_size = EXCLUDED.file_size,
            file_hash = EXCLUDED.file_hash,
            modified_at = EXCLUDED.modified_at,
            accessed_at = EXCLUDED.accessed_at,
            scan_timestamp = CURRENT_TIMESTAMP
        """

        try:
            self.cursor.executemany(insert_sql, metadata_list)
            self.connection.commit()
            logger.info(f"Successfully inserted/updated {len(metadata_list)} records")
            return True
        except Exception as e:
            logger.error(f"Failed to insert metadata records: {e}")
            self.connection.rollback()
            return False

    def start_scan_session(self, session_id: str):
        """Start a new scan session"""
        sql = """
        INSERT INTO scan_sessions (session_id) VALUES (%s)
        ON CONFLICT (session_id) DO UPDATE SET
            start_time = CURRENT_TIMESTAMP,
            status = 'running'
        """
        try:
            self.cursor.execute(sql, (session_id,))
            self.connection.commit()
            logger.info(f"Started scan session: {session_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to start scan session: {e}")
            return False

    def end_scan_session(self, session_id: str, stats: Dict[str, Any]):
        """End a scan session with statistics"""
        sql = """
        UPDATE scan_sessions SET
            end_time = CURRENT_TIMESTAMP,
            total_files = %s,
            total_directories = %s,
            total_size = %s,
            status = %s,
            errors_count = %s
        WHERE session_id = %s
        """
        try:
            self.cursor.execute(sql, (
                stats.get('total_files', 0),
                stats.get('total_directories', 0),
                stats.get('total_size', 0),
                stats.get('status', 'completed'),
                stats.get('errors_count', 0),
                session_id
            ))
            self.connection.commit()
            logger.info(f"Ended scan session: {session_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to end scan session: {e}")
            return False

    def get_file_count(self):
        """Get total number of files in database"""
        try:
            self.cursor.execute("SELECT COUNT(*) FROM file_metadata WHERE is_directory = FALSE")
            return self.cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"Failed to get file count: {e}")
            return 0

    def get_directory_count(self):
        """Get total number of directories in database"""
        try:
            self.cursor.execute("SELECT COUNT(*) FROM file_metadata WHERE is_directory = TRUE")
            return self.cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"Failed to get directory count: {e}")
            return 0

    def insert_document_content(self, file_metadata_id: int, content_data: Dict[str, Any]) -> Optional[int]:
        """Insert document content and return the content ID"""
        insert_sql = """
        INSERT INTO document_content (
            file_metadata_id, content_text, content_type, extraction_method,
            content_length, language, encoding, title, document_type
        ) VALUES (
            %(file_metadata_id)s, %(content_text)s, %(content_type)s, %(extraction_method)s,
            %(content_length)s, %(language)s, %(encoding)s, %(title)s, %(document_type)s
        ) RETURNING id
        """

        try:
            # Prepare data for insertion
            insert_data = {
                'file_metadata_id': file_metadata_id,
                'content_text': content_data.get('content_text', ''),
                'content_type': content_data.get('content_type', 'markdown'),
                'extraction_method': content_data.get('extraction_method', 'markitdown'),
                'content_length': content_data.get('content_length', 0),
                'language': content_data.get('language', 'unknown'),
                'encoding': content_data.get('encoding', 'utf-8'),
                'title': content_data.get('title'),
                'document_type': content_data.get('document_type')
            }

            self.cursor.execute(insert_sql, insert_data)
            result = self.cursor.fetchone()
            content_id = result[0] if result else None

            self.connection.commit()
            logger.debug(f"Inserted document content with ID: {content_id}")
            return content_id

        except Exception as e:
            logger.error(f"Failed to insert document content: {e}")
            self.connection.rollback()
            return None

    def insert_content_chunks(self, chunks: List[Dict[str, Any]]) -> bool:
        """Batch insert content chunks"""
        if not chunks:
            return True

        insert_sql = """
        INSERT INTO content_chunks (
            document_content_id, file_metadata_id, chunk_index, chunk_text,
            chunk_size, chunk_method, chunk_type, chunk_overlap,
            start_position, end_position, file_directory
        ) VALUES (
            %(document_content_id)s, %(file_metadata_id)s, %(chunk_index)s, %(chunk_text)s,
            %(chunk_size)s, %(chunk_method)s, %(chunk_type)s, %(chunk_overlap)s,
            %(start_position)s, %(end_position)s, %(file_directory)s
        );
        """

        try:
            # Prepare chunk data for batch insertion
            chunk_data = []
            for chunk in chunks:
                chunk_record = {
                    'document_content_id': chunk.get('document_content_id'),
                    'file_metadata_id': chunk.get('file_metadata_id'),
                    'chunk_index': chunk.get('chunk_index'),
                    'chunk_text': chunk.get('chunk_text'),
                    'chunk_size': chunk.get('chunk_size'),
                    'chunk_method': chunk.get('chunk_method'),
                    'chunk_type': chunk.get('chunk_type'),
                    'chunk_overlap': chunk.get('chunk_overlap'),
                    'start_position': chunk.get('start_position'),
                    'end_position': chunk.get('end_position'),
                    'file_directory': chunk.get('file_directory')
                }
                chunk_data.append(chunk_record)

            self.cursor.executemany(insert_sql, chunk_data)
            self.connection.commit()
            logger.info(f"Successfully inserted {len(chunk_data)} content chunks")
            return True
        except Exception as e:
            logger.error(f"Failed to insert content chunks: {e}")
            self.connection.rollback()
            return False

    def get_file_metadata_id(self, file_path: str) -> Optional[int]:
        """Get file metadata ID by file path"""
        try:
            self.cursor.execute("SELECT id FROM file_metadata WHERE file_path = %s", (file_path,))
            result = self.cursor.fetchone()
            return result[0] if result else None
        except Exception as e:
            logger.error(f"Failed to get file metadata ID for {file_path}: {e}")
            return None

    def get_content_stats(self) -> Dict[str, int]:
        """Get content processing statistics"""
        try:
            stats = {}

            # Document content stats
            self.cursor.execute("SELECT COUNT(*) FROM document_content")
            stats['total_documents_with_content'] = self.cursor.fetchone()[0]

            # Chunks stats
            self.cursor.execute("SELECT COUNT(*) FROM content_chunks")
            stats['total_chunks'] = self.cursor.fetchone()[0]

            # Average chunks per document
            self.cursor.execute("""
                SELECT AVG(chunk_count)::integer
                FROM (
                    SELECT COUNT(*) as chunk_count
                    FROM content_chunks
                    GROUP BY document_content_id
                ) sub
            """)
            result = self.cursor.fetchone()
            stats['avg_chunks_per_document'] = result[0] if result and result[0] else 0

            # Document types distribution
            self.cursor.execute("""
                SELECT document_type, COUNT(*)
                FROM document_content
                WHERE document_type IS NOT NULL
                GROUP BY document_type
            """)
            stats['document_types'] = dict(self.cursor.fetchall())

            return stats

        except Exception as e:
            logger.error(f"Failed to get content stats: {e}")
            return {}

    def update_scan_session_content_stats(self, session_id: str, content_stats: Dict[str, int]):
        """Update scan session with content processing statistics"""
        sql = """
        UPDATE scan_sessions SET
            content_files_processed = %s,
            chunks_created = %s
        WHERE session_id = %s
        """
        try:
            self.cursor.execute(sql, (
                content_stats.get('content_files_processed', 0),
                content_stats.get('chunks_created', 0),
                session_id
            ))
            self.connection.commit()
            logger.debug(f"Updated scan session {session_id} with content stats")
            return True
        except Exception as e:
            logger.error(f"Failed to update scan session content stats: {e}")
            return False