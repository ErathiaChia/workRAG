import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class Config:
    """Configuration class for file metadata extraction system"""

    # Database configuration
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_PORT = os.getenv('DB_PORT', '5432')
    DB_NAME = os.getenv('DB_NAME', 'file_metadata')
    DB_USER = os.getenv('DB_USER', 'postgres')
    DB_PASSWORD = os.getenv('DB_PASSWORD', '')

    # Target directory for scanning
    TARGET_DIRECTORY = "/Volumes/homes/Erathia/Career"

    # Processing settings
    BATCH_SIZE = 1000  # Number of records to insert at once
    MAX_FILE_SIZE_FOR_HASH = 100 * 1024 * 1024  # 100MB limit for hashing
    EXCLUDED_EXTENSIONS = {'.tmp', '.log', '.cache'}
    EXCLUDED_DIRECTORIES = {'__pycache__', '.git', '.DS_Store', 'node_modules'}

    # Logging
    LOG_LEVEL = 'INFO'
    LOG_FILE = 'file_metadata_extraction.log'

    @classmethod
    def get_db_connection_string(cls):
        """Return PostgreSQL connection string"""
        return f"postgresql://{cls.DB_USER}:{cls.DB_PASSWORD}@{cls.DB_HOST}:{cls.DB_PORT}/{cls.DB_NAME}"