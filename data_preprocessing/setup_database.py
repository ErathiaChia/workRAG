#!/usr/bin/env python3
"""
Database Setup Script

This script helps set up the PostgreSQL database for file metadata storage.
Run this before running the main preprocessing script.
"""

import sys
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from config import Config
from database import DatabaseManager
import logging

def setup_logging():
    """Configure logging"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

def create_database():
    """Create the database if it doesn't exist"""
    logger = logging.getLogger(__name__)

    try:
        # Connect to PostgreSQL server (to the 'postgres' database)
        connection = psycopg2.connect(
            host=Config.DB_HOST,
            port=Config.DB_PORT,
            database='postgres',  # Connect to default database
            user=Config.DB_USER,
            password=Config.DB_PASSWORD
        )
        connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = connection.cursor()

        # Check if database exists
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{Config.DB_NAME}'")
        if cursor.fetchone():
            logger.info(f"Database '{Config.DB_NAME}' already exists")
        else:
            # Create database
            cursor.execute(f"CREATE DATABASE {Config.DB_NAME}")
            logger.info(f"Created database '{Config.DB_NAME}'")

        cursor.close()
        connection.close()
        return True

    except Exception as e:
        logger.error(f"Failed to create database: {e}")
        return False

def setup_schema():
    """Set up the database schema"""
    logger = logging.getLogger(__name__)

    db_manager = DatabaseManager()
    if not db_manager.connect():
        logger.error("Failed to connect to database")
        return False

    if db_manager.create_schema():
        logger.info("Database schema created successfully")
        db_manager.disconnect()
        return True
    else:
        logger.error("Failed to create database schema")
        db_manager.disconnect()
        return False

def test_connection():
    """Test database connection"""
    logger = logging.getLogger(__name__)

    db_manager = DatabaseManager()
    if db_manager.connect():
        logger.info("Database connection test successful")

        # Test basic query
        try:
            db_manager.cursor.execute("SELECT version()")
            version = db_manager.cursor.fetchone()[0]
            logger.info(f"PostgreSQL version: {version}")
        except Exception as e:
            logger.error(f"Failed to query database: {e}")
            return False

        db_manager.disconnect()
        return True
    else:
        logger.error("Database connection test failed")
        return False

def main():
    """Main setup function"""
    setup_logging()
    logger = logging.getLogger(__name__)

    logger.info("="*50)
    logger.info("Database Setup for File Metadata System")
    logger.info("="*50)

    logger.info(f"Host: {Config.DB_HOST}")
    logger.info(f"Port: {Config.DB_PORT}")
    logger.info(f"Database: {Config.DB_NAME}")
    logger.info(f"User: {Config.DB_USER}")

    # Step 1: Create database
    logger.info("\nStep 1: Creating database...")
    if not create_database():
        logger.error("Database creation failed. Exiting.")
        return 1

    # Step 2: Create schema
    logger.info("\nStep 2: Creating schema...")
    if not setup_schema():
        logger.error("Schema creation failed. Exiting.")
        return 1

    # Step 3: Test connection
    logger.info("\nStep 3: Testing connection...")
    if not test_connection():
        logger.error("Connection test failed. Exiting.")
        return 1

    logger.info("\n" + "="*50)
    logger.info("Database setup completed successfully!")
    logger.info("You can now run the preprocessing script.")
    logger.info("="*50)

    return 0

if __name__ == "__main__":
    sys.exit(main())