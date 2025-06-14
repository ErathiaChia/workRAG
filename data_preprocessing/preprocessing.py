#!/usr/bin/env python3
"""
File Metadata Extraction and Storage System

This script recursively scans a directory structure, extracts comprehensive metadata
for all files and directories, and stores the information in a PostgreSQL database.
"""

import argparse
import logging
import sys
import uuid
from datetime import datetime
from typing import List, Dict, Any
from tqdm import tqdm

from config import Config
from database import DatabaseManager
from file_scanner import FileScanner

def setup_logging():
    """Configure logging for the application"""
    logging.basicConfig(
        level=getattr(logging, Config.LOG_LEVEL),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(Config.LOG_FILE),
            logging.StreamHandler(sys.stdout)
        ]
    )

def process_metadata_batch(db_manager: DatabaseManager, metadata_batch: List[Dict[str, Any]]) -> bool:
    """Process a batch of metadata records"""
    if not metadata_batch:
        return True

    try:
        success = db_manager.insert_file_metadata(metadata_batch)
        if success:
            logging.info(f"Successfully processed batch of {len(metadata_batch)} records")
        return success
    except Exception as e:
        logging.error(f"Failed to process metadata batch: {e}")
        return False

def main():
    """Main processing function"""
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Extract file metadata and store in PostgreSQL')
    parser.add_argument('--directory', '-d',
                       default=Config.TARGET_DIRECTORY,
                       help=f'Directory to scan (default: {Config.TARGET_DIRECTORY})')
    parser.add_argument('--batch-size', '-b',
                       type=int,
                       default=Config.BATCH_SIZE,
                       help=f'Batch size for database inserts (default: {Config.BATCH_SIZE})')
    parser.add_argument('--dry-run',
                       action='store_true',
                       help='Scan files but do not write to database')
    parser.add_argument('--skip-hash',
                       action='store_true',
                       help='Skip file hash calculation for faster processing')
    parser.add_argument('--create-schema',
                       action='store_true',
                       help='Create database schema before processing')

    args = parser.parse_args()

    # Setup logging
    setup_logging()
    logger = logging.getLogger(__name__)

    logger.info("="*50)
    logger.info("File Metadata Extraction Started")
    logger.info("="*50)
    logger.info(f"Target directory: {args.directory}")
    logger.info(f"Batch size: {args.batch_size}")
    logger.info(f"Dry run mode: {args.dry_run}")
    logger.info(f"Skip hash calculation: {args.skip_hash}")

    # Initialize components
    scanner = FileScanner()
    db_manager = None

    if not args.dry_run:
        db_manager = DatabaseManager()

        # Connect to database
        if not db_manager.connect():
            logger.error("Failed to connect to database. Exiting.")
            return 1

        # Create schema if requested
        if args.create_schema:
            logger.info("Creating database schema...")
            if not db_manager.create_schema():
                logger.error("Failed to create database schema. Exiting.")
                return 1

    try:
        # Generate session ID
        session_id = f"scan_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"

        if not args.dry_run:
            # Start scan session
            db_manager.start_scan_session(session_id)

        # Initialize processing variables
        metadata_batch = []
        total_processed = 0

        # Set up progress bar
        pbar = tqdm(desc="Processing files", unit="files")

        try:
            # Scan directory and process metadata
            for metadata in scanner.scan_directory(args.directory):
                if metadata:
                    # Skip hash calculation if requested
                    if args.skip_hash:
                        metadata['file_hash'] = None

                    if not args.dry_run:
                        metadata_batch.append(metadata)

                        # Process batch when it reaches the specified size
                        if len(metadata_batch) >= args.batch_size:
                            if process_metadata_batch(db_manager, metadata_batch):
                                total_processed += len(metadata_batch)
                                pbar.update(len(metadata_batch))
                            metadata_batch = []
                    else:
                        # In dry run mode, just log the metadata
                        logger.debug(f"Would process: {metadata['file_path']}")
                        total_processed += 1
                        pbar.update(1)

        except KeyboardInterrupt:
            logger.warning("Process interrupted by user")
            if not args.dry_run and metadata_batch:
                logger.info("Processing remaining batch before exit...")
                process_metadata_batch(db_manager, metadata_batch)

        finally:
            # Process any remaining metadata
            if not args.dry_run and metadata_batch:
                logger.info("Processing final batch...")
                if process_metadata_batch(db_manager, metadata_batch):
                    total_processed += len(metadata_batch)
                    pbar.update(len(metadata_batch))

            pbar.close()

        # Get final statistics
        stats = scanner.get_scan_stats()
        stats['status'] = 'completed'

        logger.info("="*50)
        logger.info("Scan Complete - Final Statistics:")
        logger.info("="*50)
        logger.info(f"Total files processed: {stats['total_files']}")
        logger.info(f"Total directories processed: {stats['total_directories']}")
        logger.info(f"Total size processed: {stats['total_size']:,} bytes ({stats['total_size']/(1024**3):.2f} GB)")
        logger.info(f"Total errors encountered: {stats['errors_count']}")
        logger.info(f"Records written to database: {total_processed}")

        if not args.dry_run:
            # End scan session
            db_manager.end_scan_session(session_id, stats)

            # Display database statistics
            file_count = db_manager.get_file_count()
            dir_count = db_manager.get_directory_count()
            logger.info(f"Database now contains: {file_count} files, {dir_count} directories")

        logger.info("File metadata extraction completed successfully!")
        return 0

    except Exception as e:
        logger.error(f"Unexpected error during processing: {e}", exc_info=True)
        return 1

    finally:
        # Cleanup
        if db_manager:
            db_manager.disconnect()

if __name__ == "__main__":
    sys.exit(main())
