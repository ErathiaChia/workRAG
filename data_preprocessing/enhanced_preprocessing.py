#!/usr/bin/env python3
"""
Enhanced File Metadata Extraction and RAG Processing System

This script extends the basic file metadata extraction to include:
1. Content extraction using Microsoft's MarkItDown
2. Document structure-based chunking
3. Storage of content and chunks in PostgreSQL
4. Optimized for career documents (resumes, job descriptions, etc.)
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
from content_extractor import ContentExtractor
from document_chunker import DocumentStructureChunker, DocumentChunk

logger = logging.getLogger(__name__)

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

def convert_chunks_to_db_format(chunks: List[DocumentChunk],
                               document_content_id: int,
                               file_metadata_id: int) -> List[Dict[str, Any]]:
    """Convert DocumentChunk objects to database-compatible format"""
    chunk_records = []

    for chunk in chunks:
        chunk_record = {
            'document_content_id': document_content_id,
            'file_metadata_id': file_metadata_id,
            'chunk_index': chunk.chunk_index,
            'chunk_text': chunk.chunk_text,
            'chunk_size': chunk.chunk_size,
            'chunk_method': chunk.chunk_method,
            'chunk_type': chunk.chunk_type,
            'chunk_overlap': chunk.overlap_with_previous,
            'start_position': chunk.start_position,
            'end_position': chunk.end_position,
            'metadata': chunk.metadata or {}
        }
        chunk_records.append(chunk_record)

    return chunk_records

def process_file_for_rag(file_metadata: Dict[str, Any],
                        content_extractor: ContentExtractor,
                        chunker: DocumentStructureChunker,
                        db_manager: DatabaseManager) -> Dict[str, Any]:
    """Process a single file for RAG: extract content, chunk, and store"""
    processing_stats = {
        'processed': False,
        'content_extracted': False,
        'chunks_created': 0,
        'error': None
    }

    try:
        file_path = file_metadata.get('file_path')
        if not file_path:
            processing_stats['error'] = "No file path in metadata"
            return processing_stats

        # Get file metadata ID from database
        file_metadata_id = db_manager.get_file_metadata_id(file_path)
        if not file_metadata_id:
            processing_stats['error'] = f"File metadata not found in database: {file_path}"
            return processing_stats

        # Check if we should extract content from this file
        if not content_extractor.should_extract_content(file_metadata):
            processing_stats['processed'] = True
            return processing_stats

        # Extract content using MarkItDown
        content_data = content_extractor.extract_content(file_path)
        if not content_data:
            processing_stats['error'] = f"Failed to extract content from: {file_path}"
            return processing_stats

        processing_stats['content_extracted'] = True

        # Detect document type for better chunking
        doc_type = chunker._detect_document_type(content_data['content_text'], file_path)
        content_data['document_type'] = doc_type

        # Insert document content into database
        document_content_id = db_manager.insert_document_content(file_metadata_id, content_data)
        if not document_content_id:
            processing_stats['error'] = f"Failed to store content in database: {file_path}"
            return processing_stats

        # Chunk the document
        chunks = chunker.chunk_document(content_data['content_text'], file_path)
        if not chunks:
            processing_stats['error'] = f"No chunks created for: {file_path}"
            return processing_stats

        # Convert chunks to database format
        chunk_records = convert_chunks_to_db_format(chunks, document_content_id, file_metadata_id)

        # Insert chunks into database
        if db_manager.insert_content_chunks(chunk_records):
            processing_stats['chunks_created'] = len(chunks)
            processing_stats['processed'] = True
            logger.info(f"Successfully processed {file_path}: {len(chunks)} chunks created")
        else:
            processing_stats['error'] = f"Failed to store chunks in database: {file_path}"

        return processing_stats

    except Exception as e:
        processing_stats['error'] = f"Unexpected error processing {file_path}: {e}"
        logger.error(f"Error processing file {file_path}: {e}", exc_info=True)
        return processing_stats

def process_metadata_batch(db_manager: DatabaseManager,
                          metadata_batch: List[Dict[str, Any]],
                          content_extractor: ContentExtractor,
                          chunker: DocumentStructureChunker,
                          enable_content_processing: bool = True) -> Dict[str, Any]:
    """Process a batch of metadata records with optional content processing"""
    batch_stats = {
        'metadata_processed': 0,
        'content_files_processed': 0,
        'total_chunks_created': 0,
        'errors': 0
    }

    if not metadata_batch:
        return batch_stats

    try:
        # Insert file metadata first
        success = db_manager.insert_file_metadata(metadata_batch)
        if success:
            batch_stats['metadata_processed'] = len(metadata_batch)
            logger.info(f"Successfully processed metadata batch of {len(metadata_batch)} records")
        else:
            logger.error("Failed to process metadata batch")
            batch_stats['errors'] += len(metadata_batch)
            return batch_stats

        # Process content if enabled
        if enable_content_processing:
            for file_metadata in metadata_batch:
                processing_result = process_file_for_rag(
                    file_metadata, content_extractor, chunker, db_manager
                )

                if processing_result['processed']:
                    if processing_result['content_extracted']:
                        batch_stats['content_files_processed'] += 1
                        batch_stats['total_chunks_created'] += processing_result['chunks_created']
                elif processing_result['error']:
                    batch_stats['errors'] += 1
                    logger.debug(f"Content processing error: {processing_result['error']}")

        return batch_stats

    except Exception as e:
        logger.error(f"Failed to process metadata batch: {e}")
        batch_stats['errors'] += len(metadata_batch)
        return batch_stats

def main():
    """Main processing function"""
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Enhanced file metadata extraction with RAG processing')
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
    parser.add_argument('--skip-content',
                       action='store_true',
                       help='Skip content extraction and chunking')
    parser.add_argument('--create-schema',
                       action='store_true',
                       help='Create database schema before processing')
    parser.add_argument('--chunk-size',
                       type=int,
                       default=1000,
                       help='Target chunk size in characters (default: 1000)')
    parser.add_argument('--max-chunk-size',
                       type=int,
                       default=2000,
                       help='Maximum chunk size in characters (default: 2000)')

    args = parser.parse_args()

    # Setup logging
    setup_logging()
    logger = logging.getLogger(__name__)

    logger.info("="*60)
    logger.info("Enhanced File Metadata Extraction and RAG Processing")
    logger.info("="*60)
    logger.info(f"Target directory: {args.directory}")
    logger.info(f"Batch size: {args.batch_size}")
    logger.info(f"Dry run mode: {args.dry_run}")
    logger.info(f"Skip hash calculation: {args.skip_hash}")
    logger.info(f"Skip content processing: {args.skip_content}")
    logger.info(f"Chunk size: {args.chunk_size}")

    # Initialize components
    scanner = FileScanner()
    content_extractor = ContentExtractor() if not args.skip_content else None
    chunker = DocumentStructureChunker(
        target_chunk_size=args.chunk_size,
        max_chunk_size=args.max_chunk_size
    ) if not args.skip_content else None

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
        session_id = f"enhanced_scan_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"

        if not args.dry_run:
            # Start scan session
            db_manager.start_scan_session(session_id)

        # Initialize processing variables
        metadata_batch = []
        total_processed = 0
        total_content_stats = {
            'content_files_processed': 0,
            'total_chunks_created': 0,
            'content_errors': 0
        }

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
                            batch_stats = process_metadata_batch(
                                db_manager, metadata_batch, content_extractor, chunker,
                                enable_content_processing=not args.skip_content
                            )

                            total_processed += batch_stats['metadata_processed']
                            total_content_stats['content_files_processed'] += batch_stats['content_files_processed']
                            total_content_stats['total_chunks_created'] += batch_stats['total_chunks_created']
                            total_content_stats['content_errors'] += batch_stats['errors']

                            pbar.update(batch_stats['metadata_processed'])
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
                batch_stats = process_metadata_batch(
                    db_manager, metadata_batch, content_extractor, chunker,
                    enable_content_processing=not args.skip_content
                )
                total_processed += batch_stats['metadata_processed']
                total_content_stats['content_files_processed'] += batch_stats['content_files_processed']
                total_content_stats['total_chunks_created'] += batch_stats['total_chunks_created']

        finally:
            # Process any remaining metadata
            if not args.dry_run and metadata_batch:
                logger.info("Processing final batch...")
                batch_stats = process_metadata_batch(
                    db_manager, metadata_batch, content_extractor, chunker,
                    enable_content_processing=not args.skip_content
                )
                total_processed += batch_stats['metadata_processed']
                total_content_stats['content_files_processed'] += batch_stats['content_files_processed']
                total_content_stats['total_chunks_created'] += batch_stats['total_chunks_created']
                pbar.update(batch_stats['metadata_processed'])

            pbar.close()

        # Get final statistics
        stats = scanner.get_scan_stats()
        stats['status'] = 'completed'

        logger.info("="*60)
        logger.info("Processing Complete - Final Statistics:")
        logger.info("="*60)
        logger.info(f"Total files scanned: {stats['total_files']}")
        logger.info(f"Total directories scanned: {stats['total_directories']}")
        logger.info(f"Total size processed: {stats['total_size']:,} bytes ({stats['total_size']/(1024**3):.2f} GB)")
        logger.info(f"File metadata records written: {total_processed}")

        if not args.skip_content:
            logger.info(f"Files with content extracted: {total_content_stats['content_files_processed']}")
            logger.info(f"Total chunks created: {total_content_stats['total_chunks_created']}")
            logger.info(f"Content processing errors: {total_content_stats['content_errors']}")

            if content_extractor:
                extraction_stats = content_extractor.get_extraction_stats()
                logger.info(f"Content extraction success rate: {extraction_stats.get('success_rate', 0):.2%}")

            if chunker:
                chunking_stats = chunker.get_chunking_stats()
                logger.info(f"Average chunk size: {chunking_stats.get('avg_chunk_size', 0):.0f} characters")

        logger.info(f"Total errors encountered: {stats['errors_count']}")

        if not args.dry_run:
            # Update scan session with content stats
            if not args.skip_content:
                db_manager.update_scan_session_content_stats(session_id, total_content_stats)

            # End scan session
            db_manager.end_scan_session(session_id, stats)

            # Display database statistics
            file_count = db_manager.get_file_count()
            dir_count = db_manager.get_directory_count()
            logger.info(f"Database now contains: {file_count} files, {dir_count} directories")

            if not args.skip_content:
                content_stats = db_manager.get_content_stats()
                logger.info(f"Content database: {content_stats.get('total_documents_with_content', 0)} documents, "
                           f"{content_stats.get('total_chunks', 0)} chunks")

        logger.info("Enhanced file processing completed successfully!")
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