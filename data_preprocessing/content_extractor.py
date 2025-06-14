#!/usr/bin/env python3
"""
Content Extraction Module using MarkItDown

This module handles the extraction of text content from various file formats
using Microsoft's MarkItDown library, which converts documents to structured Markdown.
"""

import os
import tempfile
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List
from markitdown import MarkItDown
from config import Config

logger = logging.getLogger(__name__)

class ContentExtractor:
    """Handles content extraction from various file formats using MarkItDown"""

    # Supported file extensions for content extraction
    SUPPORTED_EXTENSIONS = {
        # Office Documents
        '.pdf', '.docx', '.doc', '.pptx', '.ppt', '.xlsx', '.xls',
        # Text formats
        '.txt', '.md', '.csv', '.json', '.xml', '.html', '.htm',
        # Email
        '.msg', '.eml',
        # Images (with OCR)
        '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff',
        # Audio (with transcription)
        '.wav', '.mp3', '.m4a',
        # Archives
        '.zip', '.epub'
    }

    def __init__(self, enable_plugins: bool = False):
        """Initialize the content extractor"""
        self.markitdown = MarkItDown(enable_plugins=enable_plugins)
        self.stats = {
            'files_processed': 0,
            'successful_extractions': 0,
            'failed_extractions': 0,
            'total_content_length': 0
        }

    def is_supported_file(self, file_path: str) -> bool:
        """Check if file type is supported for content extraction"""
        file_extension = Path(file_path).suffix.lower()
        return file_extension in self.SUPPORTED_EXTENSIONS

    def should_extract_content(self, file_metadata: Dict[str, Any]) -> bool:
        """Determine if content should be extracted from this file"""
        # Skip directories
        if file_metadata.get('is_directory', False):
            return False

        # Check if file type is supported
        file_path = file_metadata.get('file_path', '')
        if not self.is_supported_file(file_path):
            logger.debug(f"Unsupported file type for content extraction: {file_path}")
            return False

        # Skip very large files (configurable limit)
        file_size = file_metadata.get('file_size', 0)
        max_size = getattr(Config, 'MAX_FILE_SIZE_FOR_CONTENT_EXTRACTION', 50 * 1024 * 1024)  # 50MB default
        if file_size and file_size > max_size:
            logger.debug(f"File too large for content extraction: {file_path} ({file_size} bytes)")
            return False

        # Skip excluded file types
        file_extension = Path(file_path).suffix.lower()
        excluded_extensions = getattr(Config, 'EXCLUDED_CONTENT_EXTENSIONS', {'.tmp', '.log', '.cache'})
        if file_extension in excluded_extensions:
            return False

        return True

    def extract_content(self, file_path: str) -> Optional[Dict[str, Any]]:
        """Extract content from a file using MarkItDown"""
        try:
            if not os.path.exists(file_path):
                logger.error(f"File not found: {file_path}")
                return None

            if not self.is_supported_file(file_path):
                logger.debug(f"File type not supported: {file_path}")
                return None

            # Use MarkItDown to convert file to Markdown
            logger.debug(f"Extracting content from: {file_path}")
            result = self.markitdown.convert(file_path)

            if not result or not result.text_content:
                logger.warning(f"No content extracted from: {file_path}")
                return None

            # Prepare content metadata
            content_data = {
                'content_text': result.text_content,
                'content_type': 'markdown',
                'extraction_method': 'markitdown',
                'content_length': len(result.text_content),
                'language': self._detect_language(result.text_content),
                'encoding': 'utf-8'
            }

            # Add any additional metadata from MarkItDown result
            if hasattr(result, 'title') and result.title:
                content_data['title'] = result.title

            self.stats['files_processed'] += 1
            self.stats['successful_extractions'] += 1
            self.stats['total_content_length'] += len(result.text_content)

            logger.info(f"Successfully extracted {len(result.text_content)} characters from: {file_path}")
            return content_data

        except Exception as e:
            logger.error(f"Failed to extract content from {file_path}: {e}")
            self.stats['files_processed'] += 1
            self.stats['failed_extractions'] += 1
            return None

    def _detect_language(self, text: str) -> str:
        """Simple language detection (can be enhanced with proper language detection library)"""
        # Simple heuristic - can be replaced with proper language detection
        if not text:
            return 'unknown'

        # Check for common English words
        english_words = {'the', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by', 'a', 'an'}
        words = text.lower().split()[:100]  # Check first 100 words
        english_count = sum(1 for word in words if word in english_words)

        if english_count > len(words) * 0.1:  # More than 10% English words
            return 'en'
        else:
            return 'unknown'

    def extract_content_batch(self, file_metadata_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Extract content from multiple files"""
        extracted_content = []

        for file_metadata in file_metadata_list:
            if self.should_extract_content(file_metadata):
                file_path = file_metadata.get('file_path')
                content_data = self.extract_content(file_path)

                if content_data:
                    # Add reference to file metadata
                    content_data['file_metadata'] = file_metadata
                    extracted_content.append(content_data)

        return extracted_content

    def get_extraction_stats(self) -> Dict[str, Any]:
        """Return extraction statistics"""
        stats = self.stats.copy()
        if stats['files_processed'] > 0:
            stats['success_rate'] = stats['successful_extractions'] / stats['files_processed']
            stats['average_content_length'] = (
                stats['total_content_length'] / stats['successful_extractions']
                if stats['successful_extractions'] > 0 else 0
            )
        else:
            stats['success_rate'] = 0
            stats['average_content_length'] = 0

        return stats

    def reset_stats(self):
        """Reset extraction statistics"""
        self.stats = {
            'files_processed': 0,
            'successful_extractions': 0,
            'failed_extractions': 0,
            'total_content_length': 0
        }

class MarkdownContentProcessor:
    """Additional processing for markdown content"""

    @staticmethod
    def clean_markdown(markdown_text: str) -> str:
        """Clean and normalize markdown text"""
        if not markdown_text:
            return ""

        lines = markdown_text.split('\n')
        cleaned_lines = []

        for line in lines:
            # Remove excessive whitespace
            line = line.strip()

            # Skip empty lines that are too frequent
            if not line and cleaned_lines and not cleaned_lines[-1]:
                continue

            cleaned_lines.append(line)

        return '\n'.join(cleaned_lines)

    @staticmethod
    def extract_metadata_from_markdown(markdown_text: str) -> Dict[str, Any]:
        """Extract metadata from markdown content (headers, lists, etc.)"""
        metadata = {
            'headers': [],
            'lists': [],
            'tables': [],
            'links': [],
            'structure_elements': []
        }

        lines = markdown_text.split('\n')

        for line_num, line in enumerate(lines):
            line = line.strip()

            # Headers
            if line.startswith('#'):
                level = len(line) - len(line.lstrip('#'))
                header_text = line.lstrip('#').strip()
                metadata['headers'].append({
                    'level': level,
                    'text': header_text,
                    'line_number': line_num
                })
                metadata['structure_elements'].append({
                    'type': 'header',
                    'level': level,
                    'line_number': line_num,
                    'content': header_text
                })

            # Lists
            elif line.startswith(('- ', '* ', '+ ')) or (line and line[0].isdigit() and '. ' in line):
                list_type = 'ordered' if line[0].isdigit() else 'unordered'
                list_content = line.lstrip('- * + ').split('. ', 1)[-1]
                metadata['lists'].append({
                    'type': list_type,
                    'content': list_content,
                    'line_number': line_num
                })
                metadata['structure_elements'].append({
                    'type': 'list_item',
                    'list_type': list_type,
                    'line_number': line_num,
                    'content': list_content
                })

            # Tables
            elif '|' in line and line.count('|') >= 2:
                metadata['tables'].append({
                    'line_number': line_num,
                    'content': line
                })
                metadata['structure_elements'].append({
                    'type': 'table_row',
                    'line_number': line_num,
                    'content': line
                })

        return metadata