#!/usr/bin/env python3
"""
Document Structure-Based Chunking Module

This module implements intelligent chunking strategies that preserve document structure,
particularly optimized for career-related documents like job responsibilities, meeting notes, presentation slides, tender, proposal documents.
"""

import re
import logging
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
from content_extractor import MarkdownContentProcessor

logger = logging.getLogger(__name__)

@dataclass
class DocumentChunk:
    """Represents a chunk of document content"""
    chunk_text: str
    chunk_index: int
    chunk_method: str
    start_position: int
    end_position: int
    chunk_size: int
    chunk_type: str = "content"  # 'content', 'header', 'section', 'list', 'table'
    overlap_with_previous: int = 0

    def __post_init__(self):
        if self.chunk_size == 0:
            self.chunk_size = len(self.chunk_text)

class DocumentStructureChunker:
    """Implements generic document structure-based chunking for a wide variety of work documents."""

    def __init__(self,
                 target_chunk_size: int = 1000,
                 max_chunk_size: int = 2000,
                 min_chunk_size: int = 100,
                 overlap_size: int = 150):
        """Initialize the document chunker"""
        self.target_chunk_size = target_chunk_size
        self.max_chunk_size = max_chunk_size
        self.min_chunk_size = min_chunk_size
        self.overlap_size = overlap_size

        self.stats = {
            'documents_processed': 0,
            'total_chunks_created': 0,
            'avg_chunk_size': 0,
            'chunk_types': {}
        }

    def chunk_document(self, content_text: str, file_path: str = "", parent_directory: str = "") -> List[DocumentChunk]:
        """Main chunking method that applies generic structure-based chunking"""
        try:
            if not content_text or not content_text.strip():
                logger.warning(f"Empty or whitespace-only content for {file_path}")
                logger.debug(f"[DEBUG] Content text is empty or whitespace for {file_path}")
                return []

            # Clean the markdown content
            cleaned_content = MarkdownContentProcessor.clean_markdown(content_text)

            # Extract document structure
            structure_metadata = MarkdownContentProcessor.extract_metadata_from_markdown(cleaned_content)

            # Always use generic structure-based chunking
            chunks = self._chunk_generic_document(cleaned_content, structure_metadata, file_path, parent_directory)

            # Post-process chunks
            chunks = self._post_process_chunks(chunks)

            # Update statistics
            self.stats['documents_processed'] += 1
            self.stats['total_chunks_created'] += len(chunks)

            chunk_type_counts = {}
            total_size = 0
            for chunk in chunks:
                chunk_type_counts[chunk.chunk_type] = chunk_type_counts.get(chunk.chunk_type, 0) + 1
                total_size += chunk.chunk_size

            self.stats['chunk_types'].update(chunk_type_counts)
            if len(chunks) > 0:
                self.stats['avg_chunk_size'] = total_size / len(chunks)

            logger.info(f"Created {len(chunks)} chunks for {file_path}")
            return chunks

        except Exception as e:
            logger.error(f"Failed to chunk document {file_path}: {e}")
            return []

    def _chunk_generic_document(self, content: str, structure_metadata: Dict, file_path: str, parent_directory: str) -> List[DocumentChunk]:
        """Generic structure-based chunking for all document types"""
        chunks = []
        running_pos = 0

        # First try to split by headers
        header_chunks = self._split_by_headers(content, structure_metadata)

        if len(header_chunks) >= 2:
            for i, chunk_text in enumerate(header_chunks):
                chunk_text_stripped = chunk_text.strip()
                if len(chunk_text_stripped) >= self.min_chunk_size:
                    start_pos = content.find(chunk_text_stripped, running_pos)
                    if start_pos == -1:
                        start_pos = running_pos
                    end_pos = start_pos + len(chunk_text_stripped)
                    chunk = self._create_chunk(
                        chunk_text_stripped, i, 'header_based', start_pos, end_pos, 'section'
                    )
                    chunks.append(chunk)
                    running_pos = end_pos
        else:
            # Fall back to content block splitting
            content_chunks = self._split_by_content_blocks(content)
            for i, chunk_text in enumerate(content_chunks):
                chunk_text_stripped = chunk_text.strip()
                if len(chunk_text_stripped) >= self.min_chunk_size:
                    start_pos = content.find(chunk_text_stripped, running_pos)
                    if start_pos == -1:
                        start_pos = running_pos
                    end_pos = start_pos + len(chunk_text_stripped)
                    chunk = self._create_chunk(
                        chunk_text_stripped, i, 'content_based', start_pos, end_pos, 'content'
                    )
                    chunks.append(chunk)
                    running_pos = end_pos

        return chunks

    def _split_by_headers(self, content: str, structure_metadata: Dict) -> List[str]:
        """Split content by markdown headers"""
        if not structure_metadata.get('headers'):
            return [content]

        chunks = []
        lines = content.split('\n')
        current_chunk = []

        for i, line in enumerate(lines):
            if line.strip().startswith('#'):
                # Start new chunk at header
                if current_chunk:
                    chunks.append('\n'.join(current_chunk))
                current_chunk = [line]
            else:
                current_chunk.append(line)

        # Add the last chunk
        if current_chunk:
            chunks.append('\n'.join(current_chunk))

        return chunks

    def _split_by_content_blocks(self, content: str) -> List[str]:
        """Split content by logical blocks (paragraphs, lists, etc.)"""
        # Split by double newlines first
        blocks = re.split(r'\n\s*\n', content.strip())

        chunks = []
        current_chunk = []
        current_size = 0

        for block in blocks:
            block = block.strip()
            if not block:
                continue

            block_size = len(block)

            # If adding this block would exceed max size, save current chunk
            if current_size + block_size > self.max_chunk_size and current_chunk:
                chunks.append('\n\n'.join(current_chunk))
                current_chunk = []
                current_size = 0

            current_chunk.append(block)
            current_size += block_size + 2  # Account for newlines

            # If current chunk is good size, save it
            if current_size >= self.target_chunk_size:
                chunks.append('\n\n'.join(current_chunk))
                current_chunk = []
                current_size = 0

        # Add remaining content
        if current_chunk:
            chunks.append('\n\n'.join(current_chunk))

        return chunks

    def _create_chunk(self, text: str, index: int, method: str,
                     start_pos: int, end_pos: int, chunk_type: str) -> DocumentChunk:
        """Create a DocumentChunk object"""
        return DocumentChunk(
            chunk_text=text.strip(),
            chunk_index=index,
            chunk_method=method,
            start_position=start_pos,
            end_position=end_pos,
            chunk_size=len(text.strip()),
            chunk_type=chunk_type
        )

    def _post_process_chunks(self, chunks: List[DocumentChunk]) -> List[DocumentChunk]:
        """Post-process chunks to handle edge cases"""
        if not chunks:
            return chunks

        processed_chunks = []

        for i, chunk in enumerate(chunks):
            # Skip chunks that are too small
            if chunk.chunk_size < self.min_chunk_size:
                # Try to merge with previous chunk
                if processed_chunks and processed_chunks[-1].chunk_size < self.target_chunk_size:
                    prev_chunk = processed_chunks[-1]
                    merged_text = prev_chunk.chunk_text + '\n\n' + chunk.chunk_text
                    if len(merged_text) <= self.max_chunk_size:
                        prev_chunk.chunk_text = merged_text
                        prev_chunk.chunk_size = len(merged_text)
                        prev_chunk.end_position = chunk.end_position
                        continue

            # Split chunks that are too large
            if chunk.chunk_size > self.max_chunk_size:
                sub_chunks = self._split_large_chunk(chunk, i)
                processed_chunks.extend(sub_chunks)
            else:
                processed_chunks.append(chunk)

        # Re-index chunks
        for i, chunk in enumerate(processed_chunks):
            chunk.chunk_index = i

        return processed_chunks

    def _split_large_chunk(self, chunk: DocumentChunk, base_index: int) -> List[DocumentChunk]:
        """Split a chunk that exceeds maximum size"""
        text = chunk.chunk_text
        sub_chunks = []

        # Split by sentences first
        sentences = re.split(r'(?<=[.!?])\s+', text)

        current_text = []
        current_size = 0
        sub_index = 0

        for sentence in sentences:
            sentence_size = len(sentence)

            if current_size + sentence_size > self.max_chunk_size and current_text:
                # Create sub-chunk
                sub_chunk_text = ' '.join(current_text)
                sub_chunk = DocumentChunk(
                    chunk_text=sub_chunk_text,
                    chunk_index=base_index + sub_index,
                    chunk_method=chunk.chunk_method + '_split',
                    start_position=chunk.start_position,
                    end_position=chunk.start_position + len(sub_chunk_text),
                    chunk_size=len(sub_chunk_text),
                    chunk_type=chunk.chunk_type
                )
                sub_chunks.append(sub_chunk)

                current_text = []
                current_size = 0
                sub_index += 1

            current_text.append(sentence)
            current_size += sentence_size + 1

        # Add remaining text
        if current_text:
            sub_chunk_text = ' '.join(current_text)
            sub_chunk = DocumentChunk(
                chunk_text=sub_chunk_text,
                chunk_index=base_index + sub_index,
                chunk_method=chunk.chunk_method + '_split',
                start_position=chunk.start_position,
                end_position=chunk.end_position,
                chunk_size=len(sub_chunk_text),
                chunk_type=chunk.chunk_type
            )
            sub_chunks.append(sub_chunk)

        return sub_chunks if sub_chunks else [chunk]

    def get_chunking_stats(self) -> Dict[str, Any]:
        """Get chunking statistics"""
        return self.stats.copy()

    def reset_stats(self):
        """Reset chunking statistics"""
        self.stats = {
            'documents_processed': 0,
            'total_chunks_created': 0,
            'avg_chunk_size': 0,
            'chunk_types': {}
        }