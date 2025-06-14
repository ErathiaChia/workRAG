#!/usr/bin/env python3
"""
Document Structure-Based Chunking Module

This module implements intelligent chunking strategies that preserve document structure,
particularly optimized for career-related documents like resumes, job descriptions, etc.
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
    metadata: Dict[str, Any] = None
    overlap_with_previous: int = 0

    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}
        if self.chunk_size == 0:
            self.chunk_size = len(self.chunk_text)

class DocumentStructureChunker:
    """Implements document structure-based chunking with career document optimization"""

    # Career-specific section patterns
    CAREER_SECTION_PATTERNS = {
        'contact': [
            r'^\s*(contact|personal)\s*(information|details|info)?\s*$',
            r'^\s*(phone|email|address|linkedin|portfolio)\s*$'
        ],
        'objective': [
            r'^\s*(career\s+)?(objective|summary|goal|profile)\s*$',
            r'^\s*professional\s+(summary|profile|overview)\s*$'
        ],
        'experience': [
            r'^\s*(work\s+)?(experience|employment|history)\s*$',
            r'^\s*professional\s+experience\s*$',
            r'^\s*employment\s+(history|record)\s*$'
        ],
        'education': [
            r'^\s*education(al\s+background)?\s*$',
            r'^\s*academic\s+(background|qualifications)\s*$'
        ],
        'skills': [
            r'^\s*(technical\s+)?skills\s*$',
            r'^\s*(core\s+)?(competencies|abilities)\s*$'
        ],
        'projects': [
            r'^\s*(key\s+)?projects?\s*$',
            r'^\s*notable\s+work\s*$'
        ],
        'achievements': [
            r'^\s*(achievements?|accomplishments?|awards?)\s*$',
            r'^\s*recognition\s*$'
        ],
        'certifications': [
            r'^\s*certifications?\s*$',
            r'^\s*licenses?\s*$'
        ]
    }

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

    def chunk_document(self, content_text: str, file_path: str = "") -> List[DocumentChunk]:
        """Main chunking method that applies structure-based chunking"""
        try:
            if not content_text or not content_text.strip():
                logger.warning(f"Empty or whitespace-only content for {file_path}")
                return []

            # Clean the markdown content
            cleaned_content = MarkdownContentProcessor.clean_markdown(content_text)

            # Extract document structure
            structure_metadata = MarkdownContentProcessor.extract_metadata_from_markdown(cleaned_content)

            # Determine document type based on content
            doc_type = self._detect_document_type(cleaned_content, file_path)

            # Apply appropriate chunking strategy
            if doc_type == 'resume':
                chunks = self._chunk_resume(cleaned_content, structure_metadata)
            elif doc_type == 'job_description':
                chunks = self._chunk_job_description(cleaned_content, structure_metadata)
            elif doc_type == 'cover_letter':
                chunks = self._chunk_cover_letter(cleaned_content, structure_metadata)
            else:
                # Fall back to generic structure-based chunking
                chunks = self._chunk_generic_document(cleaned_content, structure_metadata)

            # Post-process chunks
            chunks = self._post_process_chunks(chunks, doc_type)

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

            logger.info(f"Created {len(chunks)} chunks for {file_path} (type: {doc_type})")
            return chunks

        except Exception as e:
            logger.error(f"Failed to chunk document {file_path}: {e}")
            return []

    def _detect_document_type(self, content: str, file_path: str) -> str:
        """Detect the type of career document"""
        content_lower = content.lower()

        # Resume indicators
        resume_indicators = [
            'education', 'experience', 'skills', 'objective', 'summary',
            'employment', 'qualifications', 'achievements', 'certifications'
        ]
        resume_score = sum(1 for indicator in resume_indicators if indicator in content_lower)

        # Job description indicators
        job_indicators = [
            'requirements', 'responsibilities', 'qualifications', 'benefits',
            'position', 'role', 'company', 'salary', 'job description'
        ]
        job_score = sum(1 for indicator in job_indicators if indicator in content_lower)

        # Cover letter indicators
        cover_indicators = [
            'dear', 'sincerely', 'position', 'application', 'interested',
            'opportunity', 'thank you', 'looking forward'
        ]
        cover_score = sum(1 for indicator in cover_indicators if indicator in content_lower)

        # File name hints
        file_name_lower = file_path.lower()
        if any(word in file_name_lower for word in ['resume', 'cv']):
            resume_score += 3
        elif any(word in file_name_lower for word in ['job', 'posting', 'description']):
            job_score += 3
        elif any(word in file_name_lower for word in ['cover', 'letter']):
            cover_score += 3

        # Determine document type
        max_score = max(resume_score, job_score, cover_score)
        if max_score >= 3:
            if resume_score == max_score:
                return 'resume'
            elif job_score == max_score:
                return 'job_description'
            elif cover_score == max_score:
                return 'cover_letter'

        return 'generic'

    def _chunk_resume(self, content: str, structure_metadata: Dict) -> List[DocumentChunk]:
        """Chunk resume documents by logical sections"""
        chunks = []
        lines = content.split('\n')
        current_section = None
        section_content = []
        section_start = 0
        chunk_index = 0

        for i, line in enumerate(lines):
            line_stripped = line.strip()

            # Check if this line is a section header
            detected_section = self._detect_career_section(line_stripped)

            if detected_section and current_section is not None:
                # Save previous section as chunk
                if section_content:
                    section_text = '\n'.join(section_content).strip()
                    if len(section_text) >= self.min_chunk_size:
                        chunk = self._create_chunk(
                            section_text, chunk_index, 'section_based',
                            section_start, i, current_section
                        )
                        chunk.metadata.update({
                            'section_type': current_section,
                            'document_type': 'resume'
                        })
                        chunks.append(chunk)
                        chunk_index += 1

                # Start new section
                current_section = detected_section
                section_content = [line]
                section_start = i
            else:
                if current_section is None:
                    current_section = 'header'
                    section_start = i
                section_content.append(line)

        # Handle the last section
        if section_content:
            section_text = '\n'.join(section_content).strip()
            if len(section_text) >= self.min_chunk_size:
                chunk = self._create_chunk(
                    section_text, chunk_index, 'section_based',
                    section_start, len(lines), current_section or 'content'
                )
                chunk.metadata.update({
                    'section_type': current_section or 'content',
                    'document_type': 'resume'
                })
                chunks.append(chunk)

        return chunks

    def _chunk_job_description(self, content: str, structure_metadata: Dict) -> List[DocumentChunk]:
        """Chunk job description documents"""
        chunks = []

        # Common job description sections
        job_sections = [
            'job title', 'company', 'overview', 'description', 'responsibilities',
            'requirements', 'qualifications', 'skills', 'benefits', 'salary',
            'location', 'employment type'
        ]

        # Try to split by obvious sections first
        section_chunks = self._split_by_headers(content, structure_metadata)

        if len(section_chunks) < 2:
            # If no clear sections, split by content blocks
            section_chunks = self._split_by_content_blocks(content)

        for i, chunk_text in enumerate(section_chunks):
            if len(chunk_text.strip()) >= self.min_chunk_size:
                chunk = self._create_chunk(
                    chunk_text, i, 'content_based', 0, len(chunk_text), 'content'
                )
                chunk.metadata.update({
                    'document_type': 'job_description',
                    'section_index': i
                })
                chunks.append(chunk)

        return chunks

    def _chunk_cover_letter(self, content: str, structure_metadata: Dict) -> List[DocumentChunk]:
        """Chunk cover letter documents by paragraphs"""
        chunks = []

        # Split by paragraphs (double newlines)
        paragraphs = re.split(r'\n\s*\n', content.strip())

        chunk_index = 0
        current_position = 0

        for paragraph in paragraphs:
            paragraph = paragraph.strip()
            if len(paragraph) >= self.min_chunk_size:
                chunk = self._create_chunk(
                    paragraph, chunk_index, 'paragraph_based',
                    current_position, current_position + len(paragraph), 'paragraph'
                )
                chunk.metadata.update({
                    'document_type': 'cover_letter',
                    'paragraph_index': chunk_index
                })
                chunks.append(chunk)
                chunk_index += 1

            current_position += len(paragraph) + 2  # Account for newlines

        return chunks

    def _chunk_generic_document(self, content: str, structure_metadata: Dict) -> List[DocumentChunk]:
        """Generic structure-based chunking for other document types"""
        chunks = []

        # First try to split by headers
        header_chunks = self._split_by_headers(content, structure_metadata)

        if len(header_chunks) >= 2:
            for i, chunk_text in enumerate(header_chunks):
                if len(chunk_text.strip()) >= self.min_chunk_size:
                    chunk = self._create_chunk(
                        chunk_text, i, 'header_based', 0, len(chunk_text), 'section'
                    )
                    chunks.append(chunk)
        else:
            # Fall back to content block splitting
            content_chunks = self._split_by_content_blocks(content)
            for i, chunk_text in enumerate(content_chunks):
                if len(chunk_text.strip()) >= self.min_chunk_size:
                    chunk = self._create_chunk(
                        chunk_text, i, 'content_based', 0, len(chunk_text), 'content'
                    )
                    chunks.append(chunk)

        return chunks

    def _detect_career_section(self, line: str) -> Optional[str]:
        """Detect if a line represents a career document section header"""
        line_clean = line.lower().strip()

        for section_type, patterns in self.CAREER_SECTION_PATTERNS.items():
            for pattern in patterns:
                if re.match(pattern, line_clean, re.IGNORECASE):
                    return section_type

        return None

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
            chunk_type=chunk_type,
            metadata={}
        )

    def _post_process_chunks(self, chunks: List[DocumentChunk], doc_type: str) -> List[DocumentChunk]:
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
                    chunk_type=chunk.chunk_type,
                    metadata=chunk.metadata.copy()
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
                chunk_type=chunk.chunk_type,
                metadata=chunk.metadata.copy()
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