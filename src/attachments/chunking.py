"""
Intelligent document chunking with semantic boundary detection.
Splits long documents into manageable chunks while preserving context.
"""
import logging
import re
from typing import List, Dict, Any

logger = logging.getLogger('outlook-email.chunking')


class DocumentChunker:
    """
    Intelligent document chunking with semantic boundaries.

    Chunks documents respecting:
    1. Paragraph boundaries (double newlines)
    2. Sentence boundaries (periods, etc.)
    3. Word boundaries (spaces)

    Maintains overlap between chunks for context continuity.
    """

    def __init__(self, chunk_size: int = 800, overlap: int = 75):
        """
        Initialize the document chunker.

        Args:
            chunk_size: Target size per chunk in characters (~600 chars = ~800 tokens)
            overlap: Overlap between chunks in characters
        """
        self.chunk_size = chunk_size
        self.overlap = overlap

        # Sentence boundary patterns
        self.sentence_endings = re.compile(r'[.!?]+\s+')

    def chunk_document(self, text: str, doc_metadata: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Chunk document respecting semantic boundaries.

        Algorithm:
        1. Split on paragraph boundaries (double newlines) - priority 1
        2. If paragraph > chunk_size, split on sentences
        3. If sentence > chunk_size, split on word boundaries
        4. Add overlap between chunks for context continuity

        Args:
            text: Document text to chunk
            doc_metadata: Metadata about the document (type, filename, etc.)

        Returns:
            List of chunk dictionaries with:
                - chunk_number: Sequence number (0-indexed)
                - total_chunks: Total number of chunks
                - chunk_text: The chunk content
                - start_position: Start character position in original text
                - end_position: End character position in original text
                - token_count: Approximate token count
                - metadata: Chunk metadata
        """
        if not text or len(text) == 0:
            return []

        # If text is shorter than chunk size, return as single chunk
        if len(text) <= self.chunk_size:
            return [{
                'chunk_number': 0,
                'total_chunks': 1,
                'chunk_text': text,
                'start_position': 0,
                'end_position': len(text),
                'token_count': self._estimate_tokens(text),
                'metadata': {**doc_metadata, 'is_single_chunk': True}
            }]

        # Step 1: Detect boundaries
        boundaries = self._detect_boundaries(text)

        # Step 2: Create chunks respecting boundaries
        chunks = self._create_chunks_with_boundaries(text, boundaries, doc_metadata)

        logger.info(f"Created {len(chunks)} chunks for document (avg size: {sum(len(c['chunk_text']) for c in chunks) // len(chunks)} chars)")

        return chunks

    def _detect_boundaries(self, text: str) -> List[int]:
        """
        Detect semantic boundaries in text.

        Priority:
        1. Paragraph boundaries (double newlines)
        2. Sentence boundaries (periods, etc.)
        3. Word boundaries (spaces)

        Returns:
            List of character positions that are good split points
        """
        boundaries = []

        # Paragraph boundaries (double newlines)
        para_pattern = re.compile(r'\n\s*\n')
        for match in para_pattern.finditer(text):
            boundaries.append(match.end())

        # Sentence boundaries (periods, exclamation, question marks)
        for match in self.sentence_endings.finditer(text):
            boundaries.append(match.end())

        # Word boundaries (spaces) - add every 100 characters as fallback
        for i in range(0, len(text), 100):
            next_space = text.find(' ', i)
            if next_space != -1 and next_space not in boundaries:
                boundaries.append(next_space)

        # Sort and deduplicate
        boundaries = sorted(set(boundaries))

        return boundaries

    def _create_chunks_with_boundaries(
        self,
        text: str,
        boundaries: List[int],
        doc_metadata: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Create chunks respecting semantic boundaries.

        Args:
            text: Original text
            boundaries: List of good split points
            doc_metadata: Document metadata

        Returns:
            List of chunks with overlap
        """
        chunks = []
        text_len = len(text)
        current_pos = 0
        chunk_num = 0

        while current_pos < text_len:
            # Find the end position for this chunk
            target_end = current_pos + self.chunk_size

            if target_end >= text_len:
                # Last chunk
                chunk_text = text[current_pos:]
                end_pos = text_len
            else:
                # Find the nearest boundary after target_end
                end_pos = self._find_nearest_boundary(boundaries, target_end)

                # If no boundary found or too far, use target_end
                if end_pos is None or end_pos > target_end + (self.chunk_size // 2):
                    end_pos = target_end

                chunk_text = text[current_pos:end_pos]

            # Add chunk
            chunks.append({
                'chunk_number': chunk_num,
                'total_chunks': 0,  # Will update after creating all chunks
                'chunk_text': chunk_text.strip(),
                'start_position': current_pos,
                'end_position': end_pos,
                'token_count': self._estimate_tokens(chunk_text),
                'metadata': {
                    **doc_metadata,
                    'overlap_chars': self.overlap if chunk_num > 0 else 0
                }
            })

            # Move to next chunk with overlap
            current_pos = end_pos - self.overlap if end_pos < text_len else end_pos
            chunk_num += 1

        # Update total_chunks for all chunks
        total = len(chunks)
        for chunk in chunks:
            chunk['total_chunks'] = total

        return chunks

    def _find_nearest_boundary(self, boundaries: List[int], position: int) -> int:
        """
        Find the nearest boundary at or after the given position.

        Args:
            boundaries: Sorted list of boundary positions
            position: Target position

        Returns:
            Nearest boundary position or None if not found
        """
        # Binary search for nearest boundary >= position
        left, right = 0, len(boundaries) - 1

        while left <= right:
            mid = (left + right) // 2
            if boundaries[mid] < position:
                left = mid + 1
            elif boundaries[mid] > position:
                right = mid - 1
            else:
                return boundaries[mid]

        # Return the boundary at 'left' if it exists
        if left < len(boundaries):
            return boundaries[left]

        return None

    def _estimate_tokens(self, text: str) -> int:
        """
        Estimate token count for text.

        Rough approximation: 1 token ≈ 0.75 characters (English)

        Args:
            text: Text to estimate

        Returns:
            Estimated token count
        """
        return int(len(text) * 0.75)

    def should_chunk(self, text: str) -> bool:
        """
        Determine if text should be chunked.

        Args:
            text: Text to check

        Returns:
            bool: True if text exceeds threshold and should be chunked
        """
        # Chunk if text is more than 1.5x chunk_size
        threshold = int(self.chunk_size * 1.5)
        return len(text) > threshold
