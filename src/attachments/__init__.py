"""
Attachment processing module for email attachments.
"""

from .document_extractors import (
    PDFExtractor,
    DOCXExtractor,
    XLSXExtractor,
    PPTXExtractor,
    TextExtractor,
    DocumentExtractorFactory
)

from .chunking import DocumentChunker

__all__ = [
    'PDFExtractor',
    'DOCXExtractor',
    'XLSXExtractor',
    'PPTXExtractor',
    'TextExtractor',
    'DocumentExtractorFactory',
    'DocumentChunker'
]
