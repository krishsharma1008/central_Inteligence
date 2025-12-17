"""
Attachment Handler - Orchestrates attachment processing pipeline.
Downloads, extracts text, chunks, generates embeddings, and stores attachments.
"""
import logging
import uuid
import os
from typing import Dict, Any, List, Tuple
from src.attachments.document_extractors import DocumentExtractorFactory
from src.attachments.chunking import DocumentChunker

logger = logging.getLogger('outlook-email.attachment-handler')


class AttachmentHandler:
    """Orchestrates attachment download, processing, and storage."""

    def __init__(self, graph_connector, sqlite_handler, mongo_handler, embedding_model):
        """
        Initialize the attachment handler.

        Args:
            graph_connector: GraphConnector instance for downloading attachments
            sqlite_handler: SQLiteHandler instance for metadata storage
            mongo_handler: MongoDBHandler instance for binary and embedding storage
            embedding_model: SentenceTransformer model for generating embeddings
        """
        self.graph = graph_connector
        self.sqlite = sqlite_handler
        self.mongo = mongo_handler
        self.embedding_model = embedding_model
        self.chunker = DocumentChunker(
            chunk_size=int(os.getenv('ATTACHMENT_CHUNK_SIZE', '800')),
            overlap=int(os.getenv('ATTACHMENT_CHUNK_OVERLAP', '75'))
        )
        self.max_file_size = int(os.getenv('ATTACHMENT_MAX_SIZE_MB', '25')) * 1024 * 1024  # Convert to bytes

    def process_email_attachments(self, email_id: str, message_id: str) -> int:
        """
        Process all attachments for an email.

        Flow:
        1. Get attachment list from Graph API
        2. Filter for supported types and size limits
        3. Download attachment binary
        4. Extract text using appropriate extractor
        5. Chunk if document is long (>1000 tokens)
        6. Generate embeddings for chunks or full document
        7. Store in SQLite + MongoDB

        Args:
            email_id: Email ID in our database
            message_id: Message ID in Graph API

        Returns:
            Number of attachments successfully processed
        """
        try:
            # Step 1: Get attachment list
            logger.info(f"Getting attachments for email {email_id}")
            attachments = self.graph.get_message_attachments(message_id)

            if not attachments:
                logger.info(f"No attachments found for email {email_id}")
                return 0

            logger.info(f"Found {len(attachments)} attachments for email {email_id}")

            processed_count = 0

            for att in attachments:
                try:
                    # Skip inline attachments (usually embedded images)
                    if att.get('isInline', False):
                        logger.info(f"Skipping inline attachment: {att.get('name')}")
                        continue

                    # Check file size
                    file_size = att.get('size', 0)
                    if file_size > self.max_file_size:
                        logger.warning(f"Skipping large attachment {att.get('name')}: {file_size} bytes (max: {self.max_file_size})")
                        continue

                    # Check if supported MIME type or file extension
                    mime_type = att.get('contentType') or ''
                    filename = att.get('name', '')
                    
                    # If no MIME type or it's None/null, try to detect by filename extension
                    if not mime_type or mime_type == 'None' or str(mime_type).lower() == 'none':
                        filename_lower = filename.lower()
                        if filename_lower.endswith('.msg'):
                            mime_type = 'application/vnd.ms-outlook'
                        elif filename_lower.endswith('.pdf'):
                            mime_type = 'application/pdf'
                        elif filename_lower.endswith(('.docx', '.doc')):
                            mime_type = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
                        elif filename_lower.endswith(('.xlsx', '.xls')):
                            mime_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                        elif filename_lower.endswith(('.pptx', '.ppt')):
                            mime_type = 'application/vnd.openxmlformats-officedocument.presentationml.presentation'
                        elif filename_lower.endswith(('.txt', '.csv', '.html', '.xml', '.json')):
                            mime_type = 'text/plain'
                        else:
                            # If no extension and MIME is None, assume it might be a .msg file (embedded email)
                            # These are typically embedded email messages from Outlook
                            mime_type = 'application/vnd.ms-outlook'
                            logger.info(f"Assuming .msg format for attachment without extension: {filename}")
                    
                    # Update the attachment info with detected MIME type
                    att['contentType'] = mime_type
                    
                    # Check if supported (by MIME type or extension)
                    if not DocumentExtractorFactory.is_supported(mime_type, filename):
                        logger.info(f"Skipping unsupported attachment type {mime_type}: {filename}")
                        continue

                    # Process the attachment
                    if self._process_single_attachment(email_id, message_id, att):
                        processed_count += 1

                except Exception as e:
                    logger.error(f"Error processing attachment {att.get('name')}: {str(e)}", exc_info=True)
                    continue

            logger.info(f"Successfully processed {processed_count}/{len(attachments)} attachments for email {email_id}")
            return processed_count

        except Exception as e:
            logger.error(f"Error processing email attachments: {str(e)}", exc_info=True)
            return 0

    def _process_single_attachment(
        self,
        email_id: str,
        message_id: str,
        attachment_info: Dict[str, Any]
    ) -> bool:
        """
        Process a single attachment.

        Args:
            email_id: Email ID
            message_id: Graph API message ID
            attachment_info: Attachment metadata from Graph API

        Returns:
            bool: True if successful
        """
        attachment_id = str(uuid.uuid4())
        filename = attachment_info.get('name', 'unknown')
        mime_type = attachment_info.get('contentType', '')

        try:
            logger.info(f"Processing attachment: {filename} ({mime_type})")

            # Step 1: Download binary content
            attachment_type = attachment_info.get('attachmentType', 'fileAttachment')
            binary_data = self.graph.download_attachment(message_id, attachment_info['id'], attachment_type)
            if not binary_data:
                logger.error(f"Failed to download attachment: {filename}")
                return False

            # Step 2: Extract text
            extracted = self._extract_text(binary_data, mime_type, filename)
            if 'error' in extracted:
                logger.error(f"Text extraction failed for {filename}: {extracted['error']}")
                # Still store the attachment but without text
                extracted_text = ""
                page_count = 0
            else:
                extracted_text = extracted.get('text', '')
                page_count = extracted.get('page_count', extracted.get('slide_count', 0))

            logger.info(f"Extracted {len(extracted_text)} characters from {filename}")

            # Step 3: Determine if chunking is needed
            should_chunk = self.chunker.should_chunk(extracted_text)

            chunk_ids = []
            chunk_count = 0

            if should_chunk and extracted_text:
                # Step 4: Create chunks
                chunks = self.chunker.chunk_document(
                    extracted_text,
                    {'filename': filename, 'mime_type': mime_type}
                )
                chunk_count = len(chunks)

                logger.info(f"Created {chunk_count} chunks for {filename}")

                # Step 5: Process chunks (generate embeddings and store)
                chunk_ids = self._process_chunks(chunks, attachment_id, email_id)
            else:
                # Single chunk - generate embedding for full document
                if extracted_text:
                    embedding = self._generate_embedding(extracted_text)
                else:
                    embedding = []

                # Store attachment with embedding in MongoDB
                mongo_success = self.mongo.add_attachment_with_binary({
                    'id': attachment_id,
                    'email_id': email_id,
                    'filename': filename,
                    'binary_data': binary_data,
                    'metadata': {
                        'mime_type': mime_type,
                        'file_size': len(binary_data),
                        'page_count': page_count
                    },
                    'extracted_text': extracted_text,
                    'embedding': embedding,
                    'chunk_ids': []
                })

                if not mongo_success:
                    logger.error(f"Failed to store attachment in MongoDB: {filename}")
                    return False

            # Step 6: Store attachment metadata in SQLite
            sqlite_success = self.sqlite.add_attachment({
                'id': attachment_id,
                'email_id': email_id,
                'filename': filename,
                'file_size': len(binary_data),
                'mime_type': mime_type,
                'storage_id': attachment_id,  # MongoDB document ID
                'extracted_text': extracted_text[:5000] if extracted_text else '',  # Truncate for SQLite
                'text_length': len(extracted_text),
                'page_count': page_count,
                'is_processed': True,
                'chunk_count': chunk_count
            })

            if not sqlite_success:
                logger.error(f"Failed to store attachment metadata in SQLite: {filename}")
                return False

            logger.info(f"Successfully processed attachment: {filename}")
            return True

        except Exception as e:
            logger.error(f"Error in _process_single_attachment for {filename}: {str(e)}", exc_info=True)
            return False

    def _extract_text(self, binary_data: bytes, mime_type: str, filename: str = '') -> Dict[str, Any]:
        """
        Extract text from binary data using appropriate extractor.

        Args:
            binary_data: File content as bytes
            mime_type: MIME type of the file
            filename: Optional filename for extension-based detection

        Returns:
            Dictionary with extracted text and metadata
        """
        try:
            extractor = DocumentExtractorFactory.get_extractor(mime_type, filename)
            if not extractor:
                return {'error': f'No extractor for MIME type: {mime_type}, filename: {filename}'}

            result = extractor.extract(binary_data)
            return result

        except Exception as e:
            logger.error(f"Error extracting text: {str(e)}", exc_info=True)
            return {'error': str(e)}

    def _process_chunks(
        self,
        chunks: List[Dict[str, Any]],
        attachment_id: str,
        email_id: str
    ) -> List[str]:
        """
        Generate embeddings and store chunks.

        Args:
            chunks: List of chunk dictionaries
            attachment_id: Parent attachment ID
            email_id: Root email ID

        Returns:
            List of chunk IDs
        """
        try:
            chunk_ids = []
            chunks_with_embeddings = []

            for chunk in chunks:
                chunk_id = f"{attachment_id}_chunk_{chunk['chunk_number']}"

                # Generate embedding for chunk
                embedding = self._generate_embedding(chunk['chunk_text'])

                # Prepare chunk document for MongoDB
                chunk_doc = {
                    'id': chunk_id,
                    'parent_id': attachment_id,
                    'email_id': email_id,
                    'chunk_number': chunk['chunk_number'],
                    'total_chunks': chunk['total_chunks'],
                    'text': chunk['chunk_text'],
                    'embedding': embedding,
                    'metadata': chunk.get('metadata', {})
                }

                chunks_with_embeddings.append(chunk_doc)
                chunk_ids.append(chunk_id)

                # Also store chunk metadata in SQLite
                self.sqlite.add_chunk({
                    'id': chunk_id,
                    'parent_id': attachment_id,
                    'parent_type': 'attachment',
                    'chunk_number': chunk['chunk_number'],
                    'total_chunks': chunk['total_chunks'],
                    'chunk_text': chunk['chunk_text'][:1000],  # Truncate for SQLite
                    'token_count': chunk.get('token_count', 0),
                    'has_embedding': True
                })

            # Batch insert chunks to MongoDB
            if chunks_with_embeddings:
                self.mongo.add_chunk_embeddings(chunks_with_embeddings)

            logger.info(f"Processed {len(chunk_ids)} chunks with embeddings")
            return chunk_ids

        except Exception as e:
            logger.error(f"Error processing chunks: {str(e)}", exc_info=True)
            return []

    def _generate_embedding(self, text: str) -> List[float]:
        """
        Generate embedding for text.

        Args:
            text: Text to embed

        Returns:
            Embedding vector
        """
        try:
            if self.embedding_model is None:
                logger.warning("No embedding model available, using empty embedding")
                return []

            # Generate embedding using sentence-transformers
            embedding = self.embedding_model.encode(
                text,
                normalize_embeddings=True,
                show_progress_bar=False
            )

            return embedding.tolist()

        except Exception as e:
            logger.error(f"Error generating embedding: {str(e)}", exc_info=True)
            return []
