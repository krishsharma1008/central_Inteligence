import logging
import time
from typing import List, Dict, Any, Optional
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import DuplicateKeyError

# Configure logging
logger = logging.getLogger('outlook-email.mongodb')

class MongoDBHandler:
    def __init__(self, connection_string: str, collection_name: str) -> None:
        """
        Initialize the MongoDBHandler with the connection string and collection name.

        Args:
            connection_string (str): MongoDB connection string
            collection_name (str): Name of the collection to manage
        """
        try:
            logger.info(f"Initializing MongoDB connection")
            self.client = MongoClient(connection_string)
            self.db = self.client.get_database()
            self.collection_name = collection_name
            self.collection = self._get_or_create_collection()
            # Create index on id field
            self.collection.create_index("id", unique=True)

            # Initialize attachment and chunk collections
            self.attachments_collection = self.db[f"{collection_name}_attachments"]
            self.attachments_collection.create_index("id", unique=True)
            self.attachments_collection.create_index("email_id")

            self.chunks_collection = self.db[f"{collection_name}_chunks"]
            self.chunks_collection.create_index("id", unique=True)
            self.chunks_collection.create_index([("parent_id", 1), ("chunk_number", 1)])
            self.chunks_collection.create_index("email_id")

            logger.info("MongoDB initialized successfully with attachment and chunk collections")
        except Exception as e:
            logger.error(f"Error initializing MongoDB: {str(e)}", exc_info=True)
            raise

    def _get_or_create_collection(self, max_retries: int = 3) -> Collection:
        """Get or create collection with retry logic."""
        for attempt in range(max_retries):
            try:
                return self.db[self.collection_name]
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                logger.warning(f"Retry {attempt + 1}/{max_retries} getting collection: {str(e)}")
                time.sleep(1)  # Wait before retry

    def add_embeddings(self, embeddings: List[Dict[str, Any]], job_id: Optional[str] = None) -> bool:
        """
        Add embeddings to the MongoDB collection.

        Args:
            embeddings (List[Dict]): List of embeddings to add
            job_id (str, optional): Job ID for tracking

        Returns:
            bool: True if embeddings were added successfully
        """
        try:
            # Filter out embeddings with existing IDs
            new_embeddings = []
            for embedding in embeddings:
                if not all(k in embedding for k in ['id', 'embedding', 'document', 'metadata']):
                    raise ValueError("Missing required fields in embedding")
                
                try:
                    # Check if ID already exists
                    if not self.email_exists(str(embedding['id'])):
                        # Initialize and sanitize metadata
                        embedding['metadata'] = embedding.get('metadata', {})
                        # Ensure all metadata values are primitive types or allowed dicts
                        for key, value in embedding['metadata'].items():
                            # Keep 'analysis' as dict for structured storage
                            if key == 'analysis' and isinstance(value, dict):
                                continue
                            elif isinstance(value, (list, dict)):
                                embedding['metadata'][key] = str(value)
                            elif value is None:
                                embedding['metadata'][key] = ''
                        new_embeddings.append(embedding)
                except Exception as e:
                    logger.warning(f"Error checking email existence: {str(e)}")
                    continue
            
            if not new_embeddings:
                logger.info("No new embeddings to add")
                return True
            
            logger.info(f"Adding {len(new_embeddings)} embeddings to MongoDB")
            
            # Add embeddings with retry logic
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    # Convert embeddings to MongoDB documents
                    documents = []
                    for embedding in new_embeddings:
                        doc = {
                            'id': str(embedding['id']),
                            'embedding': embedding['embedding'],
                            'document': embedding['document'],
                            'metadata': embedding['metadata']
                        }
                        documents.append(doc)
                    
                    # Insert documents
                    self.collection.insert_many(documents)
                    logger.info("Successfully added embeddings to MongoDB")
                    return True
                except DuplicateKeyError:
                    logger.warning("Duplicate key found, skipping those documents")
                    return True
                except Exception as e:
                    if attempt == max_retries - 1:
                        logger.error(f"Failed to add embeddings after {max_retries} attempts: {str(e)}")
                        return False
                    logger.warning(f"Retry {attempt + 1}/{max_retries} adding embeddings: {str(e)}")
                    time.sleep(1)  # Wait before retry
            
        except Exception as e:
            logger.error(f"Error adding embeddings: {str(e)}", exc_info=True)
            return False

    def email_exists(self, entry_id: str) -> bool:
        """
        Check if an email entry exists.

        Args:
            entry_id (str): ID of the email entry

        Returns:
            bool: True if exists
        """
        try:
            result = self.collection.find_one({'id': str(entry_id)})
            return result is not None
        except Exception as e:
            logger.error(f"Error checking email existence: {str(e)}")
            return False

    def get_collection_count(self) -> int:
        """
        Get the count of documents in the collection.

        Returns:
            int: Number of documents
        """
        try:
            return self.collection.count_documents({})
        except Exception as e:
            logger.error(f"Error getting collection count: {str(e)}")
            return 0

    def get_metadata(self, entry_id: str) -> Optional[Dict[str, Any]]:
        """
        Get metadata for a specific entry.

        Args:
            entry_id (str): ID of the entry

        Returns:
            Optional[Dict[str, Any]]: The metadata if found
        """
        try:
            result = self.collection.find_one({'id': str(entry_id)})
            if result:
                return result.get('metadata')
            return None
        except Exception as e:
            logger.error(f"Error getting metadata: {str(e)}")
            return None

    def add_attachment_with_binary(self, attachment_data: Dict[str, Any]) -> bool:
        """
        Store attachment with binary content and embedding in MongoDB.

        Args:
            attachment_data: Dictionary with fields:
                - id: attachment ID
                - email_id: parent email ID
                - filename: filename
                - binary_data: file bytes
                - metadata: attachment metadata (mime_type, file_size, etc.)
                - extracted_text: extracted text content
                - embedding: embedding vector
                - chunk_ids: list of chunk IDs

        Returns:
            bool: True if successful
        """
        try:
            from bson.binary import Binary

            doc = {
                'id': str(attachment_data['id']),
                'email_id': str(attachment_data['email_id']),
                'filename': attachment_data['filename'],
                'binary_data': Binary(attachment_data['binary_data']),
                'metadata': attachment_data.get('metadata', {}),
                'extracted_text': attachment_data.get('extracted_text', ''),
                'embedding': attachment_data.get('embedding', []),
                'chunk_ids': attachment_data.get('chunk_ids', [])
            }

            self.attachments_collection.insert_one(doc)
            logger.info(f"Added attachment {attachment_data['filename']} to MongoDB")
            return True
        except DuplicateKeyError:
            logger.warning(f"Attachment {attachment_data['id']} already exists")
            return True
        except Exception as e:
            logger.error(f"Error adding attachment: {str(e)}", exc_info=True)
            return False

    def get_attachment_binary(self, attachment_id: str) -> Optional[bytes]:
        """
        Retrieve attachment binary content from MongoDB.

        Args:
            attachment_id: Attachment ID

        Returns:
            Optional[bytes]: Binary content if found
        """
        try:
            result = self.attachments_collection.find_one({'id': str(attachment_id)})
            if result and 'binary_data' in result:
                return bytes(result['binary_data'])
            return None
        except Exception as e:
            logger.error(f"Error getting attachment binary: {str(e)}")
            return None

    def add_chunk_embeddings(self, chunks: List[Dict[str, Any]]) -> bool:
        """
        Batch insert chunk embeddings to MongoDB.

        Args:
            chunks: List of chunk dictionaries with fields:
                - id: chunk ID
                - parent_id: parent attachment ID
                - email_id: root email ID
                - chunk_number: chunk sequence number
                - total_chunks: total number of chunks
                - text: chunk text content
                - embedding: embedding vector
                - metadata: additional chunk metadata

        Returns:
            bool: True if successful
        """
        try:
            if not chunks:
                return True

            documents = []
            for chunk in chunks:
                doc = {
                    'id': str(chunk['id']),
                    'parent_id': str(chunk['parent_id']),
                    'email_id': str(chunk['email_id']),
                    'chunk_number': chunk['chunk_number'],
                    'total_chunks': chunk['total_chunks'],
                    'text': chunk['text'],
                    'embedding': chunk['embedding'],
                    'metadata': chunk.get('metadata', {})
                }
                documents.append(doc)

            self.chunks_collection.insert_many(documents)
            logger.info(f"Added {len(documents)} chunk embeddings to MongoDB")
            return True
        except DuplicateKeyError:
            logger.warning("Some chunks already exist, skipping duplicates")
            return True
        except Exception as e:
            logger.error(f"Error adding chunk embeddings: {str(e)}", exc_info=True)
            return False

    def search_chunks(self, query_embedding: List[float], top_k: int = 10) -> List[Dict[str, Any]]:
        """
        Vector similarity search on chunk embeddings.
        Note: This is a simple implementation. For production, consider using MongoDB Atlas Vector Search.

        Args:
            query_embedding: Query embedding vector
            top_k: Number of results to return

        Returns:
            List of chunks with similarity scores
        """
        try:
            # Fetch all chunks (for simple implementation)
            # In production, use MongoDB Atlas Vector Search or similar
            chunks = list(self.chunks_collection.find({}, {'_id': 0}))

            # Calculate cosine similarity
            import numpy as np

            query_vec = np.array(query_embedding)
            query_norm = np.linalg.norm(query_vec)

            results = []
            for chunk in chunks:
                if 'embedding' in chunk and chunk['embedding']:
                    chunk_vec = np.array(chunk['embedding'])
                    chunk_norm = np.linalg.norm(chunk_vec)

                    if query_norm > 0 and chunk_norm > 0:
                        similarity = np.dot(query_vec, chunk_vec) / (query_norm * chunk_norm)
                        chunk['similarity'] = float(similarity)
                        results.append(chunk)

            # Sort by similarity and return top_k
            results.sort(key=lambda x: x['similarity'], reverse=True)
            return results[:top_k]

        except Exception as e:
            logger.error(f"Error searching chunks: {str(e)}", exc_info=True)
            return []

    def get_chunks_by_parent(self, parent_id: str) -> List[Dict[str, Any]]:
        """
        Get all chunks for a parent attachment.

        Args:
            parent_id: Parent attachment ID

        Returns:
            List of chunk dictionaries
        """
        try:
            chunks = list(self.chunks_collection.find(
                {'parent_id': str(parent_id)},
                {'_id': 0}
            ).sort('chunk_number', 1))
            return chunks
        except Exception as e:
            logger.error(f"Error getting chunks by parent: {str(e)}")
            return []

    def get_attachment_metadata(self, attachment_id: str) -> Optional[Dict[str, Any]]:
        """
        Get attachment metadata without binary content.

        Args:
            attachment_id: Attachment ID

        Returns:
            Optional[Dict]: Attachment metadata
        """
        try:
            result = self.attachments_collection.find_one(
                {'id': str(attachment_id)},
                {'_id': 0, 'binary_data': 0}
            )
            return result
        except Exception as e:
            logger.error(f"Error getting attachment metadata: {str(e)}")
            return None

    def close(self) -> None:
        """Close the MongoDB connection."""
        try:
            if hasattr(self, 'client') and self.client:
                self.client.close()
                logger.info("MongoDB connection closed")
        except Exception as e:
            logger.error(f"Error closing MongoDB connection: {str(e)}", exc_info=True)
    
    def __del__(self) -> None:
        """Destructor to ensure connection is closed when object is garbage collected."""
        self.close()
    
    def __enter__(self) -> 'MongoDBHandler':
        """Enter context manager."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit context manager and close connection."""
        self.close()
