import json
import uuid
import os
import sys
import time
import logging
from typing import List, Dict, Any, Tuple
from datetime import datetime
from src.SarvamClient import SarvamClient

# Configure logging
logger = logging.getLogger('outlook-email.embedding')

class EmbeddingProcessor:
    def __init__(self, db_path: str, collection_name: str, sarvam_api_key: str):
        """
        Initialize the embedding processor.
        
        Args:
            db_path: Path to storage
            collection_name: Name of the collection to use
            sarvam_api_key: Sarvam API key for embeddings and analysis
        """
        # Import here to avoid circular imports
        from src.MongoDBHandler import MongoDBHandler
        
        # Initialize MongoDB handler
        self.mongodb_handler = MongoDBHandler(
            db_path,
            collection_name
        )
        
        # Initialize Sarvam client for analysis
        try:
            self.sarvam_client = SarvamClient(api_key=sarvam_api_key)
            logger.info("Sarvam client initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing Sarvam client: {str(e)}")
            raise
        
        # Initialize sentence-transformers for real embeddings
        try:
            from sentence_transformers import SentenceTransformer
            model_name = os.getenv("EMBEDDING_MODEL_NAME", "sentence-transformers/all-MiniLM-L6-v2")
            logger.info(f"Loading embedding model: {model_name}")
            self.embedding_model = SentenceTransformer(model_name)
            logger.info(f"Embedding model loaded successfully (dimension: {self.embedding_model.get_sentence_embedding_dimension()})")
        except Exception as e:
            logger.error(f"Error loading embedding model: {str(e)}")
            logger.warning("Falling back to hash-based embeddings")
            self.embedding_model = None
    
    def create_email_content(self, email: Dict[str, Any]) -> str:
        """Create a formatted string of email content for embedding."""
        return f"""
Subject: {email.get('Subject', '')}
From: {email.get('SenderName', '')} <{email.get('SenderEmailAddress', '')}>
To: {email.get('To', '')}
Date: {email.get('ReceivedTime', '')}

{email.get('Body', '')}
"""

    def validate_email_data(self, email: Dict[str, Any]) -> bool:
        """Validate email data structure and content."""
        required_fields = [
            'Subject', 'SenderName', 'SenderEmailAddress', 'To', 
            'ReceivedTime', 'Folder', 'AccountName', 'Body'
        ]
        
        # Check required fields exist and are not None
        for field in required_fields:
            if field not in email or email[field] is None:
                return False
                
        # Validate dates are in ISO format
        try:
            if email['ReceivedTime']:
                datetime.fromisoformat(email['ReceivedTime'])
        except (ValueError, TypeError):
            return False
            
        return True

    def process_batch(self, emails: List[Dict[str, Any]], batch_size: int = 4) -> Tuple[int, int]:
        """
        Process a batch of emails to generate embeddings with validation.
        
        Args:
            emails: List of email dictionaries to process
            batch_size: Size of batches for processing (default: 4)
            
        Returns:
            Tuple[int, int]: (number of successfully processed emails, number of failed emails)
        """
        documents = []
        metadatas = []
        ids = []
        failed_count = 0
        
        for i, email in enumerate(emails):
            try:
                # Validate email data
                if not self.validate_email_data(email):
                    failed_count += 1
                    continue
                    
                # Create content for embedding
                content = self.create_email_content(email)
                
                # Create metadata dictionary
                metadata = {
                    'Subject': email.get('Subject', ''),
                    'SenderName': email.get('SenderName', ''),
                    'SenderEmailAddress': email.get('SenderEmailAddress', ''),
                    'To': email.get('To', ''),
                    'ReceivedTime': email.get('ReceivedTime', ''),
                    'Folder': email.get('Folder', ''),
                    'AccountName': email.get('AccountName', ''),
                    'ConversationId': email.get('ConversationId', '') or None,
                    'ConversationIndex': email.get('ConversationIndex', '') or None,
                    'InternetMessageId': email.get('InternetMessageId', '') or None
                }
                
                # Validate metadata can be JSON encoded
                try:
                    json.dumps(metadata)
                except (TypeError, ValueError):
                    failed_count += 1
                    continue
                
                documents.append(content)
                metadatas.append(metadata)
                ids.append(email.get('id', str(uuid.uuid4())))
                
            except Exception as e:
                failed_count += 1
                continue
        
        if not documents:
            return 0, failed_count
        
        # Process documents in batches
        try:
            # Generate embeddings using sentence-transformers
            embeddings = None
            
            if self.embedding_model is not None:
                try:
                    logger.info(f"Generating real embeddings for {len(documents)} documents using sentence-transformers")
                    # Normalize embeddings for cosine similarity
                    embeddings_array = self.embedding_model.encode(
                        documents, 
                        normalize_embeddings=True,
                        show_progress_bar=False
                    )
                    # Convert numpy arrays to lists for MongoDB storage
                    embeddings = [emb.tolist() for emb in embeddings_array]
                    logger.info(f"Successfully generated {len(embeddings)} real embeddings")
                except Exception as e:
                    logger.error(f"Error generating embeddings with sentence-transformers: {str(e)}")
                    logger.warning("Falling back to hash-based embeddings")
                    embeddings = None
            
            # Fallback to hash-based embeddings if model not available
            if embeddings is None:
                logger.warning("Using fallback hash-based embeddings (vector search will not be meaningful)")
                embeddings = self._generate_fallback_embeddings(documents)
            
            if not embeddings:
                logger.error("No embeddings generated")
                return 0, len(documents) + failed_count
            
            # Analyze emails using Sarvam client
            logger.info(f"Analyzing {len(documents)} emails")
            analyses = self.sarvam_client.analyze_batch(documents)
            
            # Create batch of documents to add to MongoDB
            batch = [{
                'id': id_,
                'embedding': emb,
                'document': doc,
                'metadata': {**meta, 'analysis': analysis}
            } for id_, emb, doc, meta, analysis in zip(ids, embeddings, documents, metadatas, analyses)]
            
            logger.info(f"Adding {len(batch)} documents to MongoDB")
            
            # Add to MongoDB with retries
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    if self.mongodb_handler.add_embeddings(batch):
                        logger.info(f"Successfully added {len(batch)} documents to MongoDB")
                        return len(batch), failed_count
                    else:
                        logger.warning(f"Failed to add documents to MongoDB (attempt {attempt + 1}/{max_retries})")
                        if attempt < max_retries - 1:
                            time.sleep(1)
                            continue
                        return 0, len(batch) + failed_count
                except Exception as e:
                    logger.error(f"Error adding documents to MongoDB (attempt {attempt + 1}/{max_retries}): {str(e)}")
                    if attempt < max_retries - 1:
                        time.sleep(1)
                        continue
                    return 0, len(batch) + failed_count
                    
        except Exception as e:
            logger.error(f"Error processing batch: {str(e)}")
            return 0, len(documents) + failed_count
    
    def _generate_fallback_embeddings(self, texts: List[str], dimension: int = 384) -> List[List[float]]:
        """
        Generate fallback hash-based embeddings (for when sentence-transformers fails).
        This is NOT suitable for meaningful vector search.
        
        Args:
            texts (List[str]): Texts to embed
            dimension (int): Embedding dimension
            
        Returns:
            List[List[float]]: List of embedding vectors
        """
        import hashlib
        
        embeddings = []
        for text in texts:
            # Create a hash of the text
            text_hash = hashlib.sha256(text.encode()).hexdigest()
            
            # Convert hash to numbers and normalize
            embedding = []
            for i in range(dimension):
                # Use parts of the hash to generate pseudo-random numbers
                hash_part = text_hash[(i * 2) % len(text_hash):(i * 2 + 2) % len(text_hash)]
                value = int(hash_part or "00", 16) / 255.0 * 2 - 1  # Normalize to [-1, 1]
                embedding.append(value)
            
            embeddings.append(embedding)
        
        return embeddings
            
    def close(self) -> None:
        """Close connections."""
        try:
            # Close MongoDB connection
            if hasattr(self, 'mongodb_handler'):
                self.mongodb_handler.close()
                logger.info("MongoDB connection closed from EmbeddingProcessor")
        except Exception as e:
            logger.error(f"Error closing connections: {str(e)}")
    
    def __del__(self) -> None:
        """Destructor to ensure connections are closed when object is garbage collected."""
        self.close()
