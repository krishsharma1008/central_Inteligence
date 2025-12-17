"""
MongoDB vector search and reranking helpers.
"""
import logging
import numpy as np
from typing import List, Dict, Any, Optional
from src.MongoDBHandler import MongoDBHandler

logger = logging.getLogger('outlook-email.rag.vectors')


class VectorReranker:
    """Helper class for vector-based reranking of email search results."""
    
    def __init__(self, mongodb_handler: MongoDBHandler, embedding_model=None):
        """
        Initialize the vector reranker.
        
        Args:
            mongodb_handler (MongoDBHandler): MongoDB handler
            embedding_model: Sentence-transformers model for query embedding
        """
        self.mongodb = mongodb_handler
        self.embedding_model = embedding_model
    
    def rerank(self, email_ids: List[str], query_embedding: List[float], top_k: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Rerank emails by cosine similarity with query embedding.
        
        Args:
            email_ids (List[str]): List of email IDs to rerank
            query_embedding (List[float]): Query embedding vector
            top_k (Optional[int]): Number of top results to return (None = all)
            
        Returns:
            List[Dict[str, Any]]: Reranked emails with similarity scores
        """
        if not email_ids:
            return []
        
        logger.info(f"Reranking {len(email_ids)} emails using vector similarity")
        
        # Fetch embeddings from MongoDB
        scored_emails = []
        for email_id in email_ids:
            try:
                # Get embedding from MongoDB
                doc = self.mongodb.collection.find_one({'id': email_id})
                if doc and 'embedding' in doc:
                    email_embedding = doc['embedding']
                    
                    # Compute cosine similarity
                    similarity = self._cosine_similarity(query_embedding, email_embedding)
                    
                    scored_emails.append({
                        'id': email_id,
                        'similarity': similarity,
                        'metadata': doc.get('metadata', {})
                    })
            except Exception as e:
                logger.error(f"Error getting embedding for email {email_id}: {str(e)}")
                continue
        
        # Sort by similarity (descending)
        scored_emails.sort(key=lambda x: x['similarity'], reverse=True)
        
        # Return top_k if specified
        if top_k is not None:
            scored_emails = scored_emails[:top_k]
        
        logger.info(f"Reranked to {len(scored_emails)} emails")
        return scored_emails
    
    def _cosine_similarity(self, vec1: List[float], vec2: List[float]) -> float:
        """
        Compute cosine similarity between two vectors.
        
        Args:
            vec1 (List[float]): First vector
            vec2 (List[float]): Second vector
            
        Returns:
            float: Cosine similarity score
        """
        # Convert to numpy arrays
        v1 = np.array(vec1)
        v2 = np.array(vec2)
        
        # Compute cosine similarity
        dot_product = np.dot(v1, v2)
        norm1 = np.linalg.norm(v1)
        norm2 = np.linalg.norm(v2)
        
        if norm1 == 0 or norm2 == 0:
            return 0.0
        
        return float(dot_product / (norm1 * norm2))
    
    def embed_query(self, query: str) -> Optional[List[float]]:
        """
        Generate embedding for a query string.
        
        Args:
            query (str): Query text
            
        Returns:
            Optional[List[float]]: Query embedding vector
        """
        if self.embedding_model is None:
            logger.warning("No embedding model available for query embedding")
            return None
        
        try:
            # Generate embedding with normalization
            embedding_array = self.embedding_model.encode(
                [query],
                normalize_embeddings=True,
                show_progress_bar=False
            )
            return embedding_array[0].tolist()
        except Exception as e:
            logger.error(f"Error embedding query: {str(e)}")
            return None



