"""
Vector similarity scoring with caching for context graph compilation.
"""

import logging
import numpy as np
from typing import List, Dict, Any, Optional
from functools import lru_cache
from src.MongoDBHandler import MongoDBHandler
from .models import Node, NodeType

logger = logging.getLogger('outlook-email.context_graph.vector_scorer')


class VectorScorer:
    """
    Centralized vector operations with caching for efficient graph compilation.
    """
    
    def __init__(self, mongodb_handler: MongoDBHandler, embedding_model=None):
        """
        Initialize vector scorer.
        
        Args:
            mongodb_handler: MongoDB handler for fetching embeddings
            embedding_model: SentenceTransformer model for query embedding
        """
        self.mongodb = mongodb_handler
        self.embedding_model = embedding_model
        self._embedding_cache = {}  # node_id -> embedding
    
    def embed_query(self, query: str) -> Optional[List[float]]:
        """
        Generate embedding for a query string with caching.
        
        Args:
            query: Query text
            
        Returns:
            Query embedding vector or None
        """
        if self.embedding_model is None:
            logger.warning("No embedding model available")
            return None
        
        # Simple cache key based on query
        cache_key = query.strip().lower()
        if cache_key in self._embedding_cache:
            logger.debug(f"Using cached embedding for query: {cache_key[:50]}")
            return self._embedding_cache[cache_key]
        
        try:
            embedding_array = self.embedding_model.encode(
                [query],
                normalize_embeddings=True,
                show_progress_bar=False
            )
            embedding = embedding_array[0].tolist()
            
            # Cache the embedding
            self._embedding_cache[cache_key] = embedding
            
            return embedding
        except Exception as e:
            logger.error(f"Error embedding query: {str(e)}")
            return None
    
    def get_node_embeddings_batch(self, nodes: List[Node]) -> Dict[str, List[float]]:
        """
        Batch fetch embeddings for multiple nodes from MongoDB.
        
        Args:
            nodes: List of nodes to fetch embeddings for
            
        Returns:
            Dictionary mapping node_id to embedding vector
        """
        # Extract IDs based on node type
        email_ids = []
        attachment_ids = []
        
        for node in nodes:
            if node.type == NodeType.DOCUMENT:
                email_id = node.props.get('email_id')
                if email_id:
                    email_ids.append((node.id, email_id))
            elif node.type == NodeType.ATTACHMENT:
                att_id = node.props.get('attachment_id')
                if att_id:
                    attachment_ids.append((node.id, att_id))
        
        embeddings = {}
        
        # Batch fetch email embeddings
        if email_ids:
            try:
                ids_to_fetch = [eid for _, eid in email_ids]
                docs = list(self.mongodb.collection.find(
                    {'id': {'$in': ids_to_fetch}},
                    {'id': 1, 'embedding': 1}
                ))
                
                id_to_emb = {doc['id']: doc['embedding'] for doc in docs if 'embedding' in doc}
                
                for node_id, email_id in email_ids:
                    if email_id in id_to_emb:
                        embeddings[node_id] = id_to_emb[email_id]
                        self._embedding_cache[node_id] = id_to_emb[email_id]
                
                logger.info(f"Fetched {len(embeddings)} email embeddings")
            except Exception as e:
                logger.error(f"Error fetching email embeddings: {str(e)}")
        
        # Batch fetch attachment embeddings
        if attachment_ids:
            try:
                ids_to_fetch = [aid for _, aid in attachment_ids]
                docs = list(self.mongodb.attachments_collection.find(
                    {'id': {'$in': ids_to_fetch}},
                    {'id': 1, 'embedding': 1}
                ))
                
                id_to_emb = {doc['id']: doc['embedding'] for doc in docs if 'embedding' in doc}
                
                for node_id, att_id in attachment_ids:
                    if att_id in id_to_emb:
                        embeddings[node_id] = id_to_emb[att_id]
                        self._embedding_cache[node_id] = id_to_emb[att_id]
                
                logger.info(f"Fetched {len(embeddings) - len([e for _, e in email_ids if e in embeddings])} attachment embeddings")
            except Exception as e:
                logger.error(f"Error fetching attachment embeddings: {str(e)}")
        
        return embeddings
    
    def compute_similarity(self, vec1: List[float], vec2: List[float]) -> float:
        """
        Compute cosine similarity between two vectors.
        
        Args:
            vec1: First vector
            vec2: Second vector
            
        Returns:
            Cosine similarity score (0-1)
        """
        if not vec1 or not vec2:
            return 0.0
        
        try:
            v1 = np.array(vec1)
            v2 = np.array(vec2)
            
            dot_product = np.dot(v1, v2)
            norm1 = np.linalg.norm(v1)
            norm2 = np.linalg.norm(v2)
            
            if norm1 == 0 or norm2 == 0:
                return 0.0
            
            return float(dot_product / (norm1 * norm2))
        except Exception as e:
            logger.error(f"Error computing similarity: {str(e)}")
            return 0.0
    
    def score_node_similarity(self, node: Node, query_embedding: List[float]) -> float:
        """
        Compute similarity score between a node and query embedding.
        
        Args:
            node: Node to score
            query_embedding: Query embedding vector
            
        Returns:
            Similarity score (0-1)
        """
        if not query_embedding:
            return 0.0
        
        # Check cache first
        if node.id in self._embedding_cache:
            node_embedding = self._embedding_cache[node.id]
            return self.compute_similarity(query_embedding, node_embedding)
        
        # Fetch from MongoDB
        node_embedding = None
        
        if node.type == NodeType.DOCUMENT:
            email_id = node.props.get('email_id')
            if email_id:
                try:
                    doc = self.mongodb.collection.find_one(
                        {'id': email_id},
                        {'embedding': 1}
                    )
                    if doc and 'embedding' in doc:
                        node_embedding = doc['embedding']
                except Exception as e:
                    logger.error(f"Error fetching email embedding: {str(e)}")
        
        elif node.type == NodeType.ATTACHMENT:
            att_id = node.props.get('attachment_id')
            if att_id:
                try:
                    doc = self.mongodb.attachments_collection.find_one(
                        {'id': att_id},
                        {'embedding': 1}
                    )
                    if doc and 'embedding' in doc:
                        node_embedding = doc['embedding']
                except Exception as e:
                    logger.error(f"Error fetching attachment embedding: {str(e)}")
        
        if node_embedding:
            self._embedding_cache[node.id] = node_embedding
            return self.compute_similarity(query_embedding, node_embedding)
        
        return 0.0
    
    def compute_edge_weight(self, node1: Node, node2: Node) -> float:
        """
        Compute semantic edge weight between two nodes based on their embeddings.
        
        Args:
            node1: First node
            node2: Second node
            
        Returns:
            Edge weight (similarity score)
        """
        # Get embeddings
        emb1 = self._embedding_cache.get(node1.id)
        emb2 = self._embedding_cache.get(node2.id)
        
        if not emb1 or not emb2:
            # Try to fetch if not in cache
            embeddings = self.get_node_embeddings_batch([node1, node2])
            emb1 = embeddings.get(node1.id)
            emb2 = embeddings.get(node2.id)
        
        if emb1 and emb2:
            return self.compute_similarity(emb1, emb2)
        
        return 0.5  # Default neutral weight
    
    def clear_cache(self):
        """Clear the embedding cache."""
        self._embedding_cache.clear()
        logger.info("Vector scorer cache cleared")
