"""
Tests for vector reranking functionality.
"""
import unittest
import numpy as np
from src.rag.mongo_vectors import VectorReranker


class MockMongoDBHandler:
    """Mock MongoDB handler for testing."""
    
    def __init__(self):
        self.data = {
            'email1': {
                'id': 'email1',
                'embedding': [1.0, 0.0, 0.0],
                'metadata': {'subject': 'Test 1'}
            },
            'email2': {
                'id': 'email2',
                'embedding': [0.0, 1.0, 0.0],
                'metadata': {'subject': 'Test 2'}
            },
            'email3': {
                'id': 'email3',
                'embedding': [0.7, 0.7, 0.0],  # Similar to query
                'metadata': {'subject': 'Test 3'}
            },
        }
        self.collection = self
    
    def find_one(self, query):
        """Mock find_one method."""
        email_id = query.get('id')
        return self.data.get(email_id)


class TestVectorReranker(unittest.TestCase):
    """Test vector reranking functionality."""
    
    def setUp(self):
        """Set up test reranker."""
        self.mock_mongo = MockMongoDBHandler()
        self.reranker = VectorReranker(self.mock_mongo, embedding_model=None)
    
    def test_cosine_similarity(self):
        """Test cosine similarity calculation."""
        vec1 = [1.0, 0.0, 0.0]
        vec2 = [1.0, 0.0, 0.0]
        
        similarity = self.reranker._cosine_similarity(vec1, vec2)
        self.assertAlmostEqual(similarity, 1.0, places=5)
        
        vec3 = [0.0, 1.0, 0.0]
        similarity = self.reranker._cosine_similarity(vec1, vec3)
        self.assertAlmostEqual(similarity, 0.0, places=5)
    
    def test_rerank(self):
        """Test reranking emails by similarity."""
        email_ids = ['email1', 'email2', 'email3']
        query_embedding = [0.8, 0.8, 0.0]  # Similar to email3
        
        results = self.reranker.rerank(email_ids, query_embedding, top_k=2)
        
        # Should return top 2 most similar
        self.assertEqual(len(results), 2)
        
        # email3 should be most similar
        self.assertEqual(results[0]['id'], 'email3')
        self.assertGreater(results[0]['similarity'], results[1]['similarity'])
    
    def test_rerank_empty_list(self):
        """Test reranking with empty email list."""
        results = self.reranker.rerank([], [1.0, 0.0, 0.0])
        self.assertEqual(len(results), 0)


if __name__ == '__main__':
    unittest.main()

