"""
Tests for query service.
"""
import unittest
from src.rag.query_service import QueryService


class MockEmailSearcher:
    """Mock email searcher for testing."""
    
    def search(self, query, top_k=10):
        """Mock search method."""
        return [
            {
                'id': 'email1',
                'subject': 'Test email',
                'sender_name': 'John Doe',
                'sender_email': 'john@example.com',
                'received_time': '2024-01-01T00:00:00',
                'body': 'This is a test email body.',
                'rank': -0.5
            }
        ]


class MockVectorReranker:
    """Mock vector reranker for testing."""
    
    def __init__(self):
        self.embedding_model = None
    
    def embed_query(self, query):
        """Mock embed query."""
        return None
    
    def rerank(self, email_ids, query_embedding, top_k=None):
        """Mock rerank."""
        return []


class MockSarvamClient:
    """Mock Sarvam client for testing."""
    
    def __init__(self):
        pass


class TestQueryService(unittest.TestCase):
    """Test query service functionality."""
    
    def setUp(self):
        """Set up test query service."""
        self.searcher = MockEmailSearcher()
        self.reranker = MockVectorReranker()
        self.sarvam = MockSarvamClient()
        
        self.service = QueryService(
            email_searcher=self.searcher,
            vector_reranker=self.reranker,
            sarvam_client=self.sarvam,
            enable_vector_rerank=False
        )
    
    def test_build_context(self):
        """Test context building from emails."""
        emails = [
            {
                'subject': 'Test',
                'sender_name': 'John',
                'sender_email': 'john@example.com',
                'received_time': '2024-01-01',
                'body': 'Test body'
            }
        ]
        
        context = self.service._build_context(emails)
        
        self.assertIn('Test', context)
        self.assertIn('John', context)
        self.assertIn('Test body', context)
    
    def test_build_prompt(self):
        """Test prompt building."""
        question = "What is the pricing?"
        context = "EMAIL 1: Pricing is $100"
        
        prompt = self.service._build_prompt(question, context)
        
        self.assertIn(question, prompt)
        self.assertIn(context, prompt)
        self.assertIn("ONLY using information from the emails", prompt)
    
    def test_build_citations(self):
        """Test citation building."""
        emails = [
            {
                'id': 'email1',
                'subject': 'Test',
                'sender_name': 'John',
                'sender_email': 'john@example.com',
                'received_time': '2024-01-01',
                'body': 'Test body with some content'
            }
        ]
        
        citations = self.service._build_citations(emails, [])
        
        self.assertEqual(len(citations), 1)
        self.assertEqual(citations[0]['id'], 'email1')
        self.assertEqual(citations[0]['subject'], 'Test')
        self.assertIn('snippet', citations[0])


if __name__ == '__main__':
    unittest.main()

