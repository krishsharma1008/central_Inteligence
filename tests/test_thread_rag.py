"""
Tests for thread-aware RAG functionality.
"""
import unittest
from unittest.mock import Mock, MagicMock
from datetime import datetime
from src.rag.query_service import QueryService
from src.EmailMetadata import EmailMetadata


class MockEmailSearcher:
    """Mock email searcher with thread support."""
    
    def __init__(self):
        self.threads = {}
    
    def search(self, query, top_k=10):
        """Mock search that returns emails with conversation_id."""
        return [
            {
                'id': 'email1',
                'subject': 'Hepstart Project Discussion',
                'sender_name': 'Alice',
                'sender_email': 'alice@example.com',
                'received_time': '2024-01-01T10:00:00',
                'body': 'We need to discuss the Hepstart project timeline.',
                'conversation_id': 'conv123',
                'rank': 0.1
            },
            {
                'id': 'email2',
                'subject': 'Re: Hepstart Project Discussion',
                'sender_name': 'Bob',
                'sender_email': 'bob@example.com',
                'received_time': '2024-01-01T11:00:00',
                'body': 'The Hepstart project is on track for Q2 delivery.',
                'conversation_id': 'conv123',
                'rank': 0.2
            },
            {
                'id': 'email3',
                'subject': 'Re: Hepstart Project Discussion',
                'sender_name': 'Charlie',
                'sender_email': 'charlie@example.com',
                'received_time': '2024-01-01T12:00:00',
                'body': 'Hepstart integration is complete. Ready for testing.',
                'conversation_id': 'conv123',
                'rank': 0.3
            }
        ]
    
    def get_thread_emails(self, conversation_id):
        """Mock thread retrieval."""
        if conversation_id == 'conv123':
            return [
                {
                    'id': 'email1',
                    'subject': 'Hepstart Project Discussion',
                    'sender_name': 'Alice',
                    'sender_email': 'alice@example.com',
                    'received_time': '2024-01-01T10:00:00',
                    'body': 'We need to discuss the Hepstart project timeline.',
                    'conversation_id': 'conv123'
                },
                {
                    'id': 'email2',
                    'subject': 'Re: Hepstart Project Discussion',
                    'sender_name': 'Bob',
                    'sender_email': 'bob@example.com',
                    'received_time': '2024-01-01T11:00:00',
                    'body': 'The Hepstart project is on track for Q2 delivery.',
                    'conversation_id': 'conv123'
                },
                {
                    'id': 'email3',
                    'subject': 'Re: Hepstart Project Discussion',
                    'sender_name': 'Charlie',
                    'sender_email': 'charlie@example.com',
                    'received_time': '2024-01-01T12:00:00',
                    'body': 'Hepstart integration is complete. Ready for testing.',
                    'conversation_id': 'conv123'
                }
            ]
        return []


class MockVectorReranker:
    """Mock vector reranker."""
    
    def __init__(self):
        self.embedding_model = None
    
    def embed_query(self, query):
        return None
    
    def rerank(self, email_ids, query_embedding, top_k=None):
        return []


class MockSarvamClient:
    """Mock Sarvam client."""
    
    def __init__(self):
        pass


class TestThreadRAG(unittest.TestCase):
    """Test thread-aware RAG functionality."""
    
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
    
    def test_enhanced_query_building(self):
        """Test enhanced query building for better recall."""
        question = "what happened with hepstart"
        
        enhanced = self.service._build_enhanced_query(question)
        
        # Should extract keywords and build OR query
        self.assertIsInstance(enhanced, str)
        self.assertIn('hepstart', enhanced.lower())
    
    def test_thread_grouping(self):
        """Test that emails are grouped by conversation_id."""
        emails = [
            {'id': 'e1', 'conversation_id': 'conv1', 'rank': 0.1},
            {'id': 'e2', 'conversation_id': 'conv1', 'rank': 0.2},
            {'id': 'e3', 'conversation_id': 'conv2', 'rank': 0.3},
            {'id': 'e4', 'conversation_id': None, 'rank': 0.4}
        ]
        
        from collections import defaultdict
        threads = defaultdict(list)
        for email in emails:
            conv_id = email.get('conversation_id')
            if conv_id:
                threads[conv_id].append(email)
            else:
                threads[email['id']].append(email)
        
        # Should have 3 groups: conv1, conv2, and e4
        self.assertEqual(len(threads), 3)
        self.assertEqual(len(threads['conv1']), 2)
        self.assertEqual(len(threads['conv2']), 1)
        self.assertEqual(len(threads['e4']), 1)
    
    def test_thread_context_building(self):
        """Test thread-aware context building."""
        emails = [
            {
                'id': 'email1',
                'subject': 'Hepstart Discussion',
                'sender_name': 'Alice',
                'sender_email': 'alice@example.com',
                'received_time': '2024-01-01T10:00:00',
                'body': 'First message about Hepstart.',
                'conversation_id': 'conv123'
            },
            {
                'id': 'email2',
                'subject': 'Re: Hepstart Discussion',
                'sender_name': 'Bob',
                'sender_email': 'bob@example.com',
                'received_time': '2024-01-01T11:00:00',
                'body': 'Second message in the thread.',
                'conversation_id': 'conv123'
            }
        ]
        
        thread_metadata = {
            'conv123': {
                'count': 2,
                'subject': 'Hepstart Discussion'
            }
        }
        
        context = self.service._build_thread_context(emails, thread_metadata)
        
        # Should include thread structure
        self.assertIn('THREAD', context)
        self.assertIn('Hepstart Discussion', context)
        self.assertIn('Message 1', context)
        self.assertIn('Message 2', context)
        self.assertIn('First message', context)
        self.assertIn('Second message', context)
    
    def test_email_metadata_conversation_fields(self):
        """Test that EmailMetadata includes conversation fields."""
        email = EmailMetadata(
            AccountName="test@example.com",
            Entry_ID="msg123",
            Folder="Inbox",
            Subject="Test",
            SenderName="Test User",
            SenderEmailAddress="test@example.com",
            ReceivedTime=datetime.now(),
            SentOn=datetime.now(),
            To="recipient@example.com",
            Body="Test body",
            Attachments=[],
            IsMarkedAsTask=False,
            UnRead=False,
            Categories="",
            ConversationId="conv123",
            ConversationIndex="0",
            InternetMessageId="<msg123@example.com>"
        )
        
        self.assertEqual(email.ConversationId, "conv123")
        self.assertEqual(email.ConversationIndex, "0")
        self.assertEqual(email.InternetMessageId, "<msg123@example.com>")
        
        # Test serialization
        email_dict = email.to_dict()
        self.assertIn('ConversationId', email_dict)
        self.assertEqual(email_dict['ConversationId'], "conv123")


class TestThreadRetrieval(unittest.TestCase):
    """Test thread retrieval functionality."""
    
    def setUp(self):
        """Set up test."""
        self.searcher = MockEmailSearcher()
    
    def test_get_thread_emails(self):
        """Test retrieving all emails in a thread."""
        thread_emails = self.searcher.get_thread_emails('conv123')
        
        self.assertEqual(len(thread_emails), 3)
        self.assertEqual(thread_emails[0]['id'], 'email1')
        self.assertEqual(thread_emails[1]['id'], 'email2')
        self.assertEqual(thread_emails[2]['id'], 'email3')
        
        # All should have same conversation_id
        for email in thread_emails:
            self.assertEqual(email['conversation_id'], 'conv123')


if __name__ == '__main__':
    unittest.main()

