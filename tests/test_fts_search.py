"""
Tests for FTS search functionality.
"""
import unittest
import tempfile
import os
from src.SQLiteHandler import SQLiteHandler
from src.rag.sqlite_search import EmailSearcher
from src.EmailMetadata import EmailMetadata
from datetime import datetime


class TestFTSSearch(unittest.TestCase):
    """Test FTS search functionality."""
    
    def setUp(self):
        """Set up test database with sample emails."""
        # Create temporary database
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.temp_db.close()
        
        self.sqlite = SQLiteHandler(self.temp_db.name)
        self.searcher = EmailSearcher(self.sqlite)
        
        # Add sample emails
        self.sample_emails = [
            EmailMetadata(
                AccountName="test@example.com",
                Entry_ID="email1",
                Folder="Inbox",
                Subject="Sales meeting with client",
                SenderName="John Doe",
                SenderEmailAddress="john@example.com",
                ReceivedTime=datetime.now(),
                SentOn=datetime.now(),
                To="test@example.com",
                Body="We need to discuss the pricing for 100 seats. The client wants to know the timeline.",
                Attachments=[],
                IsMarkedAsTask=False,
                UnRead=False,
                Categories=""
            ),
            EmailMetadata(
                AccountName="test@example.com",
                Entry_ID="email2",
                Folder="Inbox",
                Subject="HR policy update",
                SenderName="Jane Smith",
                SenderEmailAddress="jane@example.com",
                ReceivedTime=datetime.now(),
                SentOn=datetime.now(),
                To="test@example.com",
                Body="Please review the updated leave policy and holiday calendar.",
                Attachments=[],
                IsMarkedAsTask=False,
                UnRead=False,
                Categories=""
            ),
            EmailMetadata(
                AccountName="test@example.com",
                Entry_ID="email3",
                Folder="Inbox",
                Subject="Server maintenance",
                SenderName="Ops Team",
                SenderEmailAddress="ops@example.com",
                ReceivedTime=datetime.now(),
                SentOn=datetime.now(),
                To="test@example.com",
                Body="Server maintenance window scheduled for Friday 2am.",
                Attachments=[],
                IsMarkedAsTask=False,
                UnRead=False,
                Categories=""
            ),
        ]
        
        for email in self.sample_emails:
            self.sqlite.add_or_update_email(email)
        
        # Rebuild FTS index
        self.sqlite.rebuild_fts_index()
    
    def tearDown(self):
        """Clean up test database."""
        self.sqlite.close()
        os.unlink(self.temp_db.name)
    
    def test_search_by_subject(self):
        """Test searching by subject."""
        results = self.searcher.search("sales", top_k=10)
        self.assertGreater(len(results), 0)
        self.assertEqual(results[0]['id'], 'email1')
    
    def test_search_by_body(self):
        """Test searching by body content."""
        results = self.searcher.search("pricing timeline", top_k=10)
        self.assertGreater(len(results), 0)
        # Should find the sales email
        email_ids = [r['id'] for r in results]
        self.assertIn('email1', email_ids)
    
    def test_search_no_results(self):
        """Test search with no matching results."""
        results = self.searcher.search("nonexistent keyword xyz", top_k=10)
        self.assertEqual(len(results), 0)
    
    def test_search_phrase(self):
        """Test exact phrase search."""
        results = self.searcher.search_phrase("leave policy", top_k=10)
        self.assertGreater(len(results), 0)
        self.assertEqual(results[0]['id'], 'email2')
    
    def test_top_k_limit(self):
        """Test that top_k limits results."""
        results = self.searcher.search("email", top_k=2)
        self.assertLessEqual(len(results), 2)


if __name__ == '__main__':
    unittest.main()



