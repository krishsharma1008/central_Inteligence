"""
SQLite FTS5 search helpers for email retrieval.
"""
import logging
from typing import List, Dict, Any
from src.SQLiteHandler import SQLiteHandler

logger = logging.getLogger('outlook-email.rag.sqlite')


class EmailSearcher:
    """Helper class for searching emails using SQLite FTS5."""
    
    def __init__(self, sqlite_handler: SQLiteHandler):
        """
        Initialize the email searcher.
        
        Args:
            sqlite_handler (SQLiteHandler): SQLite database handler
        """
        self.sqlite = sqlite_handler
    
    def search(self, query: str, top_k: int = 10) -> List[Dict[str, Any]]:
        """
        Search emails using FTS5 full-text search.
        
        Args:
            query (str): Search query
            top_k (int): Number of results to return
            
        Returns:
            List[Dict[str, Any]]: List of matching emails
        """
        logger.info(f"Searching emails with query: '{query}', top_k: {top_k}")
        
        # Sanitize query for FTS5 (basic approach)
        # FTS5 supports: AND, OR, NOT, NEAR, phrase queries "..."
        # For now, we'll use the query as-is and let FTS5 handle it
        fts_query = query.strip()
        
        # If query is empty, return empty results
        if not fts_query:
            logger.warning("Empty query provided")
            return []
        
        # Perform FTS search
        results = self.sqlite.search_emails_fts(fts_query, limit=top_k)
        
        logger.info(f"Found {len(results)} emails matching query")
        return results
    
    def search_with_keywords(self, keywords: List[str], top_k: int = 10) -> List[Dict[str, Any]]:
        """
        Search emails using a list of keywords (OR query).
        
        Args:
            keywords (List[str]): List of keywords to search for
            top_k (int): Number of results to return
            
        Returns:
            List[Dict[str, Any]]: List of matching emails
        """
        if not keywords:
            return []
        
        # Build FTS5 OR query
        fts_query = " OR ".join(keywords)
        return self.search(fts_query, top_k)
    
    def search_phrase(self, phrase: str, top_k: int = 10) -> List[Dict[str, Any]]:
        """
        Search for an exact phrase in emails.
        
        Args:
            phrase (str): Exact phrase to search for
            top_k (int): Number of results to return
            
        Returns:
            List[Dict[str, Any]]: List of matching emails
        """
        if not phrase:
            return []
        
        # Wrap phrase in quotes for exact match
        fts_query = f'"{phrase}"'
        return self.search(fts_query, top_k)
    
    def get_thread_emails(self, conversation_id: str) -> List[Dict[str, Any]]:
        """
        Get all emails in a conversation thread.
        
        Args:
            conversation_id (str): Conversation ID
            
        Returns:
            List[Dict[str, Any]]: List of emails in the thread
        """
        if not conversation_id:
            return []
        return self.sqlite.get_emails_by_conversation_id(conversation_id)

