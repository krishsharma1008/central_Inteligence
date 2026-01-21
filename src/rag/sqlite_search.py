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
    
    def unified_search(self, query: str, top_k: int = 10) -> List[Dict[str, Any]]:
        """
        Search both emails and attachments, returning unified results grouped by conversation.
        
        Args:
            query (str): Search query
            top_k (int): Number of top results to return
            
        Returns:
            List[Dict[str, Any]]: List of results with conversation IDs and metadata
        """
        logger.info(f"Unified search for: '{query}', top_k: {top_k}")
        
        # Search emails with FTS
        email_results = self.search(query, top_k=top_k * 2)
        
        # Search attachments with FTS
        attachment_results = self.sqlite.search_attachments(query, top_k=top_k * 2)
        
        # Build conversation map with scores
        conv_map = {}  # conv_id -> {best_rank, source, email_id, attachment_info}
        
        # Process email results
        for email in email_results:
            conv_id = email.get('conversation_id')
            if not conv_id:
                # Use email ID as conversation for emails without conv_id
                conv_id = email['id']
            
            rank = email.get('rank', 999999)
            if conv_id not in conv_map or rank < conv_map[conv_id]['best_rank']:
                conv_map[conv_id] = {
                    'conversation_id': conv_id,
                    'best_rank': rank,
                    'source': 'email',
                    'email_id': email['id'],
                    'subject': email.get('subject', 'No Subject'),
                    'sender_name': email.get('sender_name', ''),
                    'received_time': email.get('received_time', ''),
                    'has_attachments': False
                }
        
        # Process attachment results
        for att in attachment_results:
            email_id = att.get('email_id')
            if email_id:
                # Get parent email to find conversation
                email = self.sqlite.get_email_by_id(email_id)
                if email:
                    conv_id = email.get('conversation_id') or email_id
                    rank = att.get('rank', 999999)
                    
                    if conv_id not in conv_map or rank < conv_map[conv_id]['best_rank']:
                        conv_map[conv_id] = {
                            'conversation_id': conv_id,
                            'best_rank': rank,
                            'source': 'attachment',
                            'email_id': email_id,
                            'subject': email.get('subject', 'No Subject'),
                            'sender_name': email.get('sender_name', ''),
                            'received_time': email.get('received_time', ''),
                            'attachment_filename': att.get('filename', ''),
                            'has_attachments': True
                        }
                    else:
                        # Update existing entry to mark has_attachments
                        conv_map[conv_id]['has_attachments'] = True
        
        # Sort by best rank and return top_k
        results = sorted(conv_map.values(), key=lambda x: x['best_rank'])[:top_k]
        
        logger.info(f"Unified search returned {len(results)} conversation groups")
        return results

