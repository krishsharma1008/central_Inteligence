import sqlite3
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
import json
import sys
import logging
import time
import os
from src.EmailMetadata import EmailMetadata

import logging

# Configure logging
logger = logging.getLogger('outlook-email.sqlite')

class SQLiteHandler:
    def __init__(self, db_path: str) -> None:
        """
        Initialize SQLite database connection and create tables if they don't exist.
        
        Args:
            db_path (str): Path to SQLite database file
        """
        try:
            logger.info(f"Initializing SQLite at {db_path}")
            self.db_path = db_path
            self.conn = self._create_connection()
            self.conn.row_factory = sqlite3.Row
            self._create_tables()
            logger.info("SQLite initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing SQLite: {str(e)}", exc_info=True)
            raise

    def _create_connection(self, max_retries: int = 3) -> sqlite3.Connection:
        """Create database connection with retry logic."""
        # Ensure directory exists
        db_dir = os.path.dirname(self.db_path)
        if not os.path.exists(db_dir):
            logger.info(f"Creating directory: {db_dir}")
            os.makedirs(db_dir, exist_ok=True)
            
        for attempt in range(max_retries):
            try:
                # Use isolation_level with a value instead of None to avoid autocommit mode
                # which can cause locking issues
                return sqlite3.connect(
                    self.db_path,
                    timeout=30.0,  # 30 second timeout
                    isolation_level="IMMEDIATE"  # Use explicit transactions instead of autocommit
                )
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                logger.warning(f"Retry {attempt + 1}/{max_retries} connecting to SQLite: {str(e)}")
                time.sleep(1)

    def _create_tables(self) -> None:
        """Create necessary database tables if they don't exist."""
        cursor = self.conn.cursor()
        
        # Create main emails table (no drop - preserve existing data)
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS emails (
            id TEXT PRIMARY KEY,
            account TEXT NOT NULL,
            folder TEXT NOT NULL,
            subject TEXT,
            sender_name TEXT,
            sender_email TEXT,
            received_time DATETIME,
            sent_time DATETIME,
            recipients TEXT,
            is_task BOOLEAN,
            unread BOOLEAN,
            categories TEXT,
            processed BOOLEAN DEFAULT FALSE,
            last_updated DATETIME,
            body TEXT,
            attachments TEXT
        )
        ''')
        
        # Migrate: Add thread columns if they don't exist (backward compatible)
        try:
            cursor.execute('ALTER TABLE emails ADD COLUMN conversation_id TEXT')
        except sqlite3.OperationalError:
            pass  # Column already exists
        try:
            cursor.execute('ALTER TABLE emails ADD COLUMN conversation_index TEXT')
        except sqlite3.OperationalError:
            pass
        try:
            cursor.execute('ALTER TABLE emails ADD COLUMN internet_message_id TEXT')
        except sqlite3.OperationalError:
            pass

        # Create optimized indices (IF NOT EXISTS implicit in SQLite for index creation errors)
        try:
            cursor.execute('CREATE INDEX idx_folder ON emails(folder)')
        except sqlite3.OperationalError:
            pass  # Index already exists
        try:
            cursor.execute('CREATE INDEX idx_received_time ON emails(received_time)')
        except sqlite3.OperationalError:
            pass
        try:
            cursor.execute('CREATE INDEX idx_processed ON emails(processed)')
        except sqlite3.OperationalError:
            pass
        try:
            cursor.execute('CREATE INDEX idx_conversation_id ON emails(conversation_id)')
        except sqlite3.OperationalError:
            pass
        try:
            cursor.execute('CREATE INDEX idx_conversation_time ON emails(conversation_id, received_time)')
        except sqlite3.OperationalError:
            pass
        
        # Create metadata table for storing sync state, delta links, etc.
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # Create FTS5 virtual table for full-text search
        cursor.execute('''
        CREATE VIRTUAL TABLE IF NOT EXISTS emails_fts USING fts5(
            id UNINDEXED,
            subject,
            body,
            sender_name,
            sender_email,
            recipients,
            content='emails',
            content_rowid='rowid'
        )
        ''')
        
        # Create triggers to keep FTS index in sync with emails table
        cursor.execute('''
        CREATE TRIGGER IF NOT EXISTS emails_ai AFTER INSERT ON emails BEGIN
            INSERT INTO emails_fts(rowid, id, subject, body, sender_name, sender_email, recipients)
            VALUES (new.rowid, new.id, new.subject, new.body, new.sender_name, new.sender_email, new.recipients);
        END
        ''')
        
        cursor.execute('''
        CREATE TRIGGER IF NOT EXISTS emails_ad AFTER DELETE ON emails BEGIN
            INSERT INTO emails_fts(emails_fts, rowid, id, subject, body, sender_name, sender_email, recipients)
            VALUES('delete', old.rowid, old.id, old.subject, old.body, old.sender_name, old.sender_email, old.recipients);
        END
        ''')
        
        cursor.execute('''
        CREATE TRIGGER IF NOT EXISTS emails_au AFTER UPDATE ON emails BEGIN
            INSERT INTO emails_fts(emails_fts, rowid, id, subject, body, sender_name, sender_email, recipients)
            VALUES('delete', old.rowid, old.id, old.subject, old.body, old.sender_name, old.sender_email, old.recipients);
            INSERT INTO emails_fts(rowid, id, subject, body, sender_name, sender_email, recipients)
            VALUES (new.rowid, new.id, new.subject, new.body, new.sender_name, new.sender_email, new.recipients);
        END
        ''')
        
        self.conn.commit()

    def add_or_update_email(self, email: EmailMetadata, cursor: Optional[sqlite3.Cursor] = None) -> bool:
        """
        Add or update an email in the database.
        
        Args:
            email (EmailMetadata): Email metadata to store
            cursor (Optional[sqlite3.Cursor]): Optional cursor for transaction management
            
        Returns:
            bool: True if successful
        """
        try:
            # Use provided cursor or create new one
            cursor = cursor or self.conn.cursor()
            
            # Convert email to dict
            try:
                email_dict = email.to_dict()
                logger.debug(f"Processing email: {email_dict.get('Subject', 'No Subject')}")
            except Exception as e:
                logger.error(f"Error converting email to dict: {str(e)}")
                return False
            
            try:
                # Prepare data for insertion/update
                # Convert datetime objects to ISO format strings
                received_time = email_dict.get('ReceivedTime')
                sent_time = email_dict.get('SentOn')
                
                if isinstance(received_time, datetime):
                    received_time = received_time.isoformat()
                if isinstance(sent_time, datetime):
                    sent_time = sent_time.isoformat()
                
                data = {
                    'id': email_dict.get('Entry_ID'),
                    'account': email_dict.get('AccountName'),
                    'folder': email_dict.get('Folder'),
                    'subject': email_dict.get('Subject'),
                    'sender_name': email_dict.get('SenderName'),
                    'sender_email': email_dict.get('SenderEmailAddress'),
                    'received_time': received_time,
                    'sent_time': sent_time,
                    'recipients': email_dict.get('To'),
                    'is_task': bool(email_dict.get('IsMarkedAsTask')),
                    'unread': bool(email_dict.get('UnRead')),
                    'categories': email_dict.get('Categories'),
                    'processed': bool(email_dict.get('embedding')),
                    'last_updated': datetime.now().isoformat(),
                    'body': email_dict.get('Body'),
                    'attachments': email_dict.get('Attachments', ''),
                    'conversation_id': email_dict.get('ConversationId', '') or None,
                    'conversation_index': email_dict.get('ConversationIndex', '') or None,
                    'internet_message_id': email_dict.get('InternetMessageId', '') or None
                }
                
                # Validate required fields
                required_fields = ['id', 'account', 'folder', 'subject', 'received_time', 'body']
                missing_fields = [field for field in required_fields if not data[field]]
                if missing_fields:
                    logger.warning(f"Missing required fields: {', '.join(missing_fields)}")
                    return False
                
            except Exception as e:
                logger.error(f"Error preparing data for SQLite: {str(e)}")
                return False
            
            # Use UPSERT syntax with retry logic
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    # Check if email exists in a transaction
                    cursor.execute('BEGIN IMMEDIATE')
                    cursor.execute('SELECT id FROM emails WHERE id = ?', (data['id'],))
                    exists = cursor.fetchone() is not None
                    
                    if exists:
                        logger.info(f"Email {data['id']} already exists, skipping")
                        cursor.execute('COMMIT')
                        return True
                    
                    # Insert new email
                    cursor.execute('''
                    INSERT INTO emails (
                        id, account, folder, subject, sender_name, sender_email,
                        received_time, sent_time, recipients, is_task, unread,
                        categories, processed, last_updated, body, attachments,
                        conversation_id, conversation_index, internet_message_id
                    ) VALUES (
                        :id, :account, :folder, :subject, :sender_name, :sender_email,
                        :received_time, :sent_time, :recipients, :is_task, :unread,
                        :categories, :processed, :last_updated, :body, :attachments,
                        :conversation_id, :conversation_index, :internet_message_id
                    )
                    ''', data)
                    
                    cursor.execute('COMMIT')
                    logger.info(f"Successfully added email {data['id']}")
                    return True
                    
                except sqlite3.OperationalError as e:
                    cursor.execute('ROLLBACK')
                    if "database is locked" in str(e) and attempt < max_retries - 1:
                        logger.warning(f"Database locked, retry {attempt + 1}/{max_retries}")
                        time.sleep(1)
                        continue
                    logger.error(f"SQLite operational error: {str(e)}")
                    raise
                except Exception as e:
                    cursor.execute('ROLLBACK')
                    logger.error(f"Unexpected error: {str(e)}")
                    raise
                    
                except sqlite3.OperationalError as e:
                    if "database is locked" in str(e) and attempt < max_retries - 1:
                        logger.warning(f"Database locked, retry {attempt + 1}/{max_retries}")
                        time.sleep(1)
                        continue
                    raise
                    
        except Exception as e:
            logger.error(f"Error adding/updating email: {str(e)}", exc_info=True)
            self.conn.rollback()
            return False

    def get_unprocessed_emails(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get emails that haven't been processed (no embeddings generated).
        
        Args:
            limit (int): Maximum number of emails to return
            
        Returns:
            List[Dict]: List of unprocessed emails
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute('''
            SELECT 
                id,
                account as AccountName,
                folder as Folder,
                subject as Subject,
                sender_name as SenderName,
                sender_email as SenderEmailAddress,
                received_time as ReceivedTime,
                sent_time as SentOn,
                recipients as "To",
                body as Body,
                COALESCE(attachments, '') as Attachments,
                is_task as IsMarkedAsTask,
                unread as UnRead,
                categories as Categories
            FROM emails 
            WHERE processed = FALSE 
            ORDER BY received_time DESC 
            LIMIT ?
            ''', (limit,))
            
            return [dict(row) for row in cursor.fetchall()]
            
        except Exception as e:
            logger.error(f"Error getting unprocessed emails: {str(e)}", exc_info=True)
            return []

    def mark_as_processed(self, email_id: str) -> bool:
        """
        Mark an email as processed after generating its embedding.
        
        Args:
            email_id (str): ID of the email to mark
            
        Returns:
            bool: True if successful
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute('''
            UPDATE emails 
            SET processed = TRUE, 
                last_updated = ? 
            WHERE id = ?
            ''', (datetime.now().isoformat(), email_id))
            
            self.conn.commit()
            return True
            
        except Exception as e:
            logger.error(f"Error marking email as processed: {str(e)}", exc_info=True)
            self.conn.rollback()
            return False

    def get_email_by_id(self, email_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a specific email by ID.
        
        Args:
            email_id (str): ID of the email to retrieve
            
        Returns:
            Optional[Dict]: Email data if found
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute('SELECT * FROM emails WHERE id = ?', (email_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
            
        except Exception as e:
            logger.error(f"Error getting email by ID: {str(e)}", exc_info=True)
            return None

    def get_email_count(self) -> int:
        """
        Get total number of emails in database.
        
        Returns:
            int: Number of emails
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute('SELECT COUNT(*) FROM emails')
            return cursor.fetchone()[0]
            
        except Exception as e:
            logger.error(f"Error getting email count: {str(e)}", exc_info=True)
            return 0
    
    def search_emails_fts(self, query: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Search emails using FTS5 full-text search.
        Handles case-insensitive search by normalizing query terms.
        
        Args:
            query (str): Search query (FTS5 syntax supported)
            limit (int): Maximum number of results to return
            
        Returns:
            List[Dict[str, Any]]: List of matching emails with rank scores
        """
        try:
            cursor = self.conn.cursor()
            
            # Normalize query for case-insensitive search
            # FTS5 is case-sensitive, so we need to handle this
            # For simple queries, try both case variations
            normalized_query = self._normalize_fts_query(query)
            
            # Use FTS5 MATCH with rank scoring
            cursor.execute('''
            SELECT 
                e.id,
                e.account,
                e.folder,
                e.subject,
                e.sender_name,
                e.sender_email,
                e.received_time,
                e.sent_time,
                e.recipients,
                e.body,
                e.attachments,
                e.categories,
                e.is_task,
                e.unread,
                e.conversation_id,
                fts.rank
            FROM emails_fts fts
            INNER JOIN emails e ON e.rowid = fts.rowid
            WHERE emails_fts MATCH ?
            ORDER BY rank
            LIMIT ?
            ''', (normalized_query, limit))
            
            results = []
            for row in cursor.fetchall():
                results.append({
                    'id': row[0],
                    'account': row[1],
                    'folder': row[2],
                    'subject': row[3],
                    'sender_name': row[4],
                    'sender_email': row[5],
                    'received_time': row[6],
                    'sent_time': row[7],
                    'recipients': row[8],
                    'body': row[9],
                    'attachments': row[10],
                    'categories': row[11],
                    'is_task': row[12],
                    'unread': row[13],
                    'conversation_id': row[14],
                    'rank': row[15]
                })
            
            logger.info(f"FTS search for '{query}' returned {len(results)} results")
            
            # If FTS returns no results, fallback to LIKE search for case-insensitive matching
            if len(results) == 0:
                logger.info("FTS returned no results, trying case-insensitive LIKE fallback")
                return self._fallback_like_search(query, limit)
            
            return results
            
        except Exception as e:
            logger.error(f"Error performing FTS search: {str(e)}", exc_info=True)
            # Try fallback on error too
            try:
                return self._fallback_like_search(query, limit)
            except Exception as e2:
                logger.error(f"Error in fallback LIKE search: {str(e2)}", exc_info=True)
                return []
    
    def _fallback_like_search(self, query: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Fallback case-insensitive LIKE search when FTS5 returns no results.
        
        Args:
            query (str): Search query
            limit (int): Maximum number of results to return
            
        Returns:
            List[Dict[str, Any]]: List of matching emails
        """
        try:
            cursor = self.conn.cursor()
            
            # Extract search terms (remove FTS operators)
            import re
            # Extract words from query, removing operators
            terms = re.findall(r'\b\w+\b', query.lower())
            if not terms:
                return []
            
            # Build LIKE conditions for subject and body
            like_conditions = []
            params = []
            for term in terms:
                like_conditions.append('(LOWER(subject) LIKE ? OR LOWER(body) LIKE ?)')
                params.extend([f'%{term}%', f'%{term}%'])
            
            where_clause = ' OR '.join(like_conditions)
            
            cursor.execute(f'''
            SELECT 
                id, account, folder, subject, sender_name, sender_email,
                received_time, sent_time, recipients, body, attachments,
                categories, is_task, unread, conversation_id
            FROM emails
            WHERE {where_clause}
            ORDER BY received_time DESC
            LIMIT ?
            ''', params + [limit])
            
            results = []
            for row in cursor.fetchall():
                results.append({
                    'id': row[0],
                    'account': row[1],
                    'folder': row[2],
                    'subject': row[3],
                    'sender_name': row[4],
                    'sender_email': row[5],
                    'received_time': row[6],
                    'sent_time': row[7],
                    'recipients': row[8],
                    'body': row[9],
                    'attachments': row[10],
                    'categories': row[11],
                    'is_task': row[12],
                    'unread': row[13],
                    'conversation_id': row[14],
                    'rank': 0.0  # No rank for LIKE search
                })
            
            logger.info(f"LIKE fallback search returned {len(results)} results")
            return results
            
        except Exception as e:
            logger.error(f"Error in fallback LIKE search: {str(e)}", exc_info=True)
            return []
    
    def get_emails_by_conversation_id(self, conversation_id: str) -> List[Dict[str, Any]]:
        """
        Get all emails in a conversation thread, ordered by received_time.
        
        Args:
            conversation_id (str): Conversation ID to fetch
            
        Returns:
            List[Dict[str, Any]]: List of emails in the conversation
        """
        if not conversation_id:
            return []
            
        try:
            cursor = self.conn.cursor()
            cursor.execute('''
            SELECT 
                id, account, folder, subject, sender_name, sender_email,
                received_time, sent_time, recipients, body, attachments,
                categories, is_task, unread, conversation_id, conversation_index,
                internet_message_id
            FROM emails
            WHERE conversation_id = ?
            ORDER BY received_time ASC
            ''', (conversation_id,))
            
            results = []
            for row in cursor.fetchall():
                results.append({
                    'id': row[0],
                    'account': row[1],
                    'folder': row[2],
                    'subject': row[3],
                    'sender_name': row[4],
                    'sender_email': row[5],
                    'received_time': row[6],
                    'sent_time': row[7],
                    'recipients': row[8],
                    'body': row[9],
                    'attachments': row[10],
                    'categories': row[11],
                    'is_task': row[12],
                    'unread': row[13],
                    'conversation_id': row[14],
                    'conversation_index': row[15],
                    'internet_message_id': row[16]
                })
            
            logger.info(f"Found {len(results)} emails in conversation {conversation_id}")
            return results
            
        except Exception as e:
            logger.error(f"Error fetching emails by conversation_id: {str(e)}", exc_info=True)
            return []

    def _normalize_fts_query(self, query: str) -> str:
        """
        Normalize FTS query for better case-insensitive matching.
        FTS5 is case-sensitive, so we expand queries to include case variations.
        
        Args:
            query (str): Original query
            
        Returns:
            str: Normalized query with case variations
        """
        import re
        
        # If query contains OR/AND/NOT operators, handle them carefully
        if any(op in query.upper() for op in [' OR ', ' AND ', ' NOT ']):
            # For complex queries, try to normalize individual terms
            # Split by operators while preserving them
            parts = re.split(r'(\s+(?:OR|AND|NOT)\s+)', query, flags=re.IGNORECASE)
            normalized_parts = []
            
            for part in parts:
                if part.upper().strip() in ['OR', 'AND', 'NOT']:
                    normalized_parts.append(f' {part.upper()} ')
                elif part.strip():
                    # For each term, create case variations
                    term = part.strip().strip('"')
                    if term and not term.startswith('"'):
                        # Create OR query with case variations
                        variations = [
                            term,  # Original
                            term.lower(),  # Lowercase
                            term.capitalize(),  # Capitalized
                            term.upper()  # Uppercase
                        ]
                        # Remove duplicates while preserving order
                        seen = set()
                        unique_variations = []
                        for v in variations:
                            if v not in seen:
                                seen.add(v)
                                unique_variations.append(v)
                        normalized_parts.append(f'({" OR ".join(unique_variations)})')
                    else:
                        normalized_parts.append(part)
                else:
                    normalized_parts.append(part)
            
            return ''.join(normalized_parts)
        else:
            # Simple query - create case variations
            term = query.strip().strip('"')
            if term:
                variations = [term, term.lower(), term.capitalize(), term.upper()]
                # Remove duplicates
                seen = set()
                unique_variations = []
                for v in variations:
                    if v not in seen:
                        seen.add(v)
                        unique_variations.append(v)
                if len(unique_variations) > 1:
                    return f'({" OR ".join(unique_variations)})'
                else:
                    return unique_variations[0]
        
        return query

    def get_metadata_value(self, key: str) -> Optional[str]:
        """
        Get a metadata value by key.
        
        Args:
            key (str): Metadata key
            
        Returns:
            Optional[str]: Metadata value or None if not found
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute('SELECT value FROM metadata WHERE key = ?', (key,))
            row = cursor.fetchone()
            return row[0] if row else None
        except Exception as e:
            logger.error(f"Error getting metadata value: {str(e)}", exc_info=True)
            return None
    
    def set_metadata_value(self, key: str, value: str) -> bool:
        """
        Set a metadata value.
        
        Args:
            key (str): Metadata key
            value (str): Metadata value
            
        Returns:
            bool: True if successful
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO metadata (key, value, updated_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
            ''', (key, value))
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error setting metadata value: {str(e)}", exc_info=True)
            self.conn.rollback()
            return False

    def rebuild_fts_index(self) -> bool:
        """
        Rebuild the FTS5 index from scratch.
        Useful if the index gets out of sync.
        
        Returns:
            bool: True if successful
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute("INSERT INTO emails_fts(emails_fts) VALUES('rebuild')")
            self.conn.commit()
            logger.info("FTS index rebuilt successfully")
            return True
        except Exception as e:
            logger.error(f"Error rebuilding FTS index: {str(e)}", exc_info=True)
            self.conn.rollback()
            return False

    def close(self) -> None:
        """Close the database connection."""
        try:
            if hasattr(self, 'conn') and self.conn:
                self.conn.close()
                logger.info("SQLite connection closed")
        except Exception as e:
            logger.error(f"Error closing database: {str(e)}", exc_info=True)
    
    def __del__(self) -> None:
        """Destructor to ensure connection is closed when object is garbage collected."""
        self.close()
    
    def __enter__(self) -> 'SQLiteHandler':
        """Enter context manager."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit context manager and close connection."""
        self.close()
