"""
Graph ingestion and backfill from SQLite email/attachment data.
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
import hashlib

from src.SQLiteHandler import SQLiteHandler
from .models import Node, Edge, NodeType, EdgeType, GraphLayer
from .store_sqlite import GraphStoreSQLite

logger = logging.getLogger('outlook-email.context_graph.ingestion')


class GraphIngestion:
    """
    Ingestion pipeline to populate the context graph from email/attachment data.
    """
    
    def __init__(self, sqlite_handler: SQLiteHandler, graph_store: GraphStoreSQLite):
        """
        Initialize ingestion pipeline.
        
        Args:
            sqlite_handler: SQLite handler for email data
            graph_store: Graph store for writing nodes/edges
        """
        self.sqlite = sqlite_handler
        self.graph = graph_store
    
    def backfill_all(self, tenant_id: str, limit: Optional[int] = None) -> Dict[str, int]:
        """
        Backfill entire graph from email/attachment tables.
        
        Args:
            tenant_id: Tenant ID to assign to all nodes/edges
            limit: Optional limit on emails to process
            
        Returns:
            Dictionary with counts of created nodes/edges
        """
        logger.info(f"Starting graph backfill for tenant {tenant_id}")
        
        stats = {
            'users': 0,
            'conversations': 0,
            'documents': 0,
            'attachments': 0,
            'edges': 0
        }
        
        # Get all emails
        cursor = self.sqlite.conn.cursor()
        if limit:
            cursor.execute('SELECT * FROM emails ORDER BY received_time DESC LIMIT ?', (limit,))
        else:
            cursor.execute('SELECT * FROM emails ORDER BY received_time DESC')
        
        emails = [dict(row) for row in cursor.fetchall()]
        logger.info(f"Processing {len(emails)} emails")
        
        # Track created entities
        user_cache = set()
        conversation_cache = set()
        
        for email in emails:
            try:
                # Create/update user nodes
                sender_email = email.get('sender_email')
                if sender_email and sender_email not in user_cache:
                    user_node = self._create_user_node(
                        email_address=sender_email,
                        display_name=email.get('sender_name', ''),
                        tenant_id=tenant_id
                    )
                    if self.graph.add_node(user_node):
                        stats['users'] += 1
                        user_cache.add(sender_email)
                
                # Create recipient user nodes
                recipients = email.get('recipients', '') or ''
                for recipient in recipients.split(','):
                    recipient = recipient.strip()
                    if recipient and recipient not in user_cache:
                        user_node = self._create_user_node(
                            email_address=recipient,
                            display_name='',
                            tenant_id=tenant_id
                        )
                        if self.graph.add_node(user_node):
                            stats['users'] += 1
                            user_cache.add(recipient)
                
                # Create conversation node
                conversation_id = email.get('conversation_id')
                if conversation_id and conversation_id not in conversation_cache:
                    conv_node = self._create_conversation_node(
                        conversation_id=conversation_id,
                        subject=email.get('subject', ''),
                        started_at=email.get('received_time'),
                        tenant_id=tenant_id
                    )
                    if self.graph.add_node(conv_node):
                        stats['conversations'] += 1
                        conversation_cache.add(conversation_id)
                
                # Create document node
                doc_node = self._create_document_node(
                    email=email,
                    tenant_id=tenant_id
                )
                if self.graph.add_node(doc_node):
                    stats['documents'] += 1
                
                # Create edges
                # SENT_BY edge
                if sender_email:
                    edge = Edge(
                        id=f"edge_sentby_{email['id']}",
                        src=doc_node.id,
                        dst=f"user_{self._hash_email(sender_email)}",
                        type=EdgeType.SENT_BY,
                        props={'email_id': email['id']},
                        tenant_id=tenant_id,
                        layer=GraphLayer.JOURNEY
                    )
                    if self.graph.add_edge(edge):
                        stats['edges'] += 1
                
                # SENT_TO edges
                for recipient in recipients.split(','):
                    recipient = recipient.strip()
                    if recipient:
                        edge = Edge(
                            id=f"edge_sentto_{email['id']}_{self._hash_email(recipient)}",
                            src=doc_node.id,
                            dst=f"user_{self._hash_email(recipient)}",
                            type=EdgeType.SENT_TO,
                            props={'email_id': email['id']},
                            tenant_id=tenant_id,
                            layer=GraphLayer.JOURNEY
                        )
                        if self.graph.add_edge(edge):
                            stats['edges'] += 1
                
                # PART_OF edge (document to conversation)
                if conversation_id:
                    edge = Edge(
                        id=f"edge_partof_{email['id']}",
                        src=doc_node.id,
                        dst=f"conv_{conversation_id}",
                        type=EdgeType.PART_OF,
                        props={'email_id': email['id']},
                        tenant_id=tenant_id,
                        layer=GraphLayer.JOURNEY
                    )
                    if self.graph.add_edge(edge):
                        stats['edges'] += 1
                
                # Process attachments
                attachment_count = self._process_attachments(email['id'], tenant_id, stats)
                
            except Exception as e:
                logger.error(f"Error processing email {email.get('id')}: {str(e)}")
                continue
        
        # Create FOLLOWS edges (thread ordering)
        self._create_thread_edges(tenant_id, stats)
        
        logger.info(f"Backfill complete: {stats}")
        return stats
    
    def _create_user_node(
        self,
        email_address: str,
        display_name: str,
        tenant_id: str
    ) -> Node:
        """Create a user node."""
        user_id = f"user_{self._hash_email(email_address)}"
        
        # Simple authority score based on domain
        authority_score = 5.0
        if email_address.endswith('@zapcg.com'):
            authority_score = 8.0
        
        return Node(
            id=user_id,
            type=NodeType.USER,
            props={
                'email': email_address,
                'display_name': display_name,
                'authority_score': authority_score
            },
            tenant_id=tenant_id,
            layer=GraphLayer.PROFILE
        )
    
    def _create_conversation_node(
        self,
        conversation_id: str,
        subject: str,
        started_at: Optional[str],
        tenant_id: str
    ) -> Node:
        """Create a conversation node."""
        return Node(
            id=f"conv_{conversation_id}",
            type=NodeType.CONVERSATION,
            props={
                'conversation_id': conversation_id,
                'subject': subject,
                'started_at': started_at
            },
            tenant_id=tenant_id,
            layer=GraphLayer.JOURNEY
        )
    
    def _create_document_node(
        self,
        email: Dict[str, Any],
        tenant_id: str
    ) -> Node:
        """Create a document (email) node."""
        # Get body preview (first 500 chars)
        body = email.get('body', '') or ''
        body_preview = body[:500] if body else ''
        
        return Node(
            id=f"doc_{email['id']}",
            type=NodeType.DOCUMENT,
            props={
                'email_id': email['id'],
                'subject': email.get('subject', ''),
                'received_time': email.get('received_time'),
                'body_preview': body_preview,
                'sqlite_rowid': email.get('rowid'),
                'conversation_id': email.get('conversation_id')
            },
            tenant_id=tenant_id,
            layer=GraphLayer.JOURNEY
        )
    
    def _process_attachments(
        self,
        email_id: str,
        tenant_id: str,
        stats: Dict[str, int]
    ) -> int:
        """Process attachments for an email."""
        cursor = self.sqlite.conn.cursor()
        cursor.execute('SELECT * FROM attachments WHERE email_id = ?', (email_id,))
        attachments = [dict(row) for row in cursor.fetchall()]
        
        count = 0
        for att in attachments:
            try:
                # Create attachment node
                att_node = Node(
                    id=f"att_{att['id']}",
                    type=NodeType.ATTACHMENT,
                    props={
                        'attachment_id': att['id'],
                        'filename': att.get('filename', ''),
                        'mime_type': att.get('mime_type', ''),
                        'text_preview': (att.get('extracted_text', '') or '')[:500]
                    },
                    tenant_id=tenant_id,
                    layer=GraphLayer.JOURNEY
                )
                
                if self.graph.add_node(att_node):
                    stats['attachments'] += 1
                    count += 1
                
                # Create HAS_ATTACHMENT edge
                edge = Edge(
                    id=f"edge_hasatt_{email_id}_{att['id']}",
                    src=f"doc_{email_id}",
                    dst=att_node.id,
                    type=EdgeType.HAS_ATTACHMENT,
                    props={'attachment_id': att['id']},
                    tenant_id=tenant_id,
                    layer=GraphLayer.JOURNEY
                )
                
                if self.graph.add_edge(edge):
                    stats['edges'] += 1
                    
            except Exception as e:
                logger.error(f"Error processing attachment {att.get('id')}: {str(e)}")
                continue
        
        return count
    
    def _create_thread_edges(self, tenant_id: str, stats: Dict[str, int]):
        """Create FOLLOWS edges for thread ordering."""
        logger.info("Creating thread ordering edges")
        
        # Get all conversations
        conversations = self.graph.get_nodes_by_type(NodeType.CONVERSATION, tenant_id)
        
        for conv in conversations:
            conversation_id = conv.props.get('conversation_id')
            if not conversation_id:
                continue
            
            # Get all documents in this conversation, sorted by time
            cursor = self.sqlite.conn.cursor()
            cursor.execute('''
                SELECT id, received_time 
                FROM emails 
                WHERE conversation_id = ? 
                ORDER BY received_time ASC
            ''', (conversation_id,))
            
            emails = cursor.fetchall()
            
            # Create FOLLOWS edges between consecutive emails
            for i in range(len(emails) - 1):
                prev_id = emails[i][0]
                next_id = emails[i + 1][0]
                
                edge = Edge(
                    id=f"edge_follows_{prev_id}_{next_id}",
                    src=f"doc_{next_id}",
                    dst=f"doc_{prev_id}",
                    type=EdgeType.FOLLOWS,
                    props={
                        'thread_position': i + 1,
                        'conversation_id': conversation_id
                    },
                    tenant_id=tenant_id,
                    layer=GraphLayer.JOURNEY
                )
                
                if self.graph.add_edge(edge):
                    stats['edges'] += 1
                
                # Update thread position in document node
                doc_node = self.graph.get_node(f"doc_{next_id}")
                if doc_node:
                    doc_node.props['thread_position'] = i + 1
                    self.graph.add_node(doc_node)
    
    def _hash_email(self, email: str) -> str:
        """Create a stable hash for an email address."""
        return hashlib.md5(email.lower().encode()).hexdigest()[:16]
