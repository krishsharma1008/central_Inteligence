"""
Query service for RAG-based email search with Sarvam AI.
"""
import logging
import os
import re
from typing import List, Dict, Any
from collections import defaultdict
from src.rag.sqlite_search import EmailSearcher
from src.rag.mongo_vectors import VectorReranker
from src.SarvamClient import SarvamClient

logger = logging.getLogger('outlook-email.rag.query')


def clean_html_body(html_content: str) -> str:
    """
    Strip HTML tags and decode HTML entities from email body.
    Simple regex-based approach for basic HTML cleaning.
    
    Args:
        html_content (str): HTML email body
        
    Returns:
        str: Cleaned plain text
    """
    if not html_content:
        return ""
    
    import html
    
    # Decode HTML entities
    text = html.unescape(html_content)
    
    # Remove HTML tags using regex (simple approach)
    text = re.sub(r'<[^>]+>', '', text)
    
    # Decode HTML entities again (in case they were in attributes)
    text = html.unescape(text)
    
    # Clean up whitespace
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'\n\s*\n', '\n\n', text)
    
    # Remove common email artifacts
    text = text.replace('&nbsp;', ' ')
    text = text.replace('&amp;', '&')
    text = text.replace('&lt;', '<')
    text = text.replace('&gt;', '>')
    text = text.replace('&quot;', '"')
    
    return text.strip()


class QueryService:
    """
    Main service for handling email search queries with RAG.
    Combines FTS search, vector reranking, and LLM answer generation.
    """
    
    def __init__(
        self,
        email_searcher: EmailSearcher,
        vector_reranker: VectorReranker,
        sarvam_client: SarvamClient,
        enable_vector_rerank: bool = True
    ):
        """
        Initialize the query service.
        
        Args:
            email_searcher (EmailSearcher): FTS search helper
            vector_reranker (VectorReranker): Vector reranking helper
            sarvam_client (SarvamClient): Sarvam AI client for answer generation
            enable_vector_rerank (bool): Whether to use vector reranking
        """
        self.searcher = email_searcher
        self.reranker = vector_reranker
        self.sarvam = sarvam_client
        self.enable_vector_rerank = enable_vector_rerank
    
    def query(
        self,
        question: str,
        top_k: int = 8
    ) -> Dict[str, Any]:
        """
        Process a user question and return an answer grounded in emails.
        Uses thread-aware retrieval to provide full conversation context.
        
        Args:
            question (str): User's question
            top_k (int): Number of threads to retrieve
            
        Returns:
            Dict[str, Any]: Response with answer, citations, and retrieved emails
        """
        logger.info(f"Processing query: '{question}' (top_k={top_k})")
        
        # Extract keywords once for reuse
        keywords = self._extract_keywords(question)
        logger.info(f"Extracted keywords: {keywords}")
        
        # Step 1: Build enhanced FTS query for better recall
        logger.info("Step 1: Building enhanced FTS query")
        enhanced_query = self._build_enhanced_query(question)
        logger.info(f"Enhanced query: '{enhanced_query}'")
        
        # Step 2: FTS search with expanded results
        logger.info("Step 2: FTS search")
        fts_results = self.searcher.search(enhanced_query, top_k=top_k * 3)  # Get more for thread grouping
        
        if not fts_results:
            logger.warning("No emails found matching query")
            return {
                "success": False,
                "answer": "I couldn't find any relevant emails to answer your question.",
                "citations": [],
                "retrieved_emails": []
            }
        
        logger.info(f"FTS returned {len(fts_results)} results")
        
        # Step 3: Group by conversation_id and select top threads
        logger.info("Step 3: Grouping by conversation_id")
        thread_groups = defaultdict(list)
        for email in fts_results:
            conv_id = email.get('conversation_id')
            if conv_id:
                thread_groups[conv_id].append(email)
            else:
                # Emails without conversation_id are treated as single-message threads
                thread_groups[email['id']].append(email)
        
        # Select top threads (by highest rank in thread)
        top_threads = []
        for conv_id, emails in thread_groups.items():
            # Sort by rank (lower is better in FTS5)
            emails_sorted = sorted(emails, key=lambda x: x.get('rank', 999999))
            top_threads.append((conv_id, emails_sorted[0]))  # Store best match per thread
        
        # Sort threads by best match rank
        top_threads.sort(key=lambda x: x[1].get('rank', 999999))
        top_thread_ids = [conv_id for conv_id, _ in top_threads[:top_k]]
        
        logger.info(f"Selected {len(top_thread_ids)} top threads")
        
        # Step 4: Fetch all emails in top threads and filter for relevance
        logger.info("Step 4: Fetching full thread content")
        all_thread_emails = []
        thread_metadata = {}
        processed_email_ids = set()
        
        for conv_id in top_thread_ids:
            # Check if this is a real conversation_id or a single email ID
            # Real conversation_ids are typically longer UUIDs, single email IDs are message IDs
            # Try to fetch thread first
            thread_emails = self.searcher.get_thread_emails(conv_id)
            
            if thread_emails:
                # This is a real conversation - filter thread emails for relevance
                relevant_thread_emails = []
                for email in thread_emails:
                    if email['id'] not in processed_email_ids:
                        # Only include emails that match the search query
                        if self._is_email_relevant(email, keywords):
                            relevant_thread_emails.append(email)
                            processed_email_ids.add(email['id'])
                
                # Only add thread if it has relevant emails
                if relevant_thread_emails:
                    all_thread_emails.extend(relevant_thread_emails)
                    thread_metadata[conv_id] = {
                        'count': len(relevant_thread_emails),
                        'subject': relevant_thread_emails[0].get('subject', 'No Subject')
                    }
            else:
                # No thread found - this is a single email (no conversation_id)
                single_email = next((e for e in fts_results if e['id'] == conv_id or (not e.get('conversation_id') and e['id'] == conv_id)), None)
                if single_email and single_email['id'] not in processed_email_ids:
                    # Only include if relevant
                    if self._is_email_relevant(single_email, keywords):
                        all_thread_emails.append(single_email)
                        processed_email_ids.add(single_email['id'])
                        thread_metadata[conv_id] = {
                            'count': 1,
                            'subject': single_email.get('subject', 'No Subject')
                        }
        
        logger.info(f"Fetched {len(all_thread_emails)} relevant emails across {len(thread_metadata)} threads")
        
        # Step 5: Optional vector reranking on thread level
        final_thread_emails = all_thread_emails
        if self.enable_vector_rerank and self.reranker.embedding_model is not None and len(all_thread_emails) > top_k * 5:
            logger.info("Step 5: Vector reranking thread emails")
            try:
                query_embedding = self.reranker.embed_query(question)
                if query_embedding:
                    email_ids = [email['id'] for email in all_thread_emails]
                    reranked = self.reranker.rerank(email_ids, query_embedding, top_k=top_k * 5)
                    
                    id_to_email = {email['id']: email for email in all_thread_emails}
                    final_thread_emails = []
                    for reranked_item in reranked:
                        email_id = reranked_item['id']
                        if email_id in id_to_email:
                            email = id_to_email[email_id]
                            email['vector_similarity'] = reranked_item['similarity']
                            final_thread_emails.append(email)
                    
                    logger.info(f"Reranked to {len(final_thread_emails)} emails")
            except Exception as e:
                logger.error(f"Error during vector reranking: {str(e)}")
                logger.warning("Falling back to all thread emails")
        else:
            logger.info("Step 5: Skipping vector reranking")
        
        # Step 6: Build thread-aware context
        logger.info("Step 6: Building thread-aware context")
        context = self._build_thread_context(final_thread_emails, thread_metadata)
        prompt = self._build_prompt(question, context)
        
        # Step 7: Call Sarvam for answer generation
        logger.info("Step 7: Generating answer with Sarvam")
        try:
            answer_response = self._generate_answer(prompt)
            answer = answer_response.get('answer', 'I apologize, but I encountered an error generating the answer.')
            raw_citations = answer_response.get('citations', [])
        except Exception as e:
            logger.error(f"Error generating answer: {str(e)}")
            answer = f"I found relevant emails but encountered an error generating the answer: {str(e)}"
            raw_citations = []
        
        # Step 8: Build citations from threads (filtered for relevance)
        logger.info("Step 8: Building citations")
        citations = self._build_citations(final_thread_emails, keywords, raw_citations)
        
        return {
            "success": True,
            "answer": answer,
            "citations": citations,
            "retrieved_emails": final_thread_emails[:top_k * 10]  # Return more for thread context
        }
    
    def _extract_keywords(self, question: str) -> List[str]:
        """
        Extract meaningful keywords from a question.
        
        Args:
            question (str): Original question
            
        Returns:
            List[str]: List of keywords
        """
        # Extract potential keywords (proper nouns, hyphenated terms, etc.)
        words = re.findall(r'\b[\w-]+\b', question.lower())
        
        # Filter for meaningful keywords (length > 2, not common stop words)
        stop_words = {'the', 'what', 'when', 'where', 'who', 'why', 'how', 'with', 'about', 'from', 'for', 'and', 'or', 'but', 'did', 'happened', 'said', 'say', 'give', 'me', 'brief', 'tell', 'show', 'find', 'search', 'query'}
        keywords = [w for w in words if len(w) > 2 and w not in stop_words]
        
        return keywords
    
    def _build_enhanced_query(self, question: str) -> str:
        """
        Build an enhanced FTS query with better recall.
        Extracts keywords, handles variations, and builds OR queries.
        
        Args:
            question (str): Original question
            
        Returns:
            str: Enhanced FTS query
        """
        keywords = self._extract_keywords(question)
        
        # Build OR query for better recall
        if len(keywords) > 1:
            # Use OR for multiple keywords
            enhanced = ' OR '.join(keywords)
        else:
            enhanced = question
        
        # Also try exact phrase match
        if len(keywords) > 0:
            # Add phrase query for multi-word terms
            phrase_parts = []
            for i in range(len(keywords) - 1):
                phrase = f'"{keywords[i]} {keywords[i+1]}"'
                phrase_parts.append(phrase)
            
            if phrase_parts:
                enhanced = f'({enhanced}) OR ({' OR '.join(phrase_parts)})'
        
        return enhanced
    
    def _is_email_relevant(self, email: Dict[str, Any], keywords: List[str]) -> bool:
        """
        Check if an email is relevant to the search query by checking if it contains any keywords.
        
        Args:
            email (Dict[str, Any]): Email to check
            keywords (List[str]): List of search keywords
            
        Returns:
            bool: True if email is relevant
        """
        if not keywords:
            return True  # If no keywords, include all
        
        # Get email text (subject + body)
        subject = (email.get('subject', '') or '').lower()
        body = (email.get('body', '') or '').lower()
        body_cleaned = clean_html_body(body).lower()
        
        # Check if any keyword appears in subject or body
        for keyword in keywords:
            if keyword in subject or keyword in body_cleaned:
                return True
        
        return False
    
    def _build_context(self, emails: List[Dict[str, Any]]) -> str:
        """
        Build context string from retrieved emails (legacy method).
        
        Args:
            emails (List[Dict[str, Any]]): Retrieved emails
            
        Returns:
            str: Formatted context string
        """
        context_parts = []
        for i, email in enumerate(emails, 1):
            context_parts.append(f"""
EMAIL {i}:
Subject: {email.get('subject', 'No Subject')}
From: {email.get('sender_name', '')} <{email.get('sender_email', '')}>
Date: {email.get('received_time', '')}
Body: {email.get('body', '')[:500]}...
""")
        
        return "\n---\n".join(context_parts)
    
    def _build_thread_context(self, emails: List[Dict[str, Any]], thread_metadata: Dict[str, Dict]) -> str:
        """
        Build thread-aware context string from retrieved emails.
        Groups emails by conversation and presents them chronologically.
        
        Args:
            emails (List[Dict[str, Any]]): Retrieved emails (may include full threads)
            thread_metadata (Dict[str, Dict]): Metadata about threads
            
        Returns:
            str: Formatted thread context string
        """
        # Group emails by conversation_id
        threads = defaultdict(list)
        standalone = []
        
        for email in emails:
            conv_id = email.get('conversation_id')
            if conv_id:
                threads[conv_id].append(email)
            else:
                standalone.append(email)
        
        # Sort each thread by received_time
        for conv_id in threads:
            threads[conv_id].sort(key=lambda x: x.get('received_time', ''))
        
        context_parts = []
        thread_num = 1
        
        # Process threads
        for conv_id, thread_emails in threads.items():
            thread_info = thread_metadata.get(conv_id, {})
            context_parts.append(f"""
THREAD {thread_num} ({thread_info.get('count', len(thread_emails))} messages):
Subject: {thread_info.get('subject', thread_emails[0].get('subject', 'No Subject'))}
Conversation ID: {conv_id}

""")
            
            for msg_num, email in enumerate(thread_emails, 1):
                # Clean HTML from body and use larger body budget for thread context (2000 chars per message)
                body = email.get('body', '')
                body_cleaned = clean_html_body(body)
                body_preview = body_cleaned[:2000] + ('...' if len(body_cleaned) > 2000 else '')
                
                context_parts.append(f"""  Message {msg_num}:
  From: {email.get('sender_name', '')} <{email.get('sender_email', '')}>
  Date: {email.get('received_time', '')}
  Body: {body_preview}
""")
                
                # Add attachment content if available
                email_id = email.get('id')
                if email_id:
                    attachments = self._get_email_attachments(email_id)
                    if attachments:
                        context_parts.append("  Attachments:")
                        for att in attachments[:5]:  # Limit to 5 attachments per email
                            att_text = att.get('extracted_text', '') or att.get('text', '')
                            if att_text:
                                att_preview = att_text[:500] + ('...' if len(att_text) > 500 else '')
                                context_parts.append(f"    - {att.get('filename', 'unknown')}: {att_preview}")
                
                context_parts.append("")
            
            thread_num += 1
        
        # Process standalone emails (no conversation_id)
        for email in standalone:
            body = email.get('body', '')
            body_cleaned = clean_html_body(body)
            body_preview = body_cleaned[:2000] + ('...' if len(body_cleaned) > 2000 else '')
            
            context_parts.append(f"""
STANDALONE EMAIL {thread_num}:
Subject: {email.get('subject', 'No Subject')}
From: {email.get('sender_name', '')} <{email.get('sender_email', '')}>
Date: {email.get('received_time', '')}
Body: {body_preview}
""")
            
            # Add attachment content if available
            email_id = email.get('id')
            if email_id:
                attachments = self._get_email_attachments(email_id)
                if attachments:
                    context_parts.append("Attachments:")
                    for att in attachments[:5]:  # Limit to 5 attachments per email
                        att_text = att.get('extracted_text', '') or att.get('text', '')
                        if att_text:
                            att_preview = att_text[:500] + ('...' if len(att_text) > 500 else '')
                            context_parts.append(f"  - {att.get('filename', 'unknown')}: {att_preview}")
            
            context_parts.append("")
            thread_num += 1
        
        return "\n".join(context_parts)
    
    def _get_email_attachments(self, email_id: str) -> List[Dict[str, Any]]:
        """
        Get attachments for an email from SQLite.
        
        Args:
            email_id: Email ID
            
        Returns:
            List of attachment dictionaries
        """
        try:
            # Import here to avoid circular dependencies
            from src.SQLiteHandler import SQLiteHandler
            import os
            from dotenv import load_dotenv
            load_dotenv()
            
            db_path = os.getenv('SQLITE_DB_PATH')
            handler = SQLiteHandler(db_path)
            
            cursor = handler.conn.cursor()
            cursor.execute('''
                SELECT filename, extracted_text, mime_type, text_length
                FROM attachments
                WHERE email_id = ? AND extracted_text IS NOT NULL AND extracted_text != ''
                ORDER BY text_length DESC
                LIMIT 10
            ''', (email_id,))
            
            results = cursor.fetchall()
            attachments = []
            for row in results:
                attachments.append({
                    'filename': row[0],
                    'extracted_text': row[1],
                    'text': row[1],  # Alias for compatibility
                    'mime_type': row[2],
                    'text_length': row[3]
                })
            
            handler.close()
            return attachments
        except Exception as e:
            logger.error(f"Error getting attachments for email {email_id}: {str(e)}")
            return []
    
    def _build_prompt(self, question: str, context: str) -> str:
        """
        Build prompt for Sarvam AI.
        
        Args:
            question (str): User's question
            context (str): Context from retrieved emails
            
        Returns:
            str: Formatted prompt
        """
        prompt = f"""You are a helpful assistant that answers questions based on company emails.

IMPORTANT RULES:
1. Answer ONLY using information from the emails/threads provided below
2. If the answer is not in the emails, say "I don't have enough information in the emails to answer that."
3. When referencing emails, cite by thread number and message number (e.g., "According to Thread 1, Message 2...")
4. Pay attention to the full conversation context in each thread - earlier messages may provide important context
5. Be concise and factual
6. If multiple threads discuss the same topic, synthesize information across threads

EMAIL THREADS:
{context}

QUESTION:
{question}

Please provide:
1. A clear, concise answer based on the email threads
2. Citations to specific threads and messages that support your answer
"""
        return prompt
    
    def _generate_answer(self, prompt: str) -> Dict[str, Any]:
        """
        Generate answer using Sarvam AI.
        
        Args:
            prompt (str): Formatted prompt
            
        Returns:
            Dict[str, Any]: Response with answer and citations
        """
        import requests
        
        # Use Sarvam's corrected endpoint
        url = "https://api.sarvam.ai/v1/chat/completions"
        headers = {
            "api-subscription-key": os.getenv("SARVAM_API_KEY"),
            "Content-Type": "application/json"
        }
        
        payload = {
            "model": os.getenv("SARVAM_MODEL", "sarvam-m"),
            "messages": [
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            "temperature": 0.2,
            "max_tokens": 500
        }
        
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=30)
            
            if response.status_code == 200:
                result = response.json()
                answer_text = result["choices"][0]["message"]["content"]
                
                return {
                    "answer": answer_text,
                    "citations": []  # Parse from answer text if needed
                }
            else:
                logger.error(f"Sarvam API error: {response.status_code} - {response.text}")
                return {
                    "answer": f"Error calling Sarvam API: {response.status_code}",
                    "citations": []
                }
        except Exception as e:
            logger.error(f"Exception calling Sarvam API: {str(e)}")
            raise
    
    def _build_citations(self, emails: List[Dict[str, Any]], keywords: List[str] = None, raw_citations: List[Any] = None) -> List[Dict[str, Any]]:
        """
        Build structured citations from emails, filtering for relevance.
        
        Args:
            emails (List[Dict[str, Any]]): Retrieved emails
            keywords (List[str], optional): Search keywords for relevance filtering
            raw_citations (List[Any], optional): Raw citations from LLM (if any, currently unused)
            
        Returns:
            List[Dict[str, Any]]: Structured citations (only relevant ones)
        """
        citations = []
        for email in emails:
            # Only include citations that are relevant to the query
            if keywords and not self._is_email_relevant(email, keywords):
                continue
                
            body_cleaned = clean_html_body(email.get('body', ''))
            citations.append({
                "id": email.get('id'),
                "subject": email.get('subject'),
                "sender": email.get('sender_name'),
                "sender_email": email.get('sender_email'),
                "received_time": email.get('received_time'),
                "snippet": body_cleaned[:200] + "..." if body_cleaned else ""
            })
        
        return citations

