"""
Graph-native query service with hybrid search and context compilation.
"""

import logging
import asyncio
from typing import List, Dict, Any, Optional
from collections import defaultdict

from src.rag.sqlite_search import EmailSearcher
from src.rag.mongo_vectors import VectorReranker
from src.SarvamClient import SarvamClient
from src.context_graph.compiler import ContextCompiler
from src.context_graph.models import ContextPacket, NodeScore

logger = logging.getLogger('outlook-email.rag.graph_query')


class GraphQueryService:
    """
    Graph-native query service that uses hybrid search (FTS + vector)
    with context graph compilation for deterministic, explainable answers.
    """
    
    def __init__(
        self,
        email_searcher: EmailSearcher,
        vector_reranker: VectorReranker,
        context_compiler: ContextCompiler,
        sarvam_client: SarvamClient,
        enable_vector_search: bool = True
    ):
        """
        Initialize graph query service.
        
        Args:
            email_searcher: FTS search helper
            vector_reranker: Vector similarity search helper
            context_compiler: Context graph compiler
            sarvam_client: Sarvam AI client
            enable_vector_search: Whether to use vector search in hybrid mode
        """
        self.searcher = email_searcher
        self.reranker = vector_reranker
        self.compiler = context_compiler
        self.sarvam = sarvam_client
        self.enable_vector_search = enable_vector_search
    
    def query(
        self,
        question: str,
        top_k: int = 8,
        tenant_id: str = "default",
        debug: bool = False
    ) -> Dict[str, Any]:
        """
        Process a question using graph-native context resolution.
        
        Args:
            question: User's question
            top_k: Number of top conversation threads to include
            tenant_id: Tenant ID for multi-tenancy
            debug: Enable debug mode with detailed traces
            
        Returns:
            Dict with answer, citations, context_packet, and retrieved emails
        """
        logger.info(f"Graph query: '{question}' (top_k={top_k})")
        
        try:
            # Step 1: Hybrid seed discovery (FTS + optional vector search)
            logger.info("Step 1: Hybrid seed discovery")
            seed_results = self._discover_seeds_sync(question, top_k)
            
            if not seed_results:
                return {
                    "success": False,
                    "answer": "I couldn't find any relevant emails to answer your question.",
                    "citations": [],
                    "retrieved_emails": [],
                    "context_packet": None
                }
            
            # Extract conversation IDs as seed nodes
            seed_node_ids = []
            seen_convs = set()
            for result in seed_results:
                conv_id = result.get('conversation_id')
                if conv_id and conv_id not in seen_convs:
                    seed_node_ids.append(f"conv_{conv_id}")
                    seen_convs.add(conv_id)
            
            logger.info(f"Discovered {len(seed_node_ids)} seed conversation nodes")
            
            if not seed_node_ids:
                return {
                    "success": False,
                    "answer": "I couldn't find any conversation threads matching your question.",
                    "citations": [],
                    "retrieved_emails": [],
                    "context_packet": None
                }
            
            # Step 2: Compile context packet via graph traversal
            logger.info("Step 2: Compiling context packet via graph")
            context_packet = self.compiler.compile(
                intent_text=question,
                seed_node_ids=seed_node_ids[:top_k],
                tenant_id=tenant_id,
                debug=debug
            )
            
            # Step 3: Materialize subgraph as LLM context
            logger.info("Step 3: Materializing context from graph")
            context_text = self._materialize_context(context_packet)
            
            # Step 4: Generate answer with Sarvam
            logger.info("Step 4: Generating answer")
            prompt = self._build_prompt(question, context_text)
            
            try:
                answer_response = self._generate_answer(prompt)
                answer = answer_response.get('answer', 'I apologize, but I encountered an error generating the answer.')
            except Exception as e:
                logger.error(f"Error generating answer: {str(e)}")
                answer = f"I found relevant information but encountered an error generating the answer: {str(e)}"
            
            # Step 5: Build citations from context packet
            logger.info("Step 5: Building citations")
            citations = self._build_citations_from_packet(context_packet)
            
            # Step 6: Get retrieved emails for display
            retrieved_emails = self._get_retrieved_emails(context_packet)
            
            return {
                "success": True,
                "answer": answer,
                "citations": citations,
                "retrieved_emails": retrieved_emails,
                "context_packet": context_packet.to_dict()
            }
            
        except Exception as e:
            logger.error(f"Error in graph query: {str(e)}", exc_info=True)
            return {
                "success": False,
                "answer": f"An error occurred while processing your question: {str(e)}",
                "citations": [],
                "retrieved_emails": [],
                "context_packet": None
            }
    
    def _discover_seeds_sync(self, question: str, top_k: int) -> List[Dict[str, Any]]:
        """
        Synchronous hybrid seed discovery (FTS + vector).
        
        Args:
            question: Search query
            top_k: Number of results
            
        Returns:
            List of conversation results
        """
        # Use unified search for emails + attachments
        fts_results = self.searcher.unified_search(question, top_k=top_k * 2)
        
        if not self.enable_vector_search or not self.reranker.embedding_model:
            logger.info("Using FTS-only seed discovery")
            return fts_results
        
        # Perform vector search
        try:
            query_embedding = self.reranker.embed_query(question)
            if query_embedding:
                logger.info("Performing vector search for seeds")
                vector_results = self.reranker.vector_search(query_embedding, top_k=top_k * 2)
                
                # Merge FTS and vector results
                return self._merge_search_results(fts_results, vector_results, top_k)
        except Exception as e:
            logger.error(f"Error in vector search, falling back to FTS: {str(e)}")
        
        return fts_results
    
    def _merge_search_results(
        self,
        fts_results: List[Dict[str, Any]],
        vector_results: List[Dict[str, Any]],
        top_k: int
    ) -> List[Dict[str, Any]]:
        """
        Merge and deduplicate FTS and vector search results.
        
        Args:
            fts_results: Results from FTS search
            vector_results: Results from vector search
            top_k: Number of results to return
            
        Returns:
            Merged and ranked results
        """
        # Normalize scores
        conv_scores = {}
        
        # Process FTS results (rank is lower = better, invert for score)
        max_rank = max([r.get('best_rank', 999999) for r in fts_results]) if fts_results else 1
        for i, result in enumerate(fts_results):
            conv_id = result.get('conversation_id')
            if conv_id:
                rank = result.get('best_rank', 999999)
                # Normalize to 0-1, inverted
                fts_score = 1.0 - (rank / (max_rank + 1))
                conv_scores[conv_id] = {
                    'conversation_id': conv_id,
                    'fts_score': fts_score,
                    'vector_score': 0.0,
                    'metadata': result
                }
        
        # Process vector results
        for i, result in enumerate(vector_results):
            email_id = result.get('id')
            # Get conversation ID from metadata
            metadata = result.get('metadata', {})
            conv_id = metadata.get('conversation_id', email_id)
            
            vector_score = result.get('similarity', 0.0)
            
            if conv_id in conv_scores:
                conv_scores[conv_id]['vector_score'] = max(
                    conv_scores[conv_id]['vector_score'],
                    vector_score
                )
            else:
                conv_scores[conv_id] = {
                    'conversation_id': conv_id,
                    'fts_score': 0.0,
                    'vector_score': vector_score,
                    'metadata': metadata
                }
        
        # Compute combined score (weighted average)
        for conv_id in conv_scores:
            fts_score = conv_scores[conv_id]['fts_score']
            vec_score = conv_scores[conv_id]['vector_score']
            # Weight: 40% FTS, 60% vector
            conv_scores[conv_id]['combined_score'] = (fts_score * 0.4) + (vec_score * 0.6)
        
        # Sort by combined score and return top_k
        sorted_results = sorted(
            conv_scores.values(),
            key=lambda x: x['combined_score'],
            reverse=True
        )[:top_k]
        
        # Convert back to result format
        final_results = []
        for result in sorted_results:
            metadata = result.get('metadata', {})
            final_results.append({
                'conversation_id': result['conversation_id'],
                'combined_score': result['combined_score'],
                'best_rank': metadata.get('best_rank', 0),
                'subject': metadata.get('subject', ''),
                'sender_name': metadata.get('sender_name', ''),
                'received_time': metadata.get('received_time', '')
            })
        
        logger.info(f"Merged to {len(final_results)} unique conversations")
        return final_results
    
    def _materialize_context(self, packet: ContextPacket) -> str:
        """
        Materialize the context packet subgraph into text for LLM.
        
        Args:
            packet: Compiled context packet
            
        Returns:
            Formatted context text
        """
        from .query_service import clean_html_body
        
        context_parts = []
        
        # Group nodes by type
        conversations = [n for n in packet.nodes if n.type.value == "Conversation"]
        documents = [n for n in packet.nodes if n.type.value == "Document"]
        attachments = [n for n in packet.nodes if n.type.value == "Attachment"]
        
        # Group documents by conversation
        conv_docs = defaultdict(list)
        for doc in documents:
            conv_id = doc.props.get('conversation_id')
            if conv_id:
                conv_docs[f"conv_{conv_id}"].append(doc)
        
        # Build context by conversation
        thread_num = 1
        for conv in conversations:
            docs = conv_docs.get(conv.id, [])
            if not docs:
                continue
            
            # Sort docs by received time
            docs.sort(key=lambda d: d.props.get('received_time', ''))
            
            context_parts.append(f"\nTHREAD {thread_num}:")
            context_parts.append(f"Subject: {conv.props.get('subject', 'No Subject')}")
            context_parts.append(f"Score: {packet.scores.get(conv.id, NodeScore(conv.id)).total_score:.2f}\n")
            
            for msg_num, doc in enumerate(docs, 1):
                body = doc.props.get('body_preview', '')
                body_cleaned = clean_html_body(body) if body else ''
                
                context_parts.append(f"  Message {msg_num}:")
                context_parts.append(f"  From: {doc.props.get('sender_name', '')} <{doc.props.get('sender_email', '')}>")
                context_parts.append(f"  Date: {doc.props.get('received_time', '')}")
                context_parts.append(f"  Body: {body_cleaned[:1500]}")
                
                # Add attachments for this document
                doc_atts = [a for a in attachments if any(
                    e.src == doc.id and e.dst == a.id
                    for e in packet.edges if e.type.value == "HAS_ATTACHMENT"
                )]
                
                if doc_atts:
                    context_parts.append("  Attachments:")
                    for att in doc_atts[:3]:
                        text_preview = att.props.get('text_preview', '')
                        if text_preview:
                            context_parts.append(f"    - {att.props.get('filename', 'unknown')}: {text_preview[:300]}")
                
                context_parts.append("")
            
            thread_num += 1
        
        return "\n".join(context_parts)
    
    def _build_prompt(self, question: str, context: str) -> str:
        """Build prompt for Sarvam AI."""
        return f"""You are a helpful assistant that answers questions based on company emails.

IMPORTANT RULES:
1. Answer ONLY using information from the email threads provided below
2. If the answer is not in the emails, say "I don't have enough information in the emails to answer that."
3. When referencing emails, cite by thread number and message number (e.g., "According to Thread 1, Message 2...")
4. Pay attention to the full conversation context
5. Be concise and factual

EMAIL THREADS:
{context}

QUESTION:
{question}

Please provide a clear, concise answer based on the email threads."""
    
    def _generate_answer(self, prompt: str) -> Dict[str, Any]:
        """Generate answer using Sarvam AI."""
        import requests
        import os
        
        url = "https://api.sarvam.ai/v1/chat/completions"
        headers = {
            "api-subscription-key": os.getenv("SARVAM_API_KEY"),
            "Content-Type": "application/json"
        }
        
        payload = {
            "model": os.getenv("SARVAM_MODEL", "sarvam-m"),
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.2,
            "max_tokens": 500
        }
        
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        
        if response.status_code == 200:
            result = response.json()
            return {"answer": result["choices"][0]["message"]["content"]}
        else:
            logger.error(f"Sarvam API error: {response.status_code}")
            return {"answer": f"Error calling Sarvam API: {response.status_code}"}
    
    def _build_citations_from_packet(self, packet: ContextPacket) -> List[Dict[str, Any]]:
        """Build citations from context packet."""
        from .query_service import clean_html_body
        
        citations = []
        documents = [n for n in packet.nodes if n.type.value == "Document"]
        
        # Sort by score
        documents.sort(
            key=lambda d: packet.scores.get(d.id, NodeScore(d.id)).total_score,
            reverse=True
        )
        
        for doc in documents[:10]:  # Top 10 citations
            body = doc.props.get('body_preview', '')
            body_cleaned = clean_html_body(body) if body else ''
            
            citations.append({
                "id": doc.props.get('email_id', doc.id),
                "subject": doc.props.get('subject', 'No Subject'),
                "sender": doc.props.get('sender_name', ''),
                "received_time": doc.props.get('received_time', ''),
                "snippet": body_cleaned[:200] + "..." if body_cleaned else "",
                "score": packet.scores.get(doc.id, NodeScore(doc.id)).total_score
            })
        
        return citations
    
    def _get_retrieved_emails(self, packet: ContextPacket) -> List[Dict[str, Any]]:
        """Get full email details for retrieved documents."""
        documents = [n for n in packet.nodes if n.type.value == "Document"]
        emails = []
        
        for doc in documents[:20]:  # Top 20 emails
            emails.append({
                "id": doc.props.get('email_id', doc.id),
                "subject": doc.props.get('subject', ''),
                "sender_name": doc.props.get('sender_name', ''),
                "received_time": doc.props.get('received_time', ''),
                "conversation_id": doc.props.get('conversation_id', ''),
                "body": doc.props.get('body_preview', '')
            })
        
        return emails
