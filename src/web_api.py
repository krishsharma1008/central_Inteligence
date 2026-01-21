"""
FastAPI server for Email RAG search.
Provides HTTP endpoints for querying emails and generating answers.
"""
import logging
import os
import json
from typing import Optional
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from src.SQLiteHandler import SQLiteHandler
from src.MongoDBHandler import MongoDBHandler
from src.SarvamClient import SarvamClient
from src.rag.sqlite_search import EmailSearcher
from src.rag.mongo_vectors import VectorReranker
from src.rag.query_service import QueryService
from src.rag.graph_query_service import GraphQueryService

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('outlook-email.web-api')

# Initialize FastAPI app
app = FastAPI(
    title="Email RAG API",
    description="API for RAG-based email search with Sarvam AI",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Request/Response models
class QueryRequest(BaseModel):
    question: str
    top_k: Optional[int] = 8


class QueryResponse(BaseModel):
    success: bool
    answer: str
    citations: list
    retrieved_emails: list
    context_packet: Optional[dict] = None


# Global service instances (initialized on startup)
query_service: Optional[QueryService] = None
graph_query_service: Optional[GraphQueryService] = None
sqlite_handler: Optional[SQLiteHandler] = None
mongodb_handler: Optional[MongoDBHandler] = None
graph_store = None
context_compiler = None
vector_scorer = None


@app.on_event("startup")
async def startup_event():
    """Initialize services on startup."""
    global query_service, graph_query_service, sqlite_handler, mongodb_handler, graph_store, context_compiler, vector_scorer
    
    logger.info("Initializing services...")
    
    # Load configuration from environment
    mongodb_uri = os.getenv("MONGODB_URI")
    sqlite_db_path = os.getenv("SQLITE_DB_PATH")
    sarvam_api_key = os.getenv("SARVAM_API_KEY")
    collection_name = os.getenv("COLLECTION_NAME", "CIZAPCOM")
    enable_vector_rerank = os.getenv("ENABLE_VECTOR_RERANK", "true").lower() == "true"
    
    if not all([mongodb_uri, sqlite_db_path, sarvam_api_key]):
        raise ValueError("Missing required environment variables: MONGODB_URI, SQLITE_DB_PATH, SARVAM_API_KEY")
    
    # Initialize handlers
    sqlite_handler = SQLiteHandler(sqlite_db_path)
    mongodb_handler = MongoDBHandler(mongodb_uri, collection_name)
    
    # Initialize Sarvam client
    sarvam_client = SarvamClient(api_key=sarvam_api_key)
    
    # Initialize searcher and reranker
    email_searcher = EmailSearcher(sqlite_handler)
    
    # Initialize embedding model for reranking
    embedding_model = None
    if enable_vector_rerank:
        try:
            from sentence_transformers import SentenceTransformer
            model_name = os.getenv("EMBEDDING_MODEL_NAME", "sentence-transformers/all-MiniLM-L6-v2")
            logger.info(f"Loading embedding model: {model_name}")
            embedding_model = SentenceTransformer(model_name)
            logger.info("Embedding model loaded successfully")
        except Exception as e:
            logger.error(f"Error loading embedding model: {str(e)}")
            logger.warning("Vector reranking will be disabled")
    
    # Initialize graph store and context compiler
    from src.context_graph.store_sqlite import GraphStoreSQLite
    from src.context_graph.compiler import ContextCompiler
    from src.context_graph.vector_scorer import VectorScorer
    
    graph_store = GraphStoreSQLite(sqlite_db_path)
    
    # Initialize vector scorer if embedding model is available
    if embedding_model:
        vector_scorer = VectorScorer(mongodb_handler, embedding_model)
        logger.info("Vector scorer initialized")
    else:
        vector_scorer = None
        logger.warning("Vector scorer not available (no embedding model)")
    
    context_compiler = ContextCompiler(
        graph_store=graph_store,
        vector_scorer=vector_scorer,
        recency_half_life_days=7.0,
        max_nodes=50,
        max_edges=100,
        max_tokens=8000
    )
    logger.info("Graph store and context compiler initialized")
    
    vector_reranker = VectorReranker(mongodb_handler, embedding_model)
    
    # Initialize query service (legacy)
    query_service = QueryService(
        email_searcher=email_searcher,
        vector_reranker=vector_reranker,
        sarvam_client=sarvam_client,
        enable_vector_rerank=enable_vector_rerank
    )
    
    # Initialize graph query service (new)
    graph_query_service = GraphQueryService(
        email_searcher=email_searcher,
        vector_reranker=vector_reranker,
        context_compiler=context_compiler,
        sarvam_client=sarvam_client,
        enable_vector_search=enable_vector_rerank
    )
    
    logger.info("Services initialized successfully")


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown."""
    global sqlite_handler, mongodb_handler
    
    logger.info("Shutting down services...")
    
    if sqlite_handler:
        sqlite_handler.close()
    
    if mongodb_handler:
        mongodb_handler.close()
    
    logger.info("Services shut down successfully")


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "message": "Email RAG API",
        "version": "1.0.0",
        "endpoints": {
            "query": "/query (POST)",
            "emails": "/emails (GET)",
            "email_by_id": "/emails/{id} (GET)",
            "health": "/health (GET)"
        }
    }


@app.post("/query", response_model=QueryResponse)
async def query_emails(request: QueryRequest):
    """
    Query emails using graph-native context resolution.
    Returns answer with context packet for visualization.
    
    Args:
        request (QueryRequest): Query request with question and top_k
        
    Returns:
        QueryResponse: Response with answer, citations, retrieved emails, and context packet
    """
    if not graph_query_service:
        raise HTTPException(status_code=500, detail="Graph query service not initialized")
    
    if not request.question or not request.question.strip():
        raise HTTPException(status_code=400, detail="Question cannot be empty")
    
    try:
        logger.info(f"Received graph query: {request.question}")
        result = graph_query_service.query(request.question, top_k=request.top_k)
        
        # Build response (context_packet is already in result)
        return {
            "success": result["success"],
            "answer": result["answer"],
            "citations": result["citations"],
            "retrieved_emails": result["retrieved_emails"],
            "context_packet": result.get("context_packet")
        }
    except Exception as e:
        logger.error(f"Error processing graph query: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error processing query: {str(e)}")


@app.get("/emails")
async def list_emails(limit: int = 20, offset: int = 0):
    """
    List recent emails.
    
    Args:
        limit (int): Number of emails to return
        offset (int): Offset for pagination
        
    Returns:
        dict: List of emails
    """
    if not sqlite_handler:
        raise HTTPException(status_code=500, detail="SQLite handler not initialized")
    
    try:
        cursor = sqlite_handler.conn.cursor()
        cursor.execute('''
            SELECT id, subject, sender_name, sender_email, received_time, folder
            FROM emails
            ORDER BY received_time DESC
            LIMIT ? OFFSET ?
        ''', (limit, offset))
        
        emails = []
        for row in cursor.fetchall():
            emails.append({
                "id": row[0],
                "subject": row[1],
                "sender_name": row[2],
                "sender_email": row[3],
                "received_time": row[4],
                "folder": row[5]
            })
        
        return {"emails": emails, "count": len(emails)}
    except Exception as e:
        logger.error(f"Error listing emails: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error listing emails: {str(e)}")


@app.get("/emails/{email_id}")
async def get_email(email_id: str):
    """
    Get a specific email by ID.
    
    Args:
        email_id (str): Email ID
        
    Returns:
        dict: Email details
    """
    if not sqlite_handler:
        raise HTTPException(status_code=500, detail="SQLite handler not initialized")
    
    try:
        email = sqlite_handler.get_email_by_id(email_id)
        
        if not email:
            raise HTTPException(status_code=404, detail="Email not found")
        
        return {"email": email}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting email: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error getting email: {str(e)}")


@app.get("/health")
async def health_check():
    """
    Health check endpoint.
    
    Returns:
        dict: Health status of services
    """
    health_status = {
        "status": "healthy",
        "services": {}
    }
    
    # Check SQLite
    try:
        if sqlite_handler:
            count = sqlite_handler.get_email_count()
            health_status["services"]["sqlite"] = {
                "status": "ok",
                "email_count": count
            }
        else:
            health_status["services"]["sqlite"] = {
                "status": "not_initialized"
            }
    except Exception as e:
        health_status["services"]["sqlite"] = {
            "status": "error",
            "error": str(e)
        }
        health_status["status"] = "degraded"
    
    # Check MongoDB
    try:
        if mongodb_handler:
            count = mongodb_handler.get_collection_count()
            health_status["services"]["mongodb"] = {
                "status": "ok",
                "document_count": count
            }
        else:
            health_status["services"]["mongodb"] = {
                "status": "not_initialized"
            }
    except Exception as e:
        health_status["services"]["mongodb"] = {
            "status": "error",
            "error": str(e)
        }
        health_status["status"] = "degraded"
    
    # Check Sarvam API
    try:
        if query_service and query_service.sarvam:
            test_result = query_service.sarvam.test_connection()
            health_status["services"]["sarvam"] = {
                "status": "ok" if test_result else "error",
                "endpoint": "https://api.sarvam.ai/v1/chat/completions"
            }
        else:
            health_status["services"]["sarvam"] = {
                "status": "not_initialized"
            }
    except Exception as e:
        health_status["services"]["sarvam"] = {
            "status": "error",
            "error": str(e)
        }
        health_status["status"] = "degraded"
    
    return health_status


# Graph API endpoints
class CompileRequest(BaseModel):
    question: str
    top_k: Optional[int] = 8
    tenant_id: Optional[str] = "default"
    debug: Optional[bool] = False


@app.post("/graph/compile")
async def compile_context(request: CompileRequest):
    """
    Compile a context packet using the graph compiler.
    
    Args:
        request: Compile request with question and parameters
        
    Returns:
        Context packet with nodes, edges, scores, and trace
    """
    if not context_compiler or not query_service:
        raise HTTPException(status_code=500, detail="Graph compiler not initialized")
    
    try:
        logger.info(f"Compiling context for: {request.question}")
        
        # Step 1: Get seed nodes from FTS search
        fts_results = query_service.searcher.search(request.question, top_k=request.top_k * 2)
        
        # Extract conversation IDs as seed nodes
        seed_node_ids = []
        seen_convs = set()
        for email in fts_results:
            conv_id = email.get('conversation_id')
            if conv_id and conv_id not in seen_convs:
                seed_node_ids.append(f"conv_{conv_id}")
                seen_convs.add(conv_id)
        
        if not seed_node_ids:
            return {
                "success": False,
                "message": "No seed nodes found",
                "packet": None
            }
        
        # Step 2: Compile context packet
        packet = context_compiler.compile(
            intent_text=request.question,
            seed_node_ids=seed_node_ids[:request.top_k],
            tenant_id=request.tenant_id,
            debug=request.debug
        )
        
        return {
            "success": True,
            "packet": packet.to_dict()
        }
        
    except Exception as e:
        logger.error(f"Error compiling context: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error compiling context: {str(e)}")


@app.get("/graph/nodes/{node_id}")
async def get_node(node_id: str):
    """
    Get a node by ID with its neighbors.
    
    Args:
        node_id: Node ID
        
    Returns:
        Node with connected edges and neighbors
    """
    if not graph_store:
        raise HTTPException(status_code=500, detail="Graph store not initialized")
    
    try:
        node = graph_store.get_node(node_id)
        if not node:
            raise HTTPException(status_code=404, detail="Node not found")
        
        edges = graph_store.get_edges_for_node(node_id)
        
        # Get neighbor nodes
        neighbor_ids = set()
        for edge in edges:
            if edge.src == node_id:
                neighbor_ids.add(edge.dst)
            else:
                neighbor_ids.add(edge.src)
        
        neighbors = []
        for neighbor_id in neighbor_ids:
            neighbor = graph_store.get_node(neighbor_id)
            if neighbor:
                neighbors.append(neighbor.to_dict())
        
        return {
            "node": node.to_dict(),
            "edges": [e.to_dict() for e in edges],
            "neighbors": neighbors
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting node: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error getting node: {str(e)}")


@app.get("/graph/trace/{request_id}")
async def get_compile_trace(request_id: str):
    """
    Get the compile trace for a request.
    
    Args:
        request_id: Request ID
        
    Returns:
        Compile trace with steps and statistics
    """
    if not graph_store:
        raise HTTPException(status_code=500, detail="Graph store not initialized")
    
    try:
        packet = graph_store.get_context_packet(request_id)
        if not packet:
            raise HTTPException(status_code=404, detail="Trace not found")
        
        return {
            "request_id": request_id,
            "trace": packet['trace']
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting trace: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error getting trace: {str(e)}")


@app.get("/graph/metrics")
async def get_graph_metrics(tenant_id: Optional[str] = None):
    """
    Get graph statistics and metrics.
    
    Args:
        tenant_id: Optional tenant filter
        
    Returns:
        Graph statistics
    """
    if not graph_store:
        raise HTTPException(status_code=500, detail="Graph store not initialized")
    
    try:
        stats = graph_store.get_graph_stats(tenant_id)
        return {
            "success": True,
            "stats": stats
        }
        
    except Exception as e:
        logger.error(f"Error getting metrics: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error getting metrics: {str(e)}")


class BackfillRequest(BaseModel):
    tenant_id: str = "default"
    limit: Optional[int] = None


@app.get("/graph/all")
async def get_full_graph(tenant_id: Optional[str] = "default"):
    """
    Get all nodes and edges for full graph visualization.
    
    Args:
        tenant_id: Optional tenant filter
        
    Returns:
        Dict with nodes, edges, and stats
    """
    if not graph_store:
        raise HTTPException(status_code=500, detail="Graph store not initialized")
    
    try:
        cursor = graph_store.conn.cursor()
        
        # Get all nodes
        if tenant_id:
            cursor.execute(
                'SELECT id, type, props, tenant_id, layer, created_at, updated_at FROM graph_nodes WHERE tenant_id = ?',
                (tenant_id,)
            )
        else:
            cursor.execute('SELECT id, type, props, tenant_id, layer, created_at, updated_at FROM graph_nodes')
        
        nodes = []
        for row in cursor.fetchall():
            nodes.append({
                'id': row[0],
                'type': row[1],
                'props': json.loads(row[2]),
                'tenant_id': row[3],
                'layer': row[4],
                'created_at': row[5],
                'updated_at': row[6]
            })
        
        # Get all active edges
        if tenant_id:
            cursor.execute(
                'SELECT id, src, dst, type, props, tenant_id, layer, created_at FROM graph_edges WHERE tenant_id = ? AND state = ?',
                (tenant_id, 'active')
            )
        else:
            cursor.execute(
                'SELECT id, src, dst, type, props, tenant_id, layer, created_at FROM graph_edges WHERE state = ?',
                ('active',)
            )
        
        edges = []
        for row in cursor.fetchall():
            edges.append({
                'id': row[0],
                'src': row[1],
                'dst': row[2],
                'type': row[3],
                'props': json.loads(row[4]),
                'tenant_id': row[5],
                'layer': row[6],
                'created_at': row[7]
            })
        
        # Get stats
        stats = graph_store.get_graph_stats(tenant_id)
        
        return {
            "success": True,
            "nodes": nodes,
            "edges": edges,
            "stats": stats,
            "count": {
                "nodes": len(nodes),
                "edges": len(edges)
            }
        }
        
    except Exception as e:
        logger.error(f"Error getting full graph: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error getting full graph: {str(e)}")


@app.post("/graph/rebuild")
async def rebuild_graph(request: BackfillRequest):
    """
    Rebuild the graph from email/attachment data.
    
    Args:
        request: Backfill request with tenant_id and optional limit
        
    Returns:
        Statistics on created nodes/edges
    """
    if not graph_store or not sqlite_handler:
        raise HTTPException(status_code=500, detail="Services not initialized")
    
    try:
        from src.context_graph.ingestion import GraphIngestion
        
        logger.info(f"Starting graph rebuild for tenant {request.tenant_id}")
        
        ingestion = GraphIngestion(sqlite_handler, graph_store)
        stats = ingestion.backfill_all(
            tenant_id=request.tenant_id,
            limit=request.limit
        )
        
        return {
            "success": True,
            "stats": stats
        }
        
    except Exception as e:
        logger.error(f"Error rebuilding graph: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error rebuilding graph: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    
    port = int(os.getenv("API_PORT", "8000"))
    logger.info(f"Starting FastAPI server on port {port}")
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port,
        log_level="info"
    )



