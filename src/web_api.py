"""
FastAPI server for Email RAG search.
Provides HTTP endpoints for querying emails and generating answers.
"""
import logging
import os
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


# Global service instances (initialized on startup)
query_service: Optional[QueryService] = None
sqlite_handler: Optional[SQLiteHandler] = None
mongodb_handler: Optional[MongoDBHandler] = None


@app.on_event("startup")
async def startup_event():
    """Initialize services on startup."""
    global query_service, sqlite_handler, mongodb_handler
    
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
    
    vector_reranker = VectorReranker(mongodb_handler, embedding_model)
    
    # Initialize query service
    query_service = QueryService(
        email_searcher=email_searcher,
        vector_reranker=vector_reranker,
        sarvam_client=sarvam_client,
        enable_vector_rerank=enable_vector_rerank
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
    Query emails and generate an answer.
    
    Args:
        request (QueryRequest): Query request with question and top_k
        
    Returns:
        QueryResponse: Response with answer, citations, and retrieved emails
    """
    if not query_service:
        raise HTTPException(status_code=500, detail="Query service not initialized")
    
    if not request.question or not request.question.strip():
        raise HTTPException(status_code=400, detail="Question cannot be empty")
    
    try:
        logger.info(f"Received query: {request.question}")
        result = query_service.query(request.question, top_k=request.top_k)
        return QueryResponse(**result)
    except Exception as e:
        logger.error(f"Error processing query: {str(e)}", exc_info=True)
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



