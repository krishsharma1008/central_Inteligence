#!/usr/bin/env python3
import os
import sys
import logging
from typing import Dict, Any, List, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    stream=sys.stderr
)

# Environment variables are set by the MCP config file

from datetime import datetime
from fastmcp import FastMCP, Context
from MongoDBHandler import MongoDBHandler
from SQLiteHandler import SQLiteHandler
from IMAPConnector import IMAPConnector
from EmailMetadata import EmailMetadata
from SarvamClient import SarvamClient
from debug_utils import dump_email_debug

# Initialize FastMCP server with dependencies
mcp = FastMCP(
    "outlook-email",
    dependencies=[
        "pymongo",
        "imapclient",
        "requests"
    ]
)

def validate_config(config: Dict[str, str]) -> None:
    """Validate required configuration values."""
    required_vars = [
        "MONGODB_URI",
        "SQLITE_DB_PATH",
        "SARVAM_API_KEY",
        "EMAIL_ADDRESS",
        "EMAIL_PASSWORD",
        "COLLECTION_NAME"
    ]
    missing_vars = [var for var in required_vars if not config.get(var)]
    if missing_vars:
        raise ValueError(f"Missing required configuration: {', '.join(missing_vars)}")
    
    # Set default values for optional configuration
    if "IMAP_SERVER" not in config:
        config["IMAP_SERVER"] = "outlook.office365.com"
    if "IMAP_PORT" not in config:
        config["IMAP_PORT"] = "993"

class EmailProcessor:
    def __init__(self, config: Dict[str, str]):
        """
        Initialize the email processor with configuration.
        
        Args:
            config: Dictionary containing configuration values:
                - MONGODB_URI: MongoDB connection string
                - SQLITE_DB_PATH: Path to SQLite database
                - SARVAM_API_KEY: Sarvam API key for embeddings and analysis
                - EMAIL_ADDRESS: Email address for IMAP access
                - EMAIL_PASSWORD: App password for IMAP access
                - IMAP_SERVER: IMAP server address
                - IMAP_PORT: IMAP port
                - COLLECTION_NAME: Name of the MongoDB collection to use
        """
        self.config = config
        self.collection_name = config["COLLECTION_NAME"]
        
        # Initialize embedding processor with Sarvam API key
        from tools.embedding_processor import EmbeddingProcessor
        self.embedding_processor = EmbeddingProcessor(
            db_path=config["MONGODB_URI"],
            collection_name=self.collection_name,
            sarvam_api_key=config["SARVAM_API_KEY"]
        )
        
        # Initialize SQLite handler
        self.sqlite = SQLiteHandler(config["SQLITE_DB_PATH"])
        
        # Initialize IMAP connector
        self.imap = IMAPConnector(
            email_address=config["EMAIL_ADDRESS"],
            password=config["EMAIL_PASSWORD"],
            imap_server=config.get("IMAP_SERVER", "outlook.office365.com"),
            imap_port=int(config.get("IMAP_PORT", "993"))
        )

    async def process_emails(
        self,
        start_date: str,
        end_date: str,
        mailboxes: List[str],
        ctx: Context
    ) -> Dict[str, Any]:
        """Process emails from the specified date range and mailboxes."""
        try:
            # Convert dates
            start = datetime.fromisoformat(start_date)
            end = datetime.fromisoformat(end_date)

            # Validate date range
            if (end - start).days > 30:
                raise ValueError("Date range cannot exceed 30 days")

            # Connect to IMAP and retrieve emails from inbox
            await ctx.report_progress(0, "Initializing email processing")
            
            # Connect to IMAP server
            await ctx.report_progress(5, "Connecting to IMAP server")
            self.imap.connect()
            
            # Process inbox only
            folder_names = ["Inbox"]
            
            await ctx.report_progress(10, f"Retrieving emails from {', '.join(folder_names)}")
            
            try:
                all_emails = self.imap.get_emails_within_date_range(
                    folder_names,
                    start.isoformat(),
                    end.isoformat(),
                    None
                )
                
                await ctx.report_progress(50, f"Retrieved {len(all_emails)} emails from inbox")
            except Exception as e:
                return {
                    "success": False,
                    "error": f"Failed to retrieve emails from IMAP: {str(e)}"
                }
            finally:
                # Disconnect from IMAP
                self.imap.disconnect()

            if not all_emails:
                return {
                    "success": False,
                    "error": "No emails found in any mailbox"
                }
            
            await ctx.report_progress(50, "Storing emails in SQLite")
            
            total_stored = 0
            for i, email in enumerate(all_emails):
                if self.sqlite.add_or_update_email(email):
                    total_stored += 1
                progress = 50 + (20 * (i + 1) / len(all_emails))
                await ctx.report_progress(progress, f"Storing email {i+1}/{len(all_emails)}")
            
            if total_stored == 0:
                return {
                    "success": False,
                    "error": "Failed to store any emails in SQLite"
                }
            
            unprocessed = self.sqlite.get_unprocessed_emails()
            email_dicts = [email for email in unprocessed]
            
            await ctx.report_progress(70, "Processing embeddings")
            
            if not email_dicts:
                return {
                    "success": True,
                    "processed_count": 0,
                    "message": "No new emails to process"
                }
            
            total_processed, total_failed = self.embedding_processor.process_batch(email_dicts)
            await ctx.report_progress(90, "Finalizing processing")
            
            for email in email_dicts[:total_processed]:
                self.sqlite.mark_as_processed(email['id'])
            
            result = {
                "success": True,
                "processed_count": total_processed,
                "message": (f"Successfully processed {total_processed} emails "
                          f"(retrieved: {len(all_emails)}, stored: {total_stored}, "
                          f"failed: {total_failed})")
            }
            await ctx.report_progress(100, "Processing complete")
            return result
            
        except Exception as e:
            return {
                "success": False,
                "error": str(e)
            }
            
        # Note: We don't close connections here because they might be needed for future operations
        # Connections will be closed by the atexit handler when the server shuts down

try:
    # Load configuration from environment
    config = {
        "MONGODB_URI": os.environ.get("MONGODB_URI"),
        "SQLITE_DB_PATH": os.environ.get("SQLITE_DB_PATH"),
        "SARVAM_API_KEY": os.environ.get("SARVAM_API_KEY"),
        "EMAIL_ADDRESS": os.environ.get("EMAIL_ADDRESS"),
        "EMAIL_PASSWORD": os.environ.get("EMAIL_PASSWORD"),
        "IMAP_SERVER": os.environ.get("IMAP_SERVER", "outlook.office365.com"),
        "IMAP_PORT": os.environ.get("IMAP_PORT", "993"),
        "COLLECTION_NAME": os.environ.get("COLLECTION_NAME")
    }

    # Log environment variables for debugging
    logging.info("Environment variables:")
    # Redact sensitive information
    mongodb_uri = os.environ.get('MONGODB_URI', '')
    if mongodb_uri:
        # Simple redaction that keeps the host but hides credentials
        redacted_uri = mongodb_uri
        if '@' in mongodb_uri:
            # Format is typically mongodb://username:password@host:port/db
            redacted_uri = 'mongodb://' + mongodb_uri.split('@', 1)[1]
        logging.info(f"MONGODB_URI: {redacted_uri}")
    logging.info(f"SQLITE_DB_PATH: {os.environ.get('SQLITE_DB_PATH')}")
    logging.info(f"SARVAM_API_KEY: {'***' + os.environ.get('SARVAM_API_KEY', '')[-4:] if os.environ.get('SARVAM_API_KEY') else 'Not set'}")
    logging.info(f"EMAIL_ADDRESS: {os.environ.get('EMAIL_ADDRESS')}")
    logging.info(f"EMAIL_PASSWORD: {'***' if os.environ.get('EMAIL_PASSWORD') else 'Not set'}")
    logging.info(f"IMAP_SERVER: {os.environ.get('IMAP_SERVER', 'outlook.office365.com')}")
    logging.info(f"IMAP_PORT: {os.environ.get('IMAP_PORT', '993')}")
    logging.info(f"COLLECTION_NAME: {os.environ.get('COLLECTION_NAME')}")
    
    # Validate configuration
    validate_config(config)
    
    
    processor = EmailProcessor(config)
    
except Exception as e:
    raise

# Register cleanup handler for server shutdown
import atexit

def cleanup_resources():
    """Clean up resources when the server shuts down."""
    try:
        if 'processor' in globals():
            # Close SQLite connection
            if hasattr(processor, 'sqlite'):
                processor.sqlite.close()
                logging.info("SQLite connection closed during shutdown")
            
            # Close MongoDB connection
            if hasattr(processor, 'embedding_processor') and hasattr(processor.embedding_processor, 'mongodb_handler'):
                processor.embedding_processor.mongodb_handler.close()
                logging.info("MongoDB connection closed during shutdown")
            
            # Disconnect from IMAP
            if hasattr(processor, 'imap'):
                processor.imap.disconnect()
                logging.info("IMAP connection closed during shutdown")
    except Exception as e:
        logging.error(f"Error during cleanup: {str(e)}")

atexit.register(cleanup_resources)

@mcp.tool()
async def process_emails(
    start_date: str,
    end_date: str,
    mailboxes: List[str],
    ctx: Context = None
) -> str:
    """Process emails from specified date range from your inbox.
    
    Args:
        start_date: Start date in ISO format (YYYY-MM-DD)
        end_date: End date in ISO format (YYYY-MM-DD)
        mailboxes: List parameter (kept for compatibility, but inbox is always used)
    """
    try:
        # Validate date formats
        try:
            datetime.fromisoformat(start_date)
            datetime.fromisoformat(end_date)
        except ValueError:
            return "Error: Dates must be in ISO format (YYYY-MM-DD)"
            
        result = await processor.process_emails(start_date, end_date, mailboxes, ctx)
        if result["success"]:
            return result["message"]
        else:
            return f"Error processing emails: {result['error']}"
    except Exception as e:
        return f"Error: {str(e)}"

if __name__ == "__main__":
    # Run the server
    mcp.run()
