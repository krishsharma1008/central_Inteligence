[![MseeP.ai Security Assessment Badge](https://mseep.net/pr/cam10001110101-mcp-server-outlook-email-badge.png)](https://mseep.ai/app/cam10001110101-mcp-server-outlook-email)

# Email Processing MCP Server

This MCP server provides email processing capabilities with MongoDB integration for semantic search, Sarvam AI for embeddings and email analysis, and SQLite for efficient storage and retrieval.

## Features

- Process emails from Outlook/Office 365 via IMAP with date range filtering
- Cross-platform support (macOS, Linux, Windows)
- Store emails in SQLite database with proper connection management
- Generate vector embeddings using Sarvam AI API
- Automatic email analysis and summarization using Sarvam AI
- Inbox-only processing for focused email management
- Structured email analysis with sentiment, categorization, and action items

## Upcoming Features

- Email search with semantic capabilities
- Advanced filtering options
- Customizable email reports
- Email drafting suggestions
- Rule suggestions based on email patterns
- Expanded database options with Neo4j and ChromaDB integration

## Prerequisites

- Python 3.10 or higher
- Sarvam AI API key (get from [Sarvam AI Dashboard](https://dashboard.sarvam.ai))
- Outlook/Office 365 email account with IMAP access enabled
- App password for IMAP authentication (see setup instructions below)
- MongoDB server (for storing embeddings and analysis results)
- Cross-platform compatible (macOS, Linux, Windows)

## Installation

1. Install uv (if not already installed):
  ```bash
  pip install uv
  ```

2. Create a virtual environment:
  ```bash
  uv venv .venv
  ```

3. Activate the virtual environment:  
   
   Windows: 

    ```
    .venv\Scripts\activate
    ```

   
    macOS/Linux: 

    ```bash
    source .venv/bin/activate
    ```

4. Install dependencies:
```bash
uv pip install -e .
```

5. Install the fastmcp package:
```bash
uv pip install fastmcp
```

## Setting Up IMAP Access for Outlook/Office 365

### Step 1: Enable IMAP in Outlook Settings

1. Sign in to your Outlook account at [outlook.office365.com](https://outlook.office365.com)
2. Go to **Settings** (gear icon) → **View all Outlook settings**
3. Navigate to **Mail** → **Sync email**
4. Under **POP and IMAP**, ensure **Let devices and apps use IMAP** is enabled
5. Click **Save**

### Step 2: Create an App Password

For security, you need to create an app-specific password:

1. Go to [Microsoft Account Security](https://account.microsoft.com/security)
2. Sign in with your Outlook/Office 365 account
3. Navigate to **Security** → **Advanced security options**
4. Under **App passwords**, click **Create a new app password**
5. Copy the generated password (you'll need this for configuration)

**Note**: If you don't see the app password option, you may need to enable two-factor authentication first.

### Step 3: Get Your Sarvam AI API Key

1. Visit [Sarvam AI Dashboard](https://dashboard.sarvam.ai)
2. Sign up or log in to your account
3. Navigate to **API Keys** section
4. Create a new API key or copy your existing key
5. Save this key securely (you'll need it for configuration)


## Configuration

Add the server to your Claude for Desktop configuration file:

- Windows: `%APPDATA%\Claude\claude_desktop_config.json`
- macOS: `~/Library/Application Support/Claude/claude_desktop_config.json`

### Windows Configuration Example

```json
{
  "mcpServers": {
    "outlook-email": {
      "command": "C:/Users/username/path/to/central-inteligence/.venv/Scripts/python",
      "args": [
        "C:/Users/username/path/to/central-inteligence/src/mcp_server.py"
      ],
      "env": {
        "MONGODB_URI": "mongodb://localhost:27017/MCP?authSource=admin",
        "SQLITE_DB_PATH": "C:\\Users\\username\\path\\to\\central-inteligence\\data\\emails.db",
        "SARVAM_API_KEY": "sk_mghaeht5_ilQquPT4b9KSkC4iIlfRLfwZ",
        "EMAIL_ADDRESS": "your-email@outlook.com",
        "EMAIL_PASSWORD": "your-app-password",
        "IMAP_SERVER": "outlook.office365.com",
        "IMAP_PORT": "993",
        "COLLECTION_NAME": "outlook-emails"
      }
    }
  }
}
```

### macOS/Linux Configuration Example

```json
{
  "mcpServers": {
    "outlook-email": {
      "command": "/Users/username/path/to/central-inteligence/.venv/bin/python",
      "args": [
        "/Users/username/path/to/central-inteligence/src/mcp_server.py"
      ],
      "env": {
        "MONGODB_URI": "mongodb://localhost:27017/MCP?authSource=admin",
        "SQLITE_DB_PATH": "/Users/username/path/to/central-inteligence/data/emails.db",
        "SARVAM_API_KEY": "sk_mghaeht5_ilQquPT4b9KSkC4iIlfRLfwZ",
        "EMAIL_ADDRESS": "your-email@outlook.com",
        "EMAIL_PASSWORD": "your-app-password",
        "IMAP_SERVER": "outlook.office365.com",
        "IMAP_PORT": "993",
        "COLLECTION_NAME": "outlook-emails"
      }
    }
  }
}
```

### Configuration Fields Explained

- `command`: Full path to the Python executable in your virtual environment
- `args`: Array containing the full path to the MCP server script
- `env`: Environment variables for configuration
  - `MONGODB_URI`: MongoDB connection string (required)
  - `SQLITE_DB_PATH`: Absolute path to SQLite database file (required)
  - `SARVAM_API_KEY`: Your Sarvam AI API subscription key (required)
  - `EMAIL_ADDRESS`: Your Outlook/Office 365 email address (required)
  - `EMAIL_PASSWORD`: App password for IMAP access (required - see setup instructions above)
  - `IMAP_SERVER`: IMAP server address (optional, default: `outlook.office365.com`)
  - `IMAP_PORT`: IMAP port (optional, default: `993`)
  - `COLLECTION_NAME`: Name of the MongoDB collection to use (required)

**Important Notes:**
- Replace all paths with actual paths on your system
- Windows paths in the `env` section should use double backslashes (`\\`)
- Use forward slashes (`/`) in the `command` and `args` fields on all platforms
- Never commit your API keys or passwords to version control
- Store sensitive credentials securely

## Available Tools

### 1. process_emails
Process emails from your inbox for a specified date range:
```python
{
  "start_date": "2024-01-01",    # ISO format date (YYYY-MM-DD)
  "end_date": "2024-02-15",      # ISO format date (YYYY-MM-DD)
  "mailboxes": ["Inbox"]         # Parameter kept for compatibility (inbox is always used)
}
```

The tool will:
1. Connect to your Outlook/Office 365 account via IMAP
2. Retrieve emails from your Inbox folder only
3. Store emails in SQLite database
4. Generate embeddings using Sarvam AI
5. Analyze emails using Sarvam AI (summary, sentiment, category, action items)
6. Store embeddings and analysis results in MongoDB


## Example Usage in Claude

```
"Process emails from February 1st to February 17th from my inbox"
```

The system will automatically analyze each email and provide:
- **Summary**: Brief 2-3 sentence overview
- **Key Points**: Main topics discussed in bullet points
- **Sentiment**: Positive, negative, or neutral
- **Category**: Work, personal, spam, marketing, etc.
- **Action Items**: Tasks or follow-ups needed

## Architecture

The server uses a hybrid approach with AI-powered analysis:

1. **IMAP Connector**:
   - Cross-platform email access via IMAP protocol
   - Connects to Outlook/Office 365 securely
   - Retrieves emails from inbox with date filtering
   - Parses email content (subject, body, sender, recipients)

2. **SQLite Database**:
   - Primary email storage
   - Full-text search capabilities
   - Processing status tracking
   - Efficient filtering
   - Directory is created automatically if it doesn't exist
   - Connections are properly closed to prevent database locking

3. **Sarvam AI Integration**:
   - Vector embeddings generation for semantic search
   - Email analysis and summarization
   - Sentiment analysis and categorization
   - Action item extraction
   - Rate limiting and retry logic

4. **MongoDB Storage**:
   - Vector embeddings storage
   - Email analysis results
   - Semantic similarity search
   - Metadata filtering
   - Efficient retrieval
   - Structured JSON storage for analysis

## Data Flow

```
Inbox (IMAP) → Email Parsing → SQLite Storage → Sarvam AI Processing → MongoDB Storage
                                                     ↓
                                            (Embeddings + Analysis)
```

## Error Handling

The server provides detailed error messages for common issues:
- Invalid date formats
- IMAP connection issues (authentication, server errors)
- MongoDB errors
- Sarvam AI API errors (rate limiting, authentication)
- Embedding generation failures with retry logic
- Email analysis failures with fallback responses
- SQLite storage errors
- Network connectivity issues with automatic retries

## Resource Management

The server implements proper resource management to prevent issues:
- Database connections (SQLite and MongoDB) are kept open during the server's lifetime to prevent "Cannot operate on a closed database" errors
- IMAP connections are established per request and properly closed after use
- Connections are only closed when the server shuts down, using an atexit handler
- Destructors and context managers are used as a fallback to ensure connections are closed when objects are garbage collected
- Connection management is designed to balance resource usage with operational reliability
- Robust retry logic for external services (Sarvam AI, IMAP) to handle temporary connection issues
- Rate limiting protection for API calls to Sarvam AI

## Security Notes

- **Authentication**: Uses app-specific passwords for IMAP access (more secure than regular passwords)
- **Data Storage**: All email data is stored locally (SQLite) and in MongoDB
- **API Access**: External API calls only to Sarvam AI for embeddings and analysis
- **Credentials**: API keys and passwords are stored in environment variables, never in code
- **User Approval**: Requires explicit user approval for email processing
- **Privacy**: No sensitive email data is exposed through the MCP interface
- **Encryption**: IMAP connection uses SSL/TLS (port 993)
- **App Passwords**: Two-factor authentication recommended for Outlook accounts

**Best Practices**:
- Never commit API keys or passwords to version control
- Use `.env` files or secure vaults for credential management
- Rotate app passwords periodically
- Monitor API usage to detect unusual activity
- Review processed emails regularly

## Debugging

If you encounter issues:

### IMAP Connection Issues
1. Verify IMAP is enabled in your Outlook settings
2. Ensure you're using an app password, not your regular password
3. Check that the IMAP server and port are correct (`outlook.office365.com:993`)
4. Verify your email address is correct

### Sarvam AI Issues
1. Verify your API key is correct and active
2. Check your API usage limits on the Sarvam AI dashboard
3. Monitor the logs for rate limiting errors
4. Ensure you have network connectivity to Sarvam AI servers

### Database Issues
1. Verify MongoDB is running and accessible
2. Check that the SQLite database path exists and is writable
3. Review connection strings for typos or incorrect credentials

### General Troubleshooting
1. Check the logs in stderr for detailed error messages
2. Verify all required environment variables are set
3. Test with a small date range first (1-2 days)
4. Ensure Python dependencies are installed correctly

### Testing IMAP Connection
You can test your IMAP credentials separately:
```python
import imaplib
mail = imaplib.IMAP4_SSL('outlook.office365.com', 993)
mail.login('your-email@outlook.com', 'your-app-password')
print("Connection successful!")
mail.logout()
```
