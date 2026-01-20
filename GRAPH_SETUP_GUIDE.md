# Context Graph Setup & Usage Guide

## Quick Start

### 1. Install Dependencies

**Backend:**
```bash
# Already installed via existing requirements
# The context graph uses existing dependencies
```

**Frontend:**
```bash
cd web
npm install
# This will install react-force-graph-2d and other dependencies
```

### 2. Initialize the Graph

The graph tables are created automatically when the API starts. To populate the graph from existing emails:

**Option A: Via API (Recommended)**
```bash
# Start the API server
python -m src.web_api

# In another terminal, trigger backfill
curl -X POST http://localhost:8000/graph/rebuild \
  -H "Content-Type: application/json" \
  -d '{"tenant_id": "default", "limit": 1000}'
```

**Option B: Via Python Script**
```python
from src.SQLiteHandler import SQLiteHandler
from src.context_graph.store_sqlite import GraphStoreSQLite
from src.context_graph.ingestion import GraphIngestion
import os
from dotenv import load_dotenv

load_dotenv()

# Initialize handlers
sqlite_path = os.getenv("SQLITE_DB_PATH")
sqlite_handler = SQLiteHandler(sqlite_path)
graph_store = GraphStoreSQLite(sqlite_path)

# Run ingestion
ingestion = GraphIngestion(sqlite_handler, graph_store)
stats = ingestion.backfill_all(tenant_id="default", limit=None)

print(f"Graph populated: {stats}")
# Output: {'users': 45, 'conversations': 120, 'documents': 350, 'attachments': 28, 'edges': 892}
```

### 3. Start the Services

**Backend:**
```bash
python -m src.web_api
# API runs on http://localhost:8000
```

**Frontend:**
```bash
cd web
npm run dev
# UI runs on http://localhost:3000
```

### 4. Access the Visualization

Navigate to: `http://localhost:3000/graph`

## Usage Examples

### Compile Context for a Query

**Via API:**
```bash
curl -X POST http://localhost:8000/graph/compile \
  -H "Content-Type: application/json" \
  -d '{
    "question": "What happened with the Capillary relationship?",
    "top_k": 8,
    "tenant_id": "default",
    "debug": true
  }'
```

**Response:**
```json
{
  "success": true,
  "packet": {
    "request_id": "abc-123",
    "nodes": [...],
    "edges": [...],
    "scores": {
      "doc_email123": {
        "recency_score": 8.5,
        "authority_score": 5.0,
        "stage_score": 2.0,
        "rule_score": 0.0,
        "total_score": 15.5
      }
    },
    "trace": {
      "candidate_count": 45,
      "pruned_count": 20,
      "final_count": 25,
      "duration_ms": 125.3
    }
  }
}
```

### Get Node Details

```bash
curl http://localhost:8000/graph/nodes/conv_abc123
```

### Get Compilation Trace

```bash
curl http://localhost:8000/graph/trace/abc-123
```

### Get Graph Metrics

```bash
curl http://localhost:8000/graph/metrics?tenant_id=default
```

**Response:**
```json
{
  "success": true,
  "stats": {
    "nodes_by_type": {
      "User": 45,
      "Conversation": 120,
      "Document": 350,
      "Attachment": 28
    },
    "edges_by_type": {
      "SENT_BY": 350,
      "SENT_TO": 680,
      "PART_OF": 350,
      "HAS_ATTACHMENT": 28,
      "FOLLOWS": 230
    },
    "total_nodes": 543,
    "total_edges": 1638
  }
}
```

## Visualization Features

### Graph Page (`/graph`)

**Features:**
1. **Interactive Force Graph** - Drag nodes, zoom, pan
2. **Node Filtering** - Filter by type (Intent, Conversation, Document, User, Attachment)
3. **Scoring Breakdown** - Table showing component scores for all nodes
4. **Compilation Trace** - Step-by-step compilation process
5. **Node Details** - Click any node to see properties and lineage

**Controls:**
- **Type Filter** - Dropdown to show only specific node types
- **Show/Hide Scores** - Toggle scoring breakdown table
- **Show/Hide Trace** - Toggle compilation trace panel

**Node Colors:**
- 🔴 Intent - Red
- 🔵 Conversation - Teal
- 🔵 Document - Blue
- 🟢 User - Green
- 🟡 Attachment - Yellow

**Node Size:** Proportional to total score

### Query Page Debug Mode

Add a debug toggle to the existing query page to show graph context alongside answers.

## Configuration

### Compiler Settings

Edit `src/web_api.py` startup event:

```python
context_compiler = ContextCompiler(
    graph_store=graph_store,
    recency_half_life_days=7.0,    # Adjust decay rate
    max_nodes=50,                   # Increase for more context
    max_edges=100,                  # Increase for more relationships
    max_tokens=8000                 # Adjust token budget
)
```

### Scoring Weights

Modify `src/context_graph/compiler.py` in `_score_nodes()`:

```python
# Adjust recency decay
age_days = (now - received_time).total_seconds() / 86400
score.recency_score = math.exp(-age_days / self.recency_half_life_days) * 10.0

# Adjust authority by domain
if sender_email.endswith('@important-domain.com'):
    score.authority_score = 10.0

# Add custom rules
if 'urgent' in subject.lower():
    score.rule_score += 5.0
```

### Tenant Configuration

Set tenant_id based on your mailbox/account structure:

```python
# In ingestion
stats = ingestion.backfill_all(
    tenant_id="team_sales",  # Use account name or team
    limit=None
)

# In compilation
packet = context_compiler.compile(
    intent_text=question,
    seed_node_ids=seeds,
    tenant_id="team_sales"  # Must match
)
```

## Monitoring & Debugging

### Check Graph Health

```bash
curl http://localhost:8000/graph/metrics
```

### View Compilation Traces

All compilations are saved to `context_packets` table. Query via:

```python
from src.context_graph.store_sqlite import GraphStoreSQLite

graph_store = GraphStoreSQLite("data/emails.db")
packet = graph_store.get_context_packet("request-id-123")
print(packet['trace'])
```

### Inspect Node Lineage

```python
node = graph_store.get_node("doc_email123")
print(f"Email ID: {node.props['email_id']}")
print(f"SQLite rowid: {node.props['sqlite_rowid']}")
```

### View Logs

The compiler logs detailed information:

```
INFO: Gathering candidates from 8 seed nodes
INFO: Scoring 45 candidates
INFO: Selected 25/45 nodes, ~6500 tokens
INFO: Compiled context: 25 nodes, 48 edges in 125.3ms
```

## Performance Optimization

### Indexing

The graph store creates indexes automatically:
- `idx_nodes_type`, `idx_nodes_tenant`, `idx_nodes_layer`
- `idx_edges_src`, `idx_edges_dst`, `idx_edges_type`

### Batch Operations

For large backfills, use limits:

```python
# Process in batches
for offset in range(0, total_emails, 1000):
    stats = ingestion.backfill_all(
        tenant_id="default",
        limit=1000
    )
```

### Caching

Context packets are cached in `context_packets` table. Reuse for identical queries:

```python
# Check if packet exists
existing = graph_store.get_context_packet(request_id)
if existing:
    return existing
```

## Troubleshooting

### Graph tables not created

Check SQLite permissions and path:
```python
import os
db_path = os.getenv("SQLITE_DB_PATH")
print(f"Database path: {db_path}")
print(f"Exists: {os.path.exists(db_path)}")
```

### No nodes after backfill

Check email data:
```python
sqlite_handler = SQLiteHandler(db_path)
count = sqlite_handler.get_email_count()
print(f"Emails in database: {count}")
```

### Compilation returns empty context

Check seed nodes:
```python
# Verify conversation IDs exist
cursor = sqlite_handler.conn.cursor()
cursor.execute("SELECT DISTINCT conversation_id FROM emails WHERE conversation_id IS NOT NULL LIMIT 10")
print(cursor.fetchall())
```

### Frontend graph not rendering

1. Install dependencies: `cd web && npm install`
2. Check console for errors
3. Verify API is running: `curl http://localhost:8000/health`

## Advanced Usage

### Custom Node Types

Add entity extraction for mentions:

```python
# In ingestion.py
def _extract_mentions(self, email_body):
    # Use NER to extract person/org names
    entities = ner_model.extract(email_body)
    for entity in entities:
        mention_node = Node(
            id=f"mention_{hash(entity)}",
            type=NodeType.MENTION,
            props={'name': entity},
            tenant_id=tenant_id
        )
        self.graph.add_node(mention_node)
```

### Multi-Tenant Setup

```python
# Backfill per tenant
for account in ['sales@company.com', 'support@company.com']:
    tenant_id = account.split('@')[0]
    ingestion.backfill_all(tenant_id=tenant_id)

# Query with tenant isolation
packet = compiler.compile(
    intent_text=question,
    seed_node_ids=seeds,
    tenant_id="sales"  # Only sees sales@ emails
)
```

### Graph Export

Export graph for analysis:

```python
import json

# Get all nodes and edges
nodes = graph_store.get_nodes_by_type(NodeType.DOCUMENT)
edges = []
for node in nodes:
    edges.extend(graph_store.get_edges_for_node(node.id))

# Export to JSON
graph_data = {
    'nodes': [n.to_dict() for n in nodes],
    'edges': [e.to_dict() for e in edges]
}

with open('graph_export.json', 'w') as f:
    json.dump(graph_data, f, indent=2)
```

## Next Steps

1. **Run backfill** to populate your graph
2. **Visit `/graph`** to visualize the context engine
3. **Experiment with scoring** parameters
4. **Add custom rules** for your domain
5. **Integrate with query service** for graph-aware RAG

## Support

For issues or questions:
- Check `GRAPH_ARCHITECTURE.md` for design details
- Review API logs for compilation traces
- Inspect `context_packets` table for explainability
