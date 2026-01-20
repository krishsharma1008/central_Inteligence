# Context-Graph Architecture

## Overview

This system implements a **graph-native context engine** that provides deterministic, explainable context resolution for email-based RAG queries. Instead of assembling loose text chunks, it traverses and materializes a canonical property graph to produce bounded, high-signal working contexts.

## Core Components

### 1. Canonical Context Graph

A property graph with typed nodes and edges representing the email domain:

**Nodes:**
- `User` - Email participants with authority scores
- `Conversation` - Email threads grouped by conversation_id
- `Document` - Individual emails with metadata
- `Attachment` - Email attachments with extracted text
- `Intent` - User queries/questions
- `Rule` - Business rules for scoring/filtering

**Edges:**
- `PART_OF` - Document → Conversation
- `HAS_ATTACHMENT` - Document → Attachment
- `SENT_BY` - Document → User
- `SENT_TO` - Document → User
- `FOLLOWS` - Document → Document (thread ordering)
- `MENTIONS` - Document → User (optional, for NER)
- `SEEKS_ANSWER_TO` - Intent → Conversation
- `SELECTED` - Intent → Node (compiler selections)

### 2. Storage Layer

**SQLite Tables:**
- `graph_nodes` - All nodes with type, props (JSON), tenant_id, layer, TTL
- `graph_edges` - All edges with src, dst, type, props, state
- `context_packets` - Saved compilation results for explainability

**Indexes:**
- Type, tenant_id, layer for nodes
- Src, dst, type, state for edges

### 3. Context Compiler

Deterministic compilation pipeline:

**Inputs:**
- Intent (user question)
- Constraints (tenant_id, RBAC attributes)
- Budgets (max_nodes, max_edges, max_tokens)

**Steps:**
1. **Candidate Generation** - Seed from FTS/vector search, expand via graph traversal
2. **Scoring** - Multi-signal scoring per node:
   - Recency: Exponential decay (configurable half-life)
   - Authority: User/sender reputation
   - Stage: Position in thread/journey
   - Rules: Business logic boosts/demotes
3. **Pruning** - Select top nodes within budgets, stable sort
4. **Edge Selection** - Include edges between selected nodes
5. **Lineage** - Build source attribution map

**Outputs:**
- `ContextPacket` with nodes, edges, scores, lineage, trace

### 4. Stateful Memory Model

Three graph layers with TTL-based promotion:

- **Session** - Per-query ephemeral context (Intent nodes)
- **Journey** - Per-conversation/topic context (Conversations, Documents)
- **Profile** - Long-term user/entity context (Users)

### 5. Governance

**Tenant Isolation:**
- Every node/edge carries `tenant_id`
- Compiler enforces tenant filtering

**RBAC/ABAC:**
- Session attributes checked during traversal
- Folder/category permissions

**Lineage & Attribution:**
- Nodes reference SQLite rowids and MongoDB doc IDs
- `lineage` map lists exact email_ids and attachment_filenames

## API Endpoints

### Graph Operations

**POST /graph/compile**
```json
{
  "question": "What happened with the Capillary relationship?",
  "top_k": 8,
  "tenant_id": "default",
  "debug": true
}
```
Returns: ContextPacket with nodes, edges, scores, trace

**GET /graph/nodes/{node_id}**
Returns: Node with neighbors and edges

**GET /graph/trace/{request_id}**
Returns: Compilation trace for explainability

**GET /graph/metrics?tenant_id=default**
Returns: Graph statistics (node/edge counts by type)

**POST /graph/rebuild**
```json
{
  "tenant_id": "default",
  "limit": 1000
}
```
Backfills graph from email/attachment tables

## Ingestion Pipeline

**GraphIngestion** class populates the graph:

1. Read emails from SQLite `emails` table
2. Create User nodes from senders/recipients
3. Create Conversation nodes per conversation_id
4. Create Document nodes per email
5. Create Attachment nodes from `attachments` table
6. Create edges: SENT_BY, SENT_TO, PART_OF, HAS_ATTACHMENT
7. Create FOLLOWS edges for thread ordering

**Run backfill:**
```python
from src.context_graph.ingestion import GraphIngestion
from src.context_graph.store_sqlite import GraphStoreSQLite
from src.SQLiteHandler import SQLiteHandler

sqlite_handler = SQLiteHandler("data/emails.db")
graph_store = GraphStoreSQLite("data/emails.db")
ingestion = GraphIngestion(sqlite_handler, graph_store)

stats = ingestion.backfill_all(tenant_id="default", limit=None)
print(stats)
```

Or via API:
```bash
curl -X POST http://localhost:8000/graph/rebuild \
  -H "Content-Type: application/json" \
  -d '{"tenant_id": "default"}'
```

## Visualization

### /graph Page

Interactive force-graph visualization with:

- **Node coloring** by type (Intent, Conversation, Document, User, Attachment)
- **Node sizing** by total score
- **Filters** by node type
- **Scoring breakdown** table with component scores
- **Compilation trace** with steps, counts, duration
- **Node details** panel on click

### /query Page (Debug Mode)

Add `?debug=1` to query page URL to see:
- Compact force-graph of compiled context
- Score breakdown table
- Link to full trace

## Scoring Configuration

Adjust in `web_api.py` startup:

```python
context_compiler = ContextCompiler(
    graph_store=graph_store,
    recency_half_life_days=7.0,  # Exponential decay half-life
    max_nodes=50,                 # Node budget
    max_edges=100,                # Edge budget
    max_tokens=8000               # Estimated token budget
)
```

## Integration with QueryService

The compiler integrates with existing RAG pipeline:

1. FTS search provides seed conversation IDs
2. Compiler expands and scores graph
3. Selected nodes materialized into thread-aware context
4. Context fed to Sarvam LLM for answer generation

**Future:** Replace text-based context building with structured graph prompts.

## Extensibility

### Adding New Node Types

1. Add to `NodeType` enum in `models.py`
2. Update ingestion to create nodes
3. Add scoring logic in `ContextCompiler._score_nodes()`
4. Update visualization colors in `/graph` page

### Adding New Edge Types

1. Add to `EdgeType` enum in `models.py`
2. Update ingestion to create edges
3. Update traversal logic in `ContextCompiler._gather_candidates()`

### Custom Scoring Rules

Add business rules in `ContextCompiler._score_nodes()`:

```python
# Example: Boost emails from specific senders
if node.type == NodeType.DOCUMENT:
    sender = node.props.get('sender_email', '')
    if sender in ['important@example.com']:
        score.rule_score += 5.0
```

## Observability

**Structured Logs:**
- Request ID tracking through compilation
- Step-by-step candidate/prune counts
- Duration metrics

**Context Packets:**
- Saved to `context_packets` table
- Retrieve via `/graph/trace/{request_id}`
- Full explainability: nodes, edges, scores, steps

**Metrics:**
- Node/edge counts by type
- Average scores
- Compilation durations

## Next Steps

1. **Entity Extraction** - Add NER for MENTIONS edges
2. **Neo4j Adapter** - Scale to millions of nodes
3. **Graph Prompts** - Structured prompts instead of text context
4. **Multi-tenant** - Full RBAC/ABAC enforcement
5. **TTL Promotion** - Automatic session→journey→profile promotion
6. **Rule Engine** - External rule definitions for scoring

## Files

**Backend:**
- `src/context_graph/models.py` - Core data models
- `src/context_graph/store_sqlite.py` - SQLite storage adapter
- `src/context_graph/compiler.py` - Context compilation engine
- `src/context_graph/ingestion.py` - Graph population pipeline
- `src/web_api.py` - FastAPI endpoints

**Frontend:**
- `web/app/graph/page.tsx` - Graph visualization page
- `web/app/query/page.tsx` - Query page (add debug mode)

**Database:**
- SQLite tables created automatically on first run
- Same database as email storage for lineage
