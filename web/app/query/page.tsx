'use client'

import Link from 'next/link'
import { FormEvent, useState } from 'react'
import dynamic from 'next/dynamic'
import HierarchicalGraph from '../components/HierarchicalGraph'

const ForceGraph2D = dynamic(() => import('react-force-graph-2d'), { ssr: false })

const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'

interface Citation {
  id: string
  subject: string
  snippet: string
  sender_name?: string
  received_time?: string
}

interface RetrievedEmail {
  id: string
  subject: string
  sender_name: string
  sender_email: string
  received_time: string
  body: string
}

interface ContextPacket {
  request_id: string
  intent_node_id: string
  nodes: Array<any>
  edges: Array<any>
  scores: Record<string, any>
  lineage: Record<string, string[]>
  trace: {
    request_id: string
    steps: Array<any>
    candidate_count: number
    pruned_count: number
    final_count: number
    duration_ms: number
  }
}

interface QueryResponse {
  success: boolean
  answer: string
  citations: Citation[]
  retrieved_emails: RetrievedEmail[]
  context_packet?: ContextPacket | null
}

interface FullGraphData {
  success: boolean
  nodes: Array<any>
  edges: Array<any>
  stats: any
  count: {
    nodes: number
    edges: number
  }
}

export default function QueryPage() {
  const [question, setQuestion] = useState('')
  const [topK, setTopK] = useState(8)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [response, setResponse] = useState<QueryResponse | null>(null)
  
  // Graph visualization state
  const [graphMode, setGraphMode] = useState<'question' | 'full'>('question')
  const [layoutType, setLayoutType] = useState<'hierarchical' | 'force'>('hierarchical')
  const [fullGraphData, setFullGraphData] = useState<FullGraphData | null>(null)
  const [loadingFullGraph, setLoadingFullGraph] = useState(false)
  const [selectedNode, setSelectedNode] = useState<any>(null)

  const runQuery = async () => {
    if (!question.trim()) {
      setError('Please enter a question to search your emails.')
      return
    }

    setLoading(true)
    setError(null)
    setResponse(null)
    setGraphMode('question') // Reset to question graph

    try {
      const res = await fetch(`${API_BASE_URL}/query`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ question, top_k: topK }),
      })

      if (!res.ok) {
        throw new Error(`API error: ${res.status}`)
      }

      const data = (await res.json()) as QueryResponse
      setResponse(data)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Something went wrong')
    } finally {
      setLoading(false)
    }
  }

  const loadFullGraph = async () => {
    if (fullGraphData) {
      setGraphMode('full')
      return
    }

    setLoadingFullGraph(true)
    try {
      const res = await fetch(`${API_BASE_URL}/graph/all?tenant_id=default`)
      if (!res.ok) {
        throw new Error(`API error: ${res.status}`)
      }
      const data = (await res.json()) as FullGraphData
      setFullGraphData(data)
      setGraphMode('full')
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load full graph')
    } finally {
      setLoadingFullGraph(false)
    }
  }

  const handleSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault()
    if (loading) return
    await runQuery()
  }

  const getForceGraphData = (packet: ContextPacket) => {
    const nodes = packet.nodes.map(n => ({
      id: n.id,
      name: n.props.subject || n.props.display_name || n.props.email || n.props.filename || n.type,
      type: n.type,
      layer: n.layer,
      score: packet.scores[n.id]?.total_score || 0,
      props: n.props
    }))

    const nodeIds = new Set(nodes.map(n => n.id))
    const links = packet.edges
      .filter((e: any) => nodeIds.has(e.src) && nodeIds.has(e.dst))
      .map((e: any) => ({
        source: e.src,
        target: e.dst,
        type: e.type,
        id: e.id
      }))

    return { nodes, links }
  }

  const getNodeColor = (node: any) => {
    const colors: Record<string, string> = {
      Intent: '#ff6b6b',
      Conversation: '#4ecdc4',
      Document: '#45b7d1',
      User: '#96ceb4',
      Attachment: '#ffeaa7',
      Rule: '#dfe6e9'
    }
    return colors[node.type] || '#95a5a6'
  }

  const currentGraphData = graphMode === 'question' && response?.context_packet
    ? response.context_packet
    : graphMode === 'full' && fullGraphData
    ? { nodes: fullGraphData.nodes, edges: fullGraphData.edges, scores: {}, trace: { duration_ms: 0, candidate_count: 0, pruned_count: 0, final_count: 0 } }
    : null

  const sortedScores = response?.context_packet
    ? Object.values(response.context_packet.scores).sort((a: any, b: any) => b.total_score - a.total_score)
    : []

  return (
    <main className="dashboard">
      <div className="dashboard-bg">
        <div className="orb orb-a" />
        <div className="orb orb-b" />
        <div className="orb orb-c" />
        <div className="grid-overlay" />
      </div>

      <div className="dashboard-content">
        <nav className="nav">
          <div className="nav-title">Dashboard views</div>
          <div className="nav-links">
            <Link className="nav-link" href="/">
              Capillary relationship
            </Link>
            <Link className="nav-link" href="/csat-response">
              CSAT response
            </Link>
            <Link className="nav-link active" href="/query">
              Ask & Explore
            </Link>
          </div>
        </nav>

        <section className="card query-shell" style={{
          background: 'linear-gradient(135deg, rgba(255, 255, 255, 0.95), rgba(255, 250, 242, 0.95))',
          backdropFilter: 'blur(20px)',
          border: '1px solid rgba(31, 122, 140, 0.15)',
          boxShadow: '0 20px 60px rgba(15, 23, 42, 0.08), 0 0 40px rgba(31, 122, 140, 0.05)',
          borderRadius: '20px'
        }}>
          <div className="section-title" style={{
            fontSize: '2rem',
            fontWeight: 700,
            background: 'linear-gradient(135deg, #1f7a8c, #3fa37b)',
            WebkitBackgroundClip: 'text',
            WebkitTextFillColor: 'transparent',
            marginBottom: '0.5rem'
          }}>
            ✨ Ask your mailbox
          </div>
          <p className="hero-subtitle" style={{
            fontSize: '1rem',
            color: 'var(--muted)',
            marginBottom: '1.5rem'
          }}>
            Submit a natural-language question.{" "}
            <span className="query-hint" style={{
              display: 'inline-block',
              padding: '0.25rem 0.75rem',
              background: 'rgba(31, 122, 140, 0.1)',
              borderRadius: '6px',
              fontSize: '0.875rem',
              color: 'var(--accent)'
            }}>💡 Press Enter to send, Shift + Enter for new line</span>
          </p>

          <form className="query-form" onSubmit={handleSubmit}>
            <div className="query-chat">
              <textarea
                id="question"
                value={question}
                onChange={(event) => setQuestion(event.target.value)}
                onKeyDown={(event) => {
                  if (event.key === 'Enter' && !event.shiftKey) {
                    event.preventDefault()
                    if (!loading) {
                      runQuery()
                    }
                  }
                }}
                placeholder="Ask anything about your synced inbox… 💬"
                rows={5}
                className="query-textarea"
                style={{
                  fontSize: '1.125rem',
                  lineHeight: '1.6',
                  padding: '1.5rem',
                  borderRadius: '16px',
                  border: '2px solid rgba(31, 122, 140, 0.2)',
                  background: 'rgba(255, 255, 255, 0.7)',
                  transition: 'all 0.3s ease',
                  resize: 'vertical'
                }}
              />

              <div className="query-sendbar" style={{
                display: 'flex',
                alignItems: 'center',
                gap: '1.5rem',
                marginTop: '1rem',
                padding: '1rem',
                background: 'rgba(31, 122, 140, 0.05)',
                borderRadius: '12px'
              }}>
                <div className="query-topk" style={{ flex: 1 }}>
                  <label className="query-label" htmlFor="topk" style={{
                    fontSize: '0.875rem',
                    fontWeight: 600,
                    color: 'var(--accent)',
                    marginBottom: '0.5rem',
                    display: 'block'
                  }}>
                    📊 Context Threads: {topK}
                  </label>
                  <input
                    id="topk"
                    type="range"
                    min={1}
                    max={20}
                    value={topK}
                    onChange={(event) => setTopK(Number(event.target.value))}
                    style={{
                      width: '100%',
                      height: '8px',
                      borderRadius: '4px',
                      background: 'linear-gradient(to right, #1f7a8c 0%, #1f7a8c ' + (topK * 5) + '%, rgba(31, 122, 140, 0.2) ' + (topK * 5) + '%, rgba(31, 122, 140, 0.2) 100%)',
                      outline: 'none',
                      cursor: 'pointer'
                    }}
                  />
                </div>

                <button 
                  className="query-button" 
                  disabled={loading} 
                  type="submit"
                  style={{
                    padding: '1rem 2.5rem',
                    fontSize: '1rem',
                    fontWeight: 600,
                    borderRadius: '12px',
                    background: loading ? 'rgba(31, 122, 140, 0.3)' : 'linear-gradient(135deg, #1f7a8c, #3fa37b)',
                    border: 'none',
                    color: 'white',
                    cursor: loading ? 'not-allowed' : 'pointer',
                    transition: 'all 0.3s ease',
                    boxShadow: loading ? 'none' : '0 8px 20px rgba(31, 122, 140, 0.3)',
                    transform: loading ? 'scale(0.98)' : 'scale(1)',
                    display: 'flex',
                    alignItems: 'center',
                    gap: '0.5rem',
                    whiteSpace: 'nowrap'
                  }}
                >
                  {loading ? '⏳ Searching…' : '🚀 Send'}
                </button>
              </div>
            </div>
          </form>

          {error && <p className="query-error" style={{
            marginTop: '1rem',
            padding: '1rem 1.5rem',
            background: 'rgba(229, 107, 111, 0.1)',
            border: '1px solid rgba(229, 107, 111, 0.3)',
            borderRadius: '12px',
            color: 'var(--accent-3)',
            fontSize: '0.875rem'
          }}>{error}</p>}
        </section>

        {response && (
          <>
          <section className="query-result">
              <article className="card answer-card fade-in" style={{
                background: 'linear-gradient(135deg, rgba(255, 255, 255, 0.98), rgba(207, 232, 224, 0.15))',
                backdropFilter: 'blur(20px)',
                border: '2px solid rgba(63, 163, 123, 0.2)',
                boxShadow: '0 20px 60px rgba(15, 23, 42, 0.1), 0 0 40px rgba(63, 163, 123, 0.08)',
                borderRadius: '20px',
                padding: '2rem'
              }}>
                <header style={{ marginBottom: '1.5rem' }}>
                  <div style={{ flex: 1 }}>
                    <span className="chip" style={{
                      display: 'inline-block',
                      padding: '0.5rem 1rem',
                      background: 'linear-gradient(135deg, #3fa37b, #1f7a8c)',
                      color: 'white',
                      borderRadius: '10px',
                      fontSize: '0.875rem',
                      fontWeight: 600,
                      marginBottom: '1rem',
                      boxShadow: '0 4px 12px rgba(63, 163, 123, 0.3)'
                    }}>✨ Answer</span>
                    <h2 style={{
                      fontSize: '1.75rem',
                      fontWeight: 700,
                      color: 'var(--ink)',
                      marginTop: '0.5rem'
                    }}>Your synthesized response</h2>
                  </div>
              </header>
                <p className="answer-text" style={{
                  fontSize: '1.125rem',
                  lineHeight: '1.8',
                  color: 'var(--ink)',
                  whiteSpace: 'pre-wrap'
                }}>{response.answer}</p>
            </article>

              <article className="card citations-card fade-in" style={{ 
                animationDelay: '0.1s',
                background: 'linear-gradient(135deg, rgba(255, 255, 255, 0.98), rgba(255, 214, 181, 0.15))',
                backdropFilter: 'blur(20px)',
                border: '2px solid rgba(244, 162, 89, 0.2)',
                boxShadow: '0 20px 60px rgba(15, 23, 42, 0.1)',
                borderRadius: '20px',
                padding: '2rem'
              }}>
                <div className="section-title" style={{
                  fontSize: '1.5rem',
                  fontWeight: 700,
                  background: 'linear-gradient(135deg, #f4a259, #e56b6f)',
                  WebkitBackgroundClip: 'text',
                  WebkitTextFillColor: 'transparent',
                  marginBottom: '1.5rem',
                  display: 'flex',
                  alignItems: 'center',
                  gap: '0.5rem'
                }}>
                  <span>📚</span> Citations
                </div>
              {response.citations.length === 0 ? (
                <p className="muted">No citations returned.</p>
              ) : (
                  <div className="citations-grid" style={{
                    display: 'grid',
                    gridTemplateColumns: 'repeat(auto-fill, minmax(320px, 1fr))',
                    gap: '1.25rem'
                  }}>
                    {response.citations.map((citation, idx) => (
                      <div key={citation.id} className="citation-card" style={{
                        padding: '1.5rem',
                        background: 'rgba(255, 255, 255, 0.9)',
                        border: '1px solid rgba(244, 162, 89, 0.2)',
                        borderRadius: '16px',
                        transition: 'all 0.3s ease',
                        cursor: 'pointer',
                        animation: 'fadeIn 0.5s ease-out',
                        animationDelay: `${idx * 0.05}s`,
                        animationFillMode: 'backwards'
                      }}
                      onMouseEnter={(e) => {
                        e.currentTarget.style.transform = 'translateY(-4px)'
                        e.currentTarget.style.boxShadow = '0 12px 32px rgba(244, 162, 89, 0.2)'
                        e.currentTarget.style.borderColor = 'rgba(244, 162, 89, 0.4)'
                      }}
                      onMouseLeave={(e) => {
                        e.currentTarget.style.transform = 'translateY(0)'
                        e.currentTarget.style.boxShadow = 'none'
                        e.currentTarget.style.borderColor = 'rgba(244, 162, 89, 0.2)'
                      }}>
                        <div className="citation-subject" style={{
                          fontSize: '1rem',
                          fontWeight: 600,
                          color: 'var(--ink)',
                          marginBottom: '0.75rem',
                          lineHeight: '1.4'
                        }}>{citation.subject}</div>
                        <div className="citation-meta" style={{
                          fontSize: '0.875rem',
                          color: 'var(--muted)',
                          marginBottom: '1rem',
                          display: 'flex',
                          flexDirection: 'column',
                          gap: '0.25rem'
                        }}>
                          {citation.sender_name && <span>✉️ {citation.sender_name}</span>}
                        {citation.received_time && (
                            <span>📅 {new Date(citation.received_time).toLocaleString()}</span>
                          )}
                        </div>
                        <div className="citation-snippet" style={{
                          fontSize: '0.875rem',
                          color: 'var(--ink)',
                          opacity: 0.8,
                          lineHeight: '1.6',
                          fontStyle: 'italic'
                        }}>{citation.snippet}</div>
                      </div>
                    ))}
                  </div>
                )}
              </article>
            </section>

            {/* Inline Graph Visualization */}
            {response.context_packet && (
              <section className="card fade-in" style={{
                animationDelay: '0.2s',
                background: 'linear-gradient(135deg, rgba(255, 255, 255, 0.98), rgba(247, 239, 230, 0.95))',
                backdropFilter: 'blur(20px)',
                border: '2px solid rgba(31, 122, 140, 0.2)',
                boxShadow: '0 20px 60px rgba(15, 23, 42, 0.1)',
                borderRadius: '20px',
                padding: '2rem',
                marginTop: '2rem'
              }}>
                <div style={{ marginBottom: '1.5rem', display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', flexWrap: 'wrap', gap: '1rem' }}>
                  <div>
                    <div style={{
                      fontSize: '1.75rem',
                      fontWeight: 700,
                      background: 'linear-gradient(135deg, #1f7a8c, #3fa37b)',
                      WebkitBackgroundClip: 'text',
                      WebkitTextFillColor: 'transparent',
                      marginBottom: '0.5rem',
                      display: 'flex',
                      alignItems: 'center',
                      gap: '0.5rem'
                    }}>
                      {graphMode === 'question' ? '🔵 Question Context Graph' : '🌐 Full Context Graph'}
                    </div>
                    <div style={{ fontSize: '0.875rem', color: 'var(--muted)' }}>
                      {currentGraphData && (
                        <>
                          {graphMode === 'question' 
                            ? `${response.context_packet.nodes.length} nodes • ${response.context_packet.edges.length} edges • ${response.context_packet.trace.duration_ms.toFixed(1)}ms`
                            : `${fullGraphData?.count.nodes || 0} nodes • ${fullGraphData?.count.edges || 0} edges`
                          }
                        </>
                      )}
                    </div>
                  </div>
                  <div style={{ display: 'flex', gap: '0.75rem', flexWrap: 'wrap' }}>
                    <button
                      onClick={() => setLayoutType(layoutType === 'hierarchical' ? 'force' : 'hierarchical')}
                      style={{
                        padding: '0.75rem 1.5rem',
                        borderRadius: '10px',
                        background: 'linear-gradient(135deg, rgba(31, 122, 140, 0.1), rgba(63, 163, 123, 0.1))',
                        border: '2px solid rgba(31, 122, 140, 0.3)',
                        color: 'var(--accent)',
                        cursor: 'pointer',
                        fontSize: '0.875rem',
                        fontWeight: 600,
                        transition: 'all 0.3s ease'
                      }}
                    >
                      {layoutType === 'hierarchical' ? '🌳 Hierarchical' : '🌀 Force-Directed'}
                    </button>
                    {graphMode === 'question' ? (
                      <button
                        onClick={loadFullGraph}
                        disabled={loadingFullGraph}
                        style={{
                          padding: '0.75rem 1.5rem',
                          borderRadius: '10px',
                          background: loadingFullGraph ? 'rgba(31, 122, 140, 0.3)' : 'linear-gradient(135deg, #1f7a8c, #3fa37b)',
                          border: 'none',
                          color: 'white',
                          cursor: loadingFullGraph ? 'not-allowed' : 'pointer',
                          fontSize: '0.875rem',
                          fontWeight: 600,
                          transition: 'all 0.3s ease',
                          boxShadow: '0 4px 12px rgba(31, 122, 140, 0.3)'
                        }}
                      >
                        {loadingFullGraph ? '⏳ Loading…' : '🌐 View Full Graph'}
                      </button>
                    ) : (
                      <button
                        onClick={() => setGraphMode('question')}
                        style={{
                          padding: '0.75rem 1.5rem',
                          borderRadius: '10px',
                          background: 'linear-gradient(135deg, #1f7a8c, #3fa37b)',
                          border: 'none',
                          color: 'white',
                          cursor: 'pointer',
                          fontSize: '0.875rem',
                          fontWeight: 600,
                          transition: 'all 0.3s ease',
                          boxShadow: '0 4px 12px rgba(31, 122, 140, 0.3)'
                        }}
                      >
                        🔵 Back to Question Graph
                      </button>
                    )}
                  </div>
                </div>

                {/* Graph Container */}
                <div style={{
                  background: 'linear-gradient(135deg, rgba(31, 122, 140, 0.05), rgba(63, 163, 123, 0.05))',
                  borderRadius: '16px',
                  border: '2px solid rgba(31, 122, 140, 0.15)',
                  overflow: 'hidden',
                  minHeight: '600px'
                }}>
                  {currentGraphData && (
                    layoutType === 'hierarchical' ? (
                      <HierarchicalGraph
                        nodes={currentGraphData.nodes}
                        edges={currentGraphData.edges}
                        scores={currentGraphData.scores || {}}
                        width={1200}
                        height={600}
                        onNodeClick={setSelectedNode}
                        highlightPaths={graphMode === 'question'}
                        questionNodeId={graphMode === 'question' ? response.context_packet?.intent_node_id : undefined}
                      />
                    ) : (
                      <div style={{ height: '600px' }}>
                        <ForceGraph2D
                          graphData={getForceGraphData(currentGraphData as any)}
                          width={1200}
                          height={600}
                          nodeLabel={(node: any) => `${node.type}: ${node.name}\n💯 Score: ${node.score.toFixed(2)}`}
                          nodeColor={getNodeColor}
                          nodeVal={(node: any) => 5 + (node.score || 0) * 1.2}
                          linkDirectionalArrowLength={6}
                          linkDirectionalArrowRelPos={1}
                          linkWidth={2}
                          linkColor={() => 'rgba(31, 122, 140, 0.6)'}
                          backgroundColor="transparent"
                          onNodeClick={setSelectedNode}
                        />
                      </div>
                    )
                  )}
                </div>

                {/* Graph Legend */}
                <div style={{ marginTop: '1.5rem', display: 'flex', gap: '1rem', flexWrap: 'wrap', justifyContent: 'center' }}>
                  {['Intent', 'Conversation', 'Document', 'User', 'Attachment'].map((type, idx) => (
                    <div key={type} style={{
                      display: 'flex',
                      alignItems: 'center',
                      gap: '0.75rem',
                      padding: '0.75rem 1.25rem',
                      background: 'rgba(255, 255, 255, 0.9)',
                      borderRadius: '12px',
                      border: '2px solid rgba(31, 122, 140, 0.15)',
                      boxShadow: '0 4px 12px rgba(0,0,0,0.05)',
                      transition: 'all 0.3s ease',
                      animation: 'fadeIn 0.5s ease-out',
                      animationDelay: `${idx * 0.1}s`,
                      animationFillMode: 'backwards'
                    }}>
                      <div style={{
                        width: '16px',
                        height: '16px',
                        borderRadius: '50%',
                        background: getNodeColor({ type }),
                        boxShadow: `0 0 12px ${getNodeColor({ type })}`,
                        border: '2px solid rgba(255, 255, 255, 0.9)'
                      }} />
                      <span style={{ fontSize: '0.875rem', fontWeight: 600, color: 'var(--ink)' }}>{type}</span>
                    </div>
                  ))}
                </div>

                {/* Statistics */}
                {graphMode === 'question' && response.context_packet && (
                  <div style={{ marginTop: '2rem' }}>
                    <div style={{
                      fontSize: '1.25rem',
                      fontWeight: 700,
                      color: 'var(--ink)',
                      marginBottom: '1rem',
                      display: 'flex',
                      alignItems: 'center',
                      gap: '0.5rem'
                    }}>
                      <span>📊</span> Compilation Statistics
                    </div>
                    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: '1.25rem' }}>
                      <div style={{
                        padding: '1.5rem',
                        background: 'linear-gradient(135deg, rgba(31, 122, 140, 0.1), rgba(31, 122, 140, 0.05))',
                        borderRadius: '16px',
                        border: '2px solid rgba(31, 122, 140, 0.2)',
                        boxShadow: '0 4px 12px rgba(0,0,0,0.05)',
                        transition: 'all 0.3s ease'
                      }}>
                        <div style={{ fontSize: '0.875rem', color: 'var(--muted)', marginBottom: '0.5rem', fontWeight: 600 }}>Candidates</div>
                        <div style={{ fontSize: '2rem', fontWeight: 700, color: 'var(--accent)' }}>{response.context_packet.trace.candidate_count}</div>
                      </div>
                      <div style={{
                        padding: '1.5rem',
                        background: 'linear-gradient(135deg, rgba(229, 107, 111, 0.1), rgba(229, 107, 111, 0.05))',
                        borderRadius: '16px',
                        border: '2px solid rgba(229, 107, 111, 0.2)',
                        boxShadow: '0 4px 12px rgba(0,0,0,0.05)',
                        transition: 'all 0.3s ease'
                      }}>
                        <div style={{ fontSize: '0.875rem', color: 'var(--muted)', marginBottom: '0.5rem', fontWeight: 600 }}>Pruned</div>
                        <div style={{ fontSize: '2rem', fontWeight: 700, color: 'var(--accent-3)' }}>{response.context_packet.trace.pruned_count}</div>
                      </div>
                      <div style={{
                        padding: '1.5rem',
                        background: 'linear-gradient(135deg, rgba(63, 163, 123, 0.1), rgba(63, 163, 123, 0.05))',
                        borderRadius: '16px',
                        border: '2px solid rgba(63, 163, 123, 0.2)',
                        boxShadow: '0 4px 12px rgba(0,0,0,0.05)',
                        transition: 'all 0.3s ease'
                      }}>
                        <div style={{ fontSize: '0.875rem', color: 'var(--muted)', marginBottom: '0.5rem', fontWeight: 600 }}>Final</div>
                        <div style={{ fontSize: '2rem', fontWeight: 700, color: 'var(--mint)' }}>{response.context_packet.trace.final_count}</div>
                      </div>
                      <div style={{
                        padding: '1.5rem',
                        background: 'linear-gradient(135deg, rgba(244, 162, 89, 0.1), rgba(244, 162, 89, 0.05))',
                        borderRadius: '16px',
                        border: '2px solid rgba(244, 162, 89, 0.2)',
                        boxShadow: '0 4px 12px rgba(0,0,0,0.05)',
                        transition: 'all 0.3s ease'
                      }}>
                        <div style={{ fontSize: '0.875rem', color: 'var(--muted)', marginBottom: '0.5rem', fontWeight: 600 }}>Duration</div>
                        <div style={{ fontSize: '2rem', fontWeight: 700, color: 'var(--accent-2)' }}>{response.context_packet.trace.duration_ms.toFixed(1)}ms</div>
                      </div>
                    </div>
                  </div>
                )}

                {/* Top Scoring Nodes */}
                {graphMode === 'question' && sortedScores.length > 0 && (
                  <div style={{ marginTop: '2rem' }}>
                    <div style={{
                      fontSize: '1.25rem',
                      fontWeight: 700,
                      color: 'var(--ink)',
                      marginBottom: '1rem',
                      display: 'flex',
                      alignItems: 'center',
                      gap: '0.5rem'
                    }}>
                      <span>🏆</span> Top Scoring Nodes
                    </div>
                    <div style={{ maxHeight: '300px', overflow: 'auto' }}>
                      {sortedScores.slice(0, 5).map((score: any, idx: number) => {
                        const node = response.context_packet!.nodes.find((n: any) => n.id === score.node_id)
                        const isTopThree = idx < 3
                        return (
                          <div key={score.node_id} style={{
                            display: 'flex',
                            justifyContent: 'space-between',
                            alignItems: 'center',
                            padding: '1.25rem',
                            marginBottom: '0.75rem',
                            background: isTopThree
                              ? 'linear-gradient(135deg, rgba(31, 122, 140, 0.15), rgba(63, 163, 123, 0.1))'
                              : 'rgba(255, 255, 255, 0.7)',
                            borderRadius: '12px',
                            border: isTopThree
                              ? '2px solid rgba(31, 122, 140, 0.3)'
                              : '1px solid rgba(31, 122, 140, 0.15)',
                            boxShadow: isTopThree ? '0 4px 12px rgba(31, 122, 140, 0.15)' : 'none',
                            transition: 'all 0.3s ease',
                            cursor: 'default'
                          }}>
                            <div style={{ flex: 1, display: 'flex', alignItems: 'center', gap: '1rem' }}>
                              <div style={{
                                minWidth: '32px',
                                height: '32px',
                                borderRadius: '50%',
                                background: isTopThree
                                  ? 'linear-gradient(135deg, #1f7a8c, #3fa37b)'
                                  : 'rgba(31, 122, 140, 0.2)',
                                display: 'flex',
                                alignItems: 'center',
                                justifyContent: 'center',
                                fontWeight: 700,
                                fontSize: '0.875rem',
                                color: isTopThree ? 'white' : 'var(--accent)',
                                boxShadow: isTopThree ? '0 4px 8px rgba(31, 122, 140, 0.3)' : 'none'
                              }}>
                                {idx + 1}
                              </div>
                              <div style={{ flex: 1 }}>
                                <div style={{ fontSize: '0.95rem', fontWeight: 600, color: 'var(--ink)', marginBottom: '0.25rem', lineHeight: '1.4' }}>
                                  {node?.props.subject || node?.props.email || node?.id}
                                </div>
                                <div style={{
                                  fontSize: '0.8rem',
                                  color: 'var(--muted)',
                                  display: 'inline-block',
                                  padding: '0.25rem 0.5rem',
                                  background: 'rgba(31, 122, 140, 0.1)',
                                  borderRadius: '6px'
                                }}>{node?.type}</div>
                              </div>
                            </div>
                            <div style={{
                              fontSize: '1.25rem',
                              fontWeight: 700,
                              color: 'var(--accent)',
                              padding: '0.5rem 1rem',
                              background: 'rgba(31, 122, 140, 0.1)',
                              borderRadius: '10px',
                              minWidth: '80px',
                              textAlign: 'center'
                            }}>
                              {score.total_score !== undefined ? score.total_score.toFixed(2) : 'N/A'}
                            </div>
                          </div>
                        )
                      })}
                    </div>
                </div>
              )}
          </section>
            )}
          </>
        )}

        {/* Node Details Modal */}
        {selectedNode && (
          <div
            style={{
              position: 'fixed',
              top: 0,
              left: 0,
              right: 0,
              bottom: 0,
              background: 'rgba(0,0,0,0.7)',
              backdropFilter: 'blur(8px)',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              zIndex: 1000,
              padding: '2rem',
              animation: 'fadeIn 0.2s ease-out'
            }}
            onClick={() => setSelectedNode(null)}
          >
            <div
              style={{
                background: 'linear-gradient(135deg, rgba(255, 255, 255, 0.98), rgba(247, 239, 230, 0.95))',
                borderRadius: '20px',
                maxWidth: '600px',
                width: '100%',
                maxHeight: '80vh',
                overflow: 'auto',
                border: '2px solid rgba(31, 122, 140, 0.3)',
                boxShadow: '0 20px 60px rgba(0,0,0,0.3)',
                animation: 'slideUp 0.3s ease-out'
              }}
              onClick={(e) => e.stopPropagation()}
            >
              <div style={{
                padding: '1.5rem',
                borderBottom: '2px solid rgba(31, 122, 140, 0.15)',
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center'
              }}>
                <div>
                  <h2 style={{ margin: 0, fontSize: '1.5rem', fontWeight: 700, color: 'var(--ink)' }}>
                    🔵 Node Details
                  </h2>
                  <div style={{ fontSize: '0.875rem', color: 'var(--muted)' }}>
                    {selectedNode.type}
                  </div>
                </div>
                <button
                  onClick={() => setSelectedNode(null)}
                  style={{
                    background: 'rgba(229, 107, 111, 0.15)',
                    border: '2px solid rgba(229, 107, 111, 0.3)',
                    borderRadius: '10px',
                    width: '40px',
                    height: '40px',
                    cursor: 'pointer',
                    fontSize: '1.25rem',
                    color: 'var(--accent-3)',
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                    transition: 'all 0.2s ease',
                    fontWeight: 700
                  }}
                >
                  ✕
                </button>
              </div>
              <div style={{ padding: '1.5rem' }}>
                <div style={{ marginBottom: '1rem' }}>
                  <div style={{ fontSize: '0.875rem', fontWeight: 600, color: 'var(--muted)', marginBottom: '0.5rem' }}>
                    Node ID
                  </div>
                  <div style={{ fontSize: '0.875rem', fontFamily: 'monospace', color: 'var(--ink)', wordBreak: 'break-all' }}>
                    {selectedNode.id}
                  </div>
                </div>
                {selectedNode.score && (
                  <div style={{
                    padding: '1.25rem',
                    background: 'linear-gradient(135deg, rgba(31, 122, 140, 0.1), rgba(63, 163, 123, 0.05))',
                    borderRadius: '12px',
                    border: '2px solid rgba(31, 122, 140, 0.2)',
                    marginBottom: '1rem'
                  }}>
                    <div style={{ fontSize: '0.875rem', fontWeight: 600, marginBottom: '0.5rem', color: 'var(--ink)' }}>
                      📊 Score: {typeof selectedNode.score === 'number' 
                        ? selectedNode.score.toFixed(2) 
                        : (selectedNode.score.total_score !== undefined ? selectedNode.score.total_score.toFixed(2) : 'N/A')}
                    </div>
                  </div>
                )}
                <div>
                  <div style={{ fontSize: '0.875rem', fontWeight: 600, marginBottom: '0.75rem', color: 'var(--ink)' }}>
                    📋 Properties
                  </div>
                  <div style={{
                    background: 'rgba(255, 255, 255, 0.7)',
                    borderRadius: '12px',
                    padding: '1rem',
                    border: '1px solid rgba(31, 122, 140, 0.15)',
                    maxHeight: '300px',
                    overflow: 'auto'
                  }}>
                    {Object.entries(selectedNode.props || {}).map(([key, value]) => (
                      <div key={key} style={{ marginBottom: '0.75rem', paddingBottom: '0.75rem', borderBottom: '1px solid rgba(31, 122, 140, 0.1)' }}>
                        <div style={{ fontSize: '0.75rem', color: 'var(--muted)', marginBottom: '0.25rem', textTransform: 'uppercase', fontWeight: 600 }}>
                          {key.replace(/_/g, ' ')}
                        </div>
                        <div style={{ fontSize: '0.875rem', color: 'var(--ink)', wordBreak: 'break-word' }}>
                          {typeof value === 'object' ? JSON.stringify(value, null, 2) : String(value)}
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              </div>
            </div>
          </div>
        )}
      </div>
    </main>
  )
}
