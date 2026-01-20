'use client'

import Link from 'next/link'
import { useState, useEffect, useRef } from 'react'
import dynamic from 'next/dynamic'

const ForceGraph2D = dynamic(() => import('react-force-graph-2d'), { ssr: false })

const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'

interface GraphNode {
  id: string
  type: string
  props: Record<string, any>
  layer: string
  tenant_id: string
}

interface GraphEdge {
  id: string
  src: string
  dst: string
  type: string
  props: Record<string, any>
}

interface NodeScore {
  node_id: string
  recency_score: number
  authority_score: number
  stage_score: number
  rule_score: number
  total_score: number
}

interface ContextPacket {
  request_id: string
  intent_node_id: string
  nodes: GraphNode[]
  edges: GraphEdge[]
  scores: Record<string, NodeScore>
  lineage: Record<string, string[]>
  trace: {
    request_id: string
    steps: Array<{ name: string; timestamp: string; details: any }>
    candidate_count: number
    pruned_count: number
    final_count: number
    duration_ms: number
  }
}

interface GraphStats {
  nodes_by_type: Record<string, number>
  edges_by_type: Record<string, number>
  total_nodes: number
  total_edges: number
}

interface ModalData {
  type: 'node' | 'edge'
  data: GraphNode | GraphEdge
  score?: NodeScore
}

export default function GraphPage() {
  const [question, setQuestion] = useState('')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [packet, setPacket] = useState<ContextPacket | null>(null)
  const [stats, setStats] = useState<GraphStats | null>(null)
  const [modalData, setModalData] = useState<ModalData | null>(null)
  const [showScores, setShowScores] = useState(false)
  const [showTrace, setShowTrace] = useState(false)
  const [filterType, setFilterType] = useState<string>('all')
  const [hoveredNode, setHoveredNode] = useState<any>(null)
  const [rebuildLoading, setRebuildLoading] = useState(false)
  const [rebuildMessage, setRebuildMessage] = useState<{type: 'success' | 'error', text: string} | null>(null)
  const graphRef = useRef<any>()
  const containerRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    fetchStats()
  }, [])

  const fetchStats = async () => {
    try {
      const res = await fetch(`${API_BASE_URL}/graph/metrics`)
      const data = await res.json()
      if (data.success) {
        setStats(data.stats)
      }
    } catch (err) {
      console.error('Error fetching stats:', err)
    }
  }

  const compileContext = async () => {
    if (!question.trim()) {
      setError('Please enter a question')
      return
    }

    setLoading(true)
    setError(null)
    setPacket(null)

    try {
      const res = await fetch(`${API_BASE_URL}/graph/compile`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ question, top_k: 8, debug: true })
      })

      if (!res.ok) {
        throw new Error(`API error: ${res.status}`)
      }

      const data = await res.json()
      if (data.success) {
        setPacket(data.packet)
      } else {
        setError(data.message || 'Failed to compile context')
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Something went wrong')
    } finally {
      setLoading(false)
    }
  }

  const getGraphData = () => {
    if (!packet) return { nodes: [], links: [] }

    const nodes = packet.nodes
      .filter(n => filterType === 'all' || n.type === filterType)
      .map(n => ({
        id: n.id,
        name: n.props.subject || n.props.display_name || n.props.email || n.props.filename || n.type,
        type: n.type,
        layer: n.layer,
        score: packet.scores[n.id]?.total_score || 0,
        props: n.props,
        tenant_id: n.tenant_id
      }))

    const nodeIds = new Set(nodes.map(n => n.id))
    const links = packet.edges
      .filter(e => nodeIds.has(e.src) && nodeIds.has(e.dst))
      .map(e => ({
        source: e.src,
        target: e.dst,
        type: e.type,
        id: e.id,
        props: e.props
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

  const getNodeSize = (node: any) => {
    const baseSize = 5
    const scoreMultiplier = 1.2
    return baseSize + (node.score || 0) * scoreMultiplier
  }

  const handleNodeClick = (node: any) => {
    const graphNode = packet?.nodes.find(n => n.id === node.id)
    if (graphNode) {
      setModalData({
        type: 'node',
        data: graphNode,
        score: packet?.scores[node.id]
      })
    }
  }

  const handleLinkClick = (link: any) => {
    const graphEdge = packet?.edges.find(e => e.id === link.id)
    if (graphEdge) {
      setModalData({
        type: 'edge',
        data: graphEdge
      })
    }
  }

  const closeModal = () => {
    setModalData(null)
  }

  const rebuildGraph = async () => {
    setRebuildLoading(true)
    setRebuildMessage(null)

    try {
      const res = await fetch(`${API_BASE_URL}/graph/rebuild`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ tenant_id: 'default' })
      })

      if (!res.ok) {
        throw new Error(`API error: ${res.status}`)
      }

      const data = await res.json()
      if (data.success) {
        setRebuildMessage({
          type: 'success',
          text: `Graph rebuilt successfully! Users: ${data.stats.users}, Conversations: ${data.stats.conversations}, Documents: ${data.stats.documents}, Attachments: ${data.stats.attachments}, Edges: ${data.stats.edges}`
        })
        // Refresh stats
        fetchStats()
      } else {
        setRebuildMessage({
          type: 'error',
          text: 'Failed to rebuild graph'
        })
      }
    } catch (err) {
      setRebuildMessage({
        type: 'error',
        text: err instanceof Error ? err.message : 'Something went wrong'
      })
    } finally {
      setRebuildLoading(false)
    }
  }

  const sortedScores = packet
    ? Object.values(packet.scores).sort((a, b) => b.total_score - a.total_score)
    : []

  return (
    <main className="dashboard">
      <div className="dashboard-bg">
        <div className="orb orb-a" />
        <div className="orb orb-b" />
        <div className="orb orb-c" />
        <div className="grid-overlay" />
      </div>

      <div className="dashboard-content" style={{ maxWidth: '100%', padding: '1rem' }}>
        <nav className="nav">
          <div className="nav-title">Dashboard views</div>
          <div className="nav-links">
            <Link className="nav-link" href="/">
              Capillary relationship
            </Link>
            <Link className="nav-link" href="/csat-response">
              CSAT response
            </Link>
            <Link className="nav-link" href="/query">
              Ask emails
            </Link>
            <Link className="nav-link active" href="/graph">
              Context Graph
            </Link>
          </div>
        </nav>

        <section className="card">
          <div className="section-title">Context Graph Compiler</div>
          <p className="hero-subtitle">
            Visualize how the graph-native context engine compiles deterministic working contexts
          </p>

          {stats && (
            <div style={{ marginBottom: '1.5rem', padding: '1.25rem', background: 'linear-gradient(135deg, rgba(78, 205, 196, 0.1), rgba(69, 183, 209, 0.1))', borderRadius: '12px', border: '1px solid rgba(78, 205, 196, 0.2)' }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1rem', flexWrap: 'wrap', gap: '1rem' }}>
                <div style={{ fontSize: '0.875rem', fontWeight: 600, color: '#ffffff' }}>📊 Graph Statistics</div>
                <button
                  onClick={rebuildGraph}
                  disabled={rebuildLoading}
                  style={{
                    padding: '0.625rem 1.25rem',
                    borderRadius: '8px',
                    background: rebuildLoading ? 'rgba(78, 205, 196, 0.3)' : 'linear-gradient(135deg, #ff6b6b, #ee5a6f)',
                    border: 'none',
                    color: '#ffffff',
                    cursor: rebuildLoading ? 'not-allowed' : 'pointer',
                    fontSize: '0.875rem',
                    fontWeight: 600,
                    transition: 'all 0.2s ease',
                    display: 'flex',
                    alignItems: 'center',
                    gap: '0.5rem'
                  }}
                >
                  {rebuildLoading ? '⏳ Rebuilding...' : '🔄 Rebuild Graph'}
                </button>
              </div>
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(140px, 1fr))', gap: '1.25rem' }}>
                <div style={{ textAlign: 'center' }}>
                  <div style={{ fontSize: '0.75rem', textTransform: 'uppercase', letterSpacing: '0.05em', opacity: 0.6, marginBottom: '0.5rem' }}>Total Nodes</div>
                  <div style={{ fontSize: '2rem', fontWeight: 700, background: 'linear-gradient(135deg, #4ecdc4, #45b7d1)', WebkitBackgroundClip: 'text', WebkitTextFillColor: 'transparent' }}>{stats.total_nodes}</div>
                </div>
                <div style={{ textAlign: 'center' }}>
                  <div style={{ fontSize: '0.75rem', textTransform: 'uppercase', letterSpacing: '0.05em', opacity: 0.6, marginBottom: '0.5rem' }}>Total Edges</div>
                  <div style={{ fontSize: '2rem', fontWeight: 700, background: 'linear-gradient(135deg, #4ecdc4, #45b7d1)', WebkitBackgroundClip: 'text', WebkitTextFillColor: 'transparent' }}>{stats.total_edges}</div>
                </div>
                <div style={{ textAlign: 'center' }}>
                  <div style={{ fontSize: '0.75rem', textTransform: 'uppercase', letterSpacing: '0.05em', opacity: 0.6, marginBottom: '0.5rem' }}>Conversations</div>
                  <div style={{ fontSize: '2rem', fontWeight: 700, background: 'linear-gradient(135deg, #4ecdc4, #45b7d1)', WebkitBackgroundClip: 'text', WebkitTextFillColor: 'transparent' }}>{stats.nodes_by_type?.Conversation || 0}</div>
                </div>
                <div style={{ textAlign: 'center' }}>
                  <div style={{ fontSize: '0.75rem', textTransform: 'uppercase', letterSpacing: '0.05em', opacity: 0.6, marginBottom: '0.5rem' }}>Documents</div>
                  <div style={{ fontSize: '2rem', fontWeight: 700, background: 'linear-gradient(135deg, #4ecdc4, #45b7d1)', WebkitBackgroundClip: 'text', WebkitTextFillColor: 'transparent' }}>{stats.nodes_by_type?.Document || 0}</div>
                </div>
              </div>
            </div>
          )}

          {rebuildMessage && (
            <div style={{
              marginBottom: '1rem',
              padding: '1rem',
              borderRadius: '8px',
              background: rebuildMessage.type === 'success' ? 'rgba(78, 205, 196, 0.15)' : 'rgba(255, 107, 107, 0.15)',
              border: `1px solid ${rebuildMessage.type === 'success' ? 'rgba(78, 205, 196, 0.4)' : 'rgba(255, 107, 107, 0.4)'}`,
              color: '#ffffff',
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'center'
            }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
                <span style={{ fontSize: '1.25rem' }}>{rebuildMessage.type === 'success' ? '✅' : '❌'}</span>
                <span style={{ fontSize: '0.875rem' }}>{rebuildMessage.text}</span>
              </div>
              <button
                onClick={() => setRebuildMessage(null)}
                style={{
                  background: 'rgba(255,255,255,0.1)',
                  border: 'none',
                  borderRadius: '4px',
                  width: '24px',
                  height: '24px',
                  cursor: 'pointer',
                  color: 'white',
                  fontSize: '1rem'
                }}
              >
                ✕
              </button>
            </div>
          )}

          <div style={{ marginBottom: '1.5rem' }}>
            <textarea
              value={question}
              onChange={(e) => setQuestion(e.target.value)}
              placeholder="Enter a question to compile context graph..."
              rows={3}
              className="query-textarea"
              style={{ 
                width: '100%', 
                marginBottom: '0.75rem', 
                fontSize: '1rem', 
                padding: '1rem', 
                borderRadius: '8px', 
                background: '#4a4a5e', 
                border: '1px solid rgba(78, 205, 196, 0.4)', 
                color: '#ffffff', 
                resize: 'vertical',
                WebkitTextFillColor: '#ffffff',
                caretColor: '#ffffff'
              }}
            />
            <button
              onClick={compileContext}
              disabled={loading}
              className="query-button"
              style={{ width: '100%', padding: '1rem', fontSize: '1rem', fontWeight: 600, borderRadius: '8px', background: loading ? 'rgba(78, 205, 196, 0.3)' : 'linear-gradient(135deg, #4ecdc4, #45b7d1)', border: 'none', color: 'white', cursor: loading ? 'not-allowed' : 'pointer', transition: 'all 0.3s ease' }}
            >
              {loading ? '⏳ Compiling...' : '🚀 Compile Context Graph'}
            </button>
          </div>

          {error && <p className="query-error">{error}</p>}
        </section>

        {packet && (
          <>
            <section className="card fade-in" style={{ marginBottom: '1.5rem' }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1.5rem', flexWrap: 'wrap', gap: '1rem' }}>
                <div>
                  <div className="section-title" style={{ marginBottom: '0.25rem' }}>Context Graph Visualization</div>
                  <div style={{ fontSize: '0.875rem', opacity: 0.7 }}>{packet.nodes.length} nodes • {packet.edges.length} edges • {packet.trace.duration_ms.toFixed(1)}ms</div>
                </div>
                <div style={{ display: 'flex', gap: '0.75rem', flexWrap: 'wrap' }}>
                  <select
                    value={filterType}
                    onChange={(e) => setFilterType(e.target.value)}
                    style={{ 
                      padding: '0.625rem 1rem', 
                      borderRadius: '8px', 
                      background: 'rgba(30,30,45,0.95)', 
                      border: '1px solid rgba(78, 205, 196, 0.4)', 
                      color: '#ffffff', 
                      fontSize: '0.875rem', 
                      cursor: 'pointer', 
                      transition: 'all 0.2s ease',
                      WebkitAppearance: 'none',
                      MozAppearance: 'none',
                      appearance: 'none',
                      backgroundImage: 'url("data:image/svg+xml;charset=UTF-8,%3csvg xmlns=\'http://www.w3.org/2000/svg\' viewBox=\'0 0 24 24\' fill=\'none\' stroke=\'white\' stroke-width=\'2\' stroke-linecap=\'round\' stroke-linejoin=\'round\'%3e%3cpolyline points=\'6 9 12 15 18 9\'%3e%3c/polyline%3e%3c/svg%3e")',
                      backgroundRepeat: 'no-repeat',
                      backgroundPosition: 'right 0.5rem center',
                      backgroundSize: '1.25rem',
                      paddingRight: '2.5rem'
                    }}
                  >
                    <option value="all" style={{ background: '#1a1a2e', color: '#ffffff' }}>🔍 All Types</option>
                    <option value="Intent" style={{ background: '#1a1a2e', color: '#ffffff' }}>🎯 Intent</option>
                    <option value="Conversation" style={{ background: '#1a1a2e', color: '#ffffff' }}>💬 Conversation</option>
                    <option value="Document" style={{ background: '#1a1a2e', color: '#ffffff' }}>📄 Document</option>
                    <option value="User" style={{ background: '#1a1a2e', color: '#ffffff' }}>👤 User</option>
                    <option value="Attachment" style={{ background: '#1a1a2e', color: '#ffffff' }}>📎 Attachment</option>
                  </select>
                  <button
                    onClick={() => setShowScores(!showScores)}
                    style={{ padding: '0.625rem 1.25rem', borderRadius: '8px', background: showScores ? 'linear-gradient(135deg, #4ecdc4, #45b7d1)' : 'rgba(255,255,255,0.15)', border: '1px solid rgba(255,255,255,0.3)', color: '#10131a', cursor: 'pointer', fontSize: '0.875rem', fontWeight: 500, transition: 'all 0.2s ease' }}
                  >
                    {showScores ? '📊 Scores' : '📊 Scores'}
                  </button>
                  <button
                    onClick={() => setShowTrace(!showTrace)}
                    style={{ padding: '0.625rem 1.25rem', borderRadius: '8px', background: showTrace ? 'linear-gradient(135deg, #4ecdc4, #45b7d1)' : 'rgba(255,255,255,0.15)', border: '1px solid rgba(255,255,255,0.3)', color: '#10131a', cursor: 'pointer', fontSize: '0.875rem', fontWeight: 500, transition: 'all 0.2s ease' }}
                  >
                    {showTrace ? '🔬 Trace' : '🔬 Trace'}
                  </button>
                </div>
              </div>

              <div ref={containerRef} style={{ height: '70vh', minHeight: '500px', background: 'linear-gradient(135deg, rgba(0,0,0,0.4), rgba(0,0,0,0.6))', borderRadius: '12px', position: 'relative', border: '1px solid rgba(255,255,255,0.1)', overflow: 'hidden' }}>
                <ForceGraph2D
                  ref={graphRef}
                  graphData={getGraphData()}
                  width={containerRef.current?.clientWidth}
                  height={containerRef.current?.clientHeight || 500}
                  nodeLabel={(node: any) => `${node.type}: ${node.name}\n💯 Score: ${node.score.toFixed(2)}`}
                  nodeColor={getNodeColor}
                  nodeVal={(node: any) => getNodeSize(node)}
                  nodeCanvasObject={(node: any, ctx: any, globalScale: number) => {
                    const label = node.name
                    const fontSize = 12 / globalScale
                    ctx.font = `${fontSize}px Sans-Serif`
                    const textWidth = ctx.measureText(label).width
                    const bckgDimensions = [textWidth, fontSize].map(n => n + fontSize * 0.4)

                    const nodeSize = getNodeSize(node)
                    
                    ctx.fillStyle = getNodeColor(node)
                    ctx.beginPath()
                    ctx.arc(node.x, node.y, nodeSize, 0, 2 * Math.PI, false)
                    ctx.fill()

                    if (hoveredNode?.id === node.id) {
                      ctx.strokeStyle = '#ffffff'
                      ctx.lineWidth = 2 / globalScale
                      ctx.stroke()
                    }

                    if (globalScale > 1.5) {
                      ctx.textAlign = 'center'
                      ctx.textBaseline = 'middle'
                      ctx.fillStyle = 'rgba(255, 255, 255, 0.9)'
                      ctx.fillText(label, node.x, node.y + nodeSize + fontSize)
                    }
                  }}
                  linkDirectionalArrowLength={6}
                  linkDirectionalArrowRelPos={1}
                  linkWidth={2}
                  linkColor={(link: any) => {
                    const edgeColors: Record<string, string> = {
                      PART_OF: 'rgba(78, 205, 196, 0.6)',
                      HAS_ATTACHMENT: 'rgba(255, 234, 167, 0.6)',
                      SENT_BY: 'rgba(150, 206, 180, 0.6)',
                      SENT_TO: 'rgba(150, 206, 180, 0.5)',
                      FOLLOWS: 'rgba(69, 183, 209, 0.6)',
                      SEEKS_ANSWER_TO: 'rgba(255, 107, 107, 0.6)'
                    }
                    return edgeColors[link.type] || 'rgba(255,255,255,0.3)'
                  }}
                  linkDirectionalParticles={2}
                  linkDirectionalParticleWidth={2}
                  onNodeClick={handleNodeClick}
                  onLinkClick={handleLinkClick}
                  onNodeHover={(node: any) => setHoveredNode(node)}
                  backgroundColor="transparent"
                  cooldownTicks={50}
                  d3VelocityDecay={0.2}
                  d3AlphaDecay={0.02}
                  warmupTicks={50}
                  enableNodeDrag={true}
                  enableZoomInteraction={true}
                  enablePanInteraction={true}
                />
                <div style={{ position: 'absolute', top: '1rem', left: '1rem', background: 'rgba(0,0,0,0.85)', padding: '0.75rem', borderRadius: '8px', fontSize: '0.75rem', backdropFilter: 'blur(10px)', border: '1px solid rgba(255,255,255,0.2)' }}>
                  <div style={{ fontWeight: 600, marginBottom: '0.5rem', color: '#ffffff' }}>💡 Tip: Click nodes/edges for details</div>
                  <div style={{ color: '#e0e0e0' }}>Scroll to zoom • Drag to pan</div>
                </div>
              </div>

              <div style={{ marginTop: '1.5rem', display: 'flex', gap: '1.5rem', flexWrap: 'wrap', justifyContent: 'center' }}>
                {['Intent', 'Conversation', 'Document', 'User', 'Attachment'].map(type => (
                  <div key={type} style={{ display: 'flex', alignItems: 'center', gap: '0.625rem', padding: '0.5rem 1rem', background: 'rgba(255,255,255,0.05)', borderRadius: '8px', border: '1px solid rgba(255,255,255,0.1)' }}>
                    <div style={{ width: '14px', height: '14px', borderRadius: '50%', background: getNodeColor({ type }), boxShadow: `0 0 10px ${getNodeColor({ type })}` }} />
                    <span style={{ fontSize: '0.875rem', fontWeight: 500 }}>{type}</span>
                  </div>
                ))}
              </div>
            </section>

            {showScores && (
              <section className="card fade-in" style={{ animationDelay: '0.1s' }}>
                <div className="section-title">Scoring Breakdown</div>
                <div style={{ overflowX: 'auto' }}>
                  <table style={{ width: '100%', borderCollapse: 'collapse' }}>
                    <thead>
                      <tr style={{ borderBottom: '1px solid rgba(255,255,255,0.1)' }}>
                        <th style={{ padding: '0.75rem', textAlign: 'left' }}>Node</th>
                        <th style={{ padding: '0.75rem', textAlign: 'right' }}>Recency</th>
                        <th style={{ padding: '0.75rem', textAlign: 'right' }}>Authority</th>
                        <th style={{ padding: '0.75rem', textAlign: 'right' }}>Stage</th>
                        <th style={{ padding: '0.75rem', textAlign: 'right' }}>Rule</th>
                        <th style={{ padding: '0.75rem', textAlign: 'right' }}>Total</th>
                      </tr>
                    </thead>
                    <tbody>
                      {sortedScores.slice(0, 20).map((score) => {
                        const node = packet.nodes.find(n => n.id === score.node_id)
                        return (
                          <tr key={score.node_id} style={{ borderBottom: '1px solid rgba(255,255,255,0.05)' }}>
                            <td style={{ padding: '0.75rem' }}>
                              <div style={{ fontSize: '0.875rem', fontWeight: 500 }}>
                                {node?.props.subject || node?.props.email || node?.id}
                              </div>
                              <div style={{ fontSize: '0.75rem', opacity: 0.6 }}>{node?.type}</div>
                            </td>
                            <td style={{ padding: '0.75rem', textAlign: 'right' }}>{score.recency_score.toFixed(2)}</td>
                            <td style={{ padding: '0.75rem', textAlign: 'right' }}>{score.authority_score.toFixed(2)}</td>
                            <td style={{ padding: '0.75rem', textAlign: 'right' }}>{score.stage_score.toFixed(2)}</td>
                            <td style={{ padding: '0.75rem', textAlign: 'right' }}>{score.rule_score.toFixed(2)}</td>
                            <td style={{ padding: '0.75rem', textAlign: 'right', fontWeight: 600 }}>{score.total_score.toFixed(2)}</td>
                          </tr>
                        )
                      })}
                    </tbody>
                  </table>
                </div>
              </section>
            )}

            {showTrace && packet.trace && (
              <section className="card fade-in" style={{ animationDelay: '0.2s' }}>
                <div className="section-title">Compilation Trace</div>
                <div style={{ marginBottom: '1rem', display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(150px, 1fr))', gap: '1rem' }}>
                  <div>
                    <div style={{ fontSize: '0.875rem', opacity: 0.7 }}>Candidates</div>
                    <div style={{ fontSize: '1.25rem', fontWeight: 600 }}>{packet.trace.candidate_count}</div>
                  </div>
                  <div>
                    <div style={{ fontSize: '0.875rem', opacity: 0.7 }}>Pruned</div>
                    <div style={{ fontSize: '1.25rem', fontWeight: 600 }}>{packet.trace.pruned_count}</div>
                  </div>
                  <div>
                    <div style={{ fontSize: '0.875rem', opacity: 0.7 }}>Final</div>
                    <div style={{ fontSize: '1.25rem', fontWeight: 600 }}>{packet.trace.final_count}</div>
                  </div>
                  <div>
                    <div style={{ fontSize: '0.875rem', opacity: 0.7 }}>Duration</div>
                    <div style={{ fontSize: '1.25rem', fontWeight: 600 }}>{packet.trace.duration_ms.toFixed(2)}ms</div>
                  </div>
                </div>

                <div style={{ marginTop: '1rem' }}>
                  <div style={{ fontSize: '0.875rem', fontWeight: 600, marginBottom: '0.5rem' }}>Compilation Steps</div>
                  {packet.trace.steps.map((step, idx) => (
                    <div key={idx} style={{ padding: '0.75rem', background: 'rgba(255,255,255,0.05)', borderRadius: '4px', marginBottom: '0.5rem' }}>
                      <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '0.25rem' }}>
                        <span style={{ fontWeight: 500 }}>{step.name}</span>
                        <span style={{ fontSize: '0.75rem', opacity: 0.6 }}>{new Date(step.timestamp).toLocaleTimeString()}</span>
                      </div>
                      <pre style={{ fontSize: '0.75rem', opacity: 0.8, margin: 0, whiteSpace: 'pre-wrap' }}>
                        {JSON.stringify(step.details, null, 2)}
                      </pre>
                    </div>
                  ))}
                </div>
              </section>
            )}

          </>
        )}

        {modalData && (
          <div 
            style={{ 
              position: 'fixed', 
              top: 0, 
              left: 0, 
              right: 0, 
              bottom: 0, 
              background: 'rgba(0,0,0,0.8)', 
              backdropFilter: 'blur(8px)',
              display: 'flex', 
              alignItems: 'center', 
              justifyContent: 'center', 
              zIndex: 1000,
              padding: '1rem',
              animation: 'fadeIn 0.2s ease-out'
            }}
            onClick={closeModal}
          >
            <div 
              style={{ 
                background: 'linear-gradient(135deg, rgba(20,20,30,0.98), rgba(30,30,45,0.98))',
                borderRadius: '16px',
                maxWidth: '700px',
                width: '100%',
                maxHeight: '85vh',
                overflow: 'auto',
                border: '1px solid rgba(78, 205, 196, 0.3)',
                boxShadow: '0 20px 60px rgba(0,0,0,0.5), 0 0 40px rgba(78, 205, 196, 0.1)',
                animation: 'slideUp 0.3s ease-out'
              }}
              onClick={(e) => e.stopPropagation()}
            >
              <div style={{ 
                padding: '1.5rem', 
                borderBottom: '1px solid rgba(255,255,255,0.1)',
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center',
                background: 'linear-gradient(135deg, rgba(78, 205, 196, 0.15), rgba(69, 183, 209, 0.15))'
              }}>
                <div>
                  <h2 style={{ margin: 0, fontSize: '1.5rem', fontWeight: 700, marginBottom: '0.25rem' }}>
                    {modalData.type === 'node' ? '🔵 Node Details' : '🔗 Edge Details'}
                  </h2>
                  <div style={{ fontSize: '0.875rem', opacity: 0.7 }}>
                    {modalData.type === 'node' 
                      ? `${(modalData.data as GraphNode).type} • ${(modalData.data as GraphNode).layer}`
                      : `${(modalData.data as GraphEdge).type}`
                    }
                  </div>
                </div>
                <button
                  onClick={closeModal}
                  style={{
                    background: 'rgba(255,255,255,0.1)',
                    border: 'none',
                    borderRadius: '8px',
                    width: '40px',
                    height: '40px',
                    cursor: 'pointer',
                    fontSize: '1.25rem',
                    color: 'white',
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                    transition: 'all 0.2s ease'
                  }}
                  onMouseEnter={(e) => e.currentTarget.style.background = 'rgba(255,255,255,0.2)'}
                  onMouseLeave={(e) => e.currentTarget.style.background = 'rgba(255,255,255,0.1)'}
                >
                  ✕
                </button>
              </div>

              <div style={{ padding: '1.5rem' }}>
                {modalData.type === 'node' ? (
                  <>
                    <div style={{ marginBottom: '1.5rem' }}>
                      <div style={{ 
                        display: 'grid', 
                        gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', 
                        gap: '1rem',
                        marginBottom: '1.5rem'
                      }}>
                        <div style={{ 
                          padding: '1rem', 
                          background: 'rgba(255,255,255,0.05)', 
                          borderRadius: '8px',
                          border: '1px solid rgba(255,255,255,0.1)'
                        }}>
                          <div style={{ fontSize: '0.75rem', color: '#b0b0b0', marginBottom: '0.25rem' }}>Node ID</div>
                          <div style={{ fontSize: '0.875rem', fontFamily: 'monospace', wordBreak: 'break-all', color: '#ffffff' }}>
                            {(modalData.data as GraphNode).id}
                          </div>
                        </div>
                        <div style={{ 
                          padding: '1rem', 
                          background: 'rgba(255,255,255,0.05)', 
                          borderRadius: '8px',
                          border: '1px solid rgba(255,255,255,0.1)'
                        }}>
                          <div style={{ fontSize: '0.75rem', color: '#b0b0b0', marginBottom: '0.25rem' }}>Type</div>
                          <div style={{ fontSize: '1rem', fontWeight: 600, color: '#ffffff' }}>
                            {(modalData.data as GraphNode).type}
                          </div>
                        </div>
                        <div style={{ 
                          padding: '1rem', 
                          background: 'rgba(255,255,255,0.05)', 
                          borderRadius: '8px',
                          border: '1px solid rgba(255,255,255,0.1)'
                        }}>
                          <div style={{ fontSize: '0.75rem', color: '#b0b0b0', marginBottom: '0.25rem' }}>Layer</div>
                          <div style={{ fontSize: '1rem', fontWeight: 600, color: '#ffffff' }}>
                            {(modalData.data as GraphNode).layer}
                          </div>
                        </div>
                      </div>

                      {modalData.score && (
                        <div style={{ 
                          padding: '1.25rem', 
                          background: 'linear-gradient(135deg, rgba(78, 205, 196, 0.1), rgba(69, 183, 209, 0.1))', 
                          borderRadius: '12px',
                          border: '1px solid rgba(78, 205, 196, 0.2)',
                          marginBottom: '1.5rem'
                        }}>
                          <div style={{ fontSize: '0.875rem', fontWeight: 600, marginBottom: '1rem', color: '#ffffff' }}>
                            📊 Scoring Breakdown
                          </div>
                          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(120px, 1fr))', gap: '1rem' }}>
                            <div style={{ textAlign: 'center' }}>
                              <div style={{ fontSize: '0.75rem', color: '#b0b0b0', marginBottom: '0.25rem' }}>Recency</div>
                              <div style={{ fontSize: '1.5rem', fontWeight: 700, color: '#4ecdc4' }}>
                                {modalData.score.recency_score.toFixed(1)}
                              </div>
                            </div>
                            <div style={{ textAlign: 'center' }}>
                              <div style={{ fontSize: '0.75rem', color: '#b0b0b0', marginBottom: '0.25rem' }}>Authority</div>
                              <div style={{ fontSize: '1.5rem', fontWeight: 700, color: '#96ceb4' }}>
                                {modalData.score.authority_score.toFixed(1)}
                              </div>
                            </div>
                            <div style={{ textAlign: 'center' }}>
                              <div style={{ fontSize: '0.75rem', color: '#b0b0b0', marginBottom: '0.25rem' }}>Stage</div>
                              <div style={{ fontSize: '1.5rem', fontWeight: 700, color: '#45b7d1' }}>
                                {modalData.score.stage_score.toFixed(1)}
                              </div>
                            </div>
                            <div style={{ textAlign: 'center' }}>
                              <div style={{ fontSize: '0.75rem', color: '#b0b0b0', marginBottom: '0.25rem' }}>Rule</div>
                              <div style={{ fontSize: '1.5rem', fontWeight: 700, color: '#ffeaa7' }}>
                                {modalData.score.rule_score.toFixed(1)}
                              </div>
                            </div>
                            <div style={{ 
                              textAlign: 'center', 
                              gridColumn: 'span 2',
                              padding: '0.75rem',
                              background: 'rgba(255,255,255,0.05)',
                              borderRadius: '8px'
                            }}>
                              <div style={{ fontSize: '0.75rem', color: '#b0b0b0', marginBottom: '0.25rem' }}>Total Score</div>
                              <div style={{ fontSize: '2rem', fontWeight: 700, background: 'linear-gradient(135deg, #4ecdc4, #45b7d1)', WebkitBackgroundClip: 'text', WebkitTextFillColor: 'transparent' }}>
                                {modalData.score.total_score.toFixed(2)}
                              </div>
                            </div>
                          </div>
                        </div>
                      )}

                      <div>
                        <div style={{ fontSize: '0.875rem', fontWeight: 600, marginBottom: '0.75rem', color: '#ffffff' }}>
                          📋 Properties
                        </div>
                        <div style={{ 
                          background: 'rgba(0,0,0,0.4)', 
                          borderRadius: '8px', 
                          padding: '1rem',
                          border: '1px solid rgba(255,255,255,0.1)',
                          maxHeight: '300px',
                          overflow: 'auto'
                        }}>
                          {Object.entries((modalData.data as GraphNode).props).map(([key, value]) => (
                            <div key={key} style={{ marginBottom: '0.75rem', paddingBottom: '0.75rem', borderBottom: '1px solid rgba(255,255,255,0.05)' }}>
                              <div style={{ fontSize: '0.75rem', color: '#b0b0b0', marginBottom: '0.25rem', textTransform: 'uppercase', letterSpacing: '0.05em' }}>
                                {key.replace(/_/g, ' ')}
                              </div>
                              <div style={{ fontSize: '0.875rem', fontFamily: typeof value === 'string' && value.length > 50 ? 'inherit' : 'monospace', wordBreak: 'break-word', color: '#ffffff' }}>
                                {typeof value === 'object' ? JSON.stringify(value, null, 2) : String(value)}
                              </div>
                            </div>
                          ))}
                        </div>
                      </div>
                    </div>
                  </>
                ) : (
                  <>
                    <div style={{ marginBottom: '1.5rem' }}>
                      <div style={{ 
                        display: 'grid', 
                        gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', 
                        gap: '1rem',
                        marginBottom: '1.5rem'
                      }}>
                        <div style={{ 
                          padding: '1rem', 
                          background: 'rgba(255,255,255,0.05)', 
                          borderRadius: '8px',
                          border: '1px solid rgba(255,255,255,0.1)'
                        }}>
                          <div style={{ fontSize: '0.75rem', color: '#b0b0b0', marginBottom: '0.25rem' }}>Edge Type</div>
                          <div style={{ fontSize: '1rem', fontWeight: 600, color: '#ffffff' }}>
                            {(modalData.data as GraphEdge).type}
                          </div>
                        </div>
                        <div style={{ 
                          padding: '1rem', 
                          background: 'rgba(255,255,255,0.05)', 
                          borderRadius: '8px',
                          border: '1px solid rgba(255,255,255,0.1)'
                        }}>
                          <div style={{ fontSize: '0.75rem', color: '#b0b0b0', marginBottom: '0.25rem' }}>Source</div>
                          <div style={{ fontSize: '0.875rem', fontFamily: 'monospace', wordBreak: 'break-all', color: '#ffffff' }}>
                            {(modalData.data as GraphEdge).src}
                          </div>
                        </div>
                        <div style={{ 
                          padding: '1rem', 
                          background: 'rgba(255,255,255,0.05)', 
                          borderRadius: '8px',
                          border: '1px solid rgba(255,255,255,0.1)'
                        }}>
                          <div style={{ fontSize: '0.75rem', color: '#b0b0b0', marginBottom: '0.25rem' }}>Destination</div>
                          <div style={{ fontSize: '0.875rem', fontFamily: 'monospace', wordBreak: 'break-all', color: '#ffffff' }}>
                            {(modalData.data as GraphEdge).dst}
                          </div>
                        </div>
                      </div>

                      {Object.keys((modalData.data as GraphEdge).props).length > 0 && (
                        <div>
                          <div style={{ fontSize: '0.875rem', fontWeight: 600, marginBottom: '0.75rem', color: '#ffffff' }}>
                            📋 Edge Properties
                          </div>
                          <div style={{ 
                            background: 'rgba(0,0,0,0.4)', 
                            borderRadius: '8px', 
                            padding: '1rem',
                            border: '1px solid rgba(255,255,255,0.1)'
                          }}>
                            {Object.entries((modalData.data as GraphEdge).props).map(([key, value]) => (
                              <div key={key} style={{ marginBottom: '0.75rem', paddingBottom: '0.75rem', borderBottom: '1px solid rgba(255,255,255,0.05)' }}>
                                <div style={{ fontSize: '0.75rem', color: '#b0b0b0', marginBottom: '0.25rem', textTransform: 'uppercase', letterSpacing: '0.05em' }}>
                                  {key.replace(/_/g, ' ')}
                                </div>
                                <div style={{ fontSize: '0.875rem', fontFamily: 'monospace', wordBreak: 'break-word', color: '#ffffff' }}>
                                  {typeof value === 'object' ? JSON.stringify(value, null, 2) : String(value)}
                                </div>
                              </div>
                            ))}
                          </div>
                        </div>
                      )}
                    </div>
                  </>
                )}
              </div>
            </div>
          </div>
        )}
      </div>

      <style jsx>{`
        @keyframes fadeIn {
          from { opacity: 0; }
          to { opacity: 1; }
        }
        @keyframes slideUp {
          from { 
            opacity: 0;
            transform: translateY(20px);
          }
          to { 
            opacity: 1;
            transform: translateY(0);
          }
        }
      `}</style>
    </main>
  )
}
