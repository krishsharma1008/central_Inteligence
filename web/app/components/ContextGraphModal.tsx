'use client'

import { useRef, useEffect } from 'react'
import dynamic from 'next/dynamic'

const ForceGraph2D = dynamic(() => import('react-force-graph-2d'), { ssr: false })

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

interface ContextGraphModalProps {
  packet: ContextPacket
  onClose: () => void
}

export default function ContextGraphModal({ packet, onClose }: ContextGraphModalProps) {
  const graphRef = useRef<any>()
  const containerRef = useRef<HTMLDivElement>(null)

  const getGraphData = () => {
    const nodes = packet.nodes.map(n => ({
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

  const sortedScores = Object.values(packet.scores).sort((a, b) => b.total_score - a.total_score)

  return (
    <div 
      style={{ 
        position: 'fixed', 
        top: 0, 
        left: 0, 
        right: 0, 
        bottom: 0, 
        background: 'linear-gradient(135deg, rgba(16, 19, 26, 0.95), rgba(31, 122, 140, 0.15))', 
        backdropFilter: 'blur(16px)',
        display: 'flex', 
        alignItems: 'center', 
        justifyContent: 'center', 
        zIndex: 1000,
        padding: '2rem',
        animation: 'fadeIn 0.3s ease-out'
      }}
      onClick={onClose}
    >
      <div 
        style={{ 
          background: 'linear-gradient(135deg, rgba(255, 255, 255, 0.98), rgba(247, 239, 230, 0.95))',
          borderRadius: '24px',
          maxWidth: '95vw',
          width: '1400px',
          maxHeight: '90vh',
          overflow: 'auto',
          border: '2px solid rgba(31, 122, 140, 0.3)',
          boxShadow: '0 30px 90px rgba(0,0,0,0.3), 0 0 60px rgba(31, 122, 140, 0.15)',
          animation: 'slideUp 0.4s cubic-bezier(0.34, 1.56, 0.64, 1)'
        }}
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header */}
        <div style={{ 
          padding: '2rem', 
          borderBottom: '2px solid rgba(31, 122, 140, 0.15)',
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          background: 'linear-gradient(135deg, rgba(31, 122, 140, 0.08), rgba(63, 163, 123, 0.08))',
          borderRadius: '24px 24px 0 0'
        }}>
          <div>
            <h2 style={{ 
              margin: 0, 
              fontSize: '2rem', 
              fontWeight: 700, 
              marginBottom: '0.5rem', 
              color: 'var(--ink)',
              display: 'flex',
              alignItems: 'center',
              gap: '0.75rem'
            }}>
              <span style={{ 
                fontSize: '2.5rem',
                background: 'linear-gradient(135deg, #1f7a8c, #3fa37b)',
                borderRadius: '16px',
                padding: '0.5rem',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                boxShadow: '0 8px 20px rgba(31, 122, 140, 0.2)'
              }}>🔵</span>
              Context Graph Explorer
            </h2>
            <div style={{ 
              fontSize: '1rem', 
              color: 'var(--muted)',
              display: 'flex',
              gap: '1.5rem',
              marginTop: '0.5rem',
              flexWrap: 'wrap'
            }}>
              <span style={{ 
                display: 'inline-flex',
                alignItems: 'center',
                gap: '0.5rem',
                padding: '0.5rem 1rem',
                background: 'rgba(31, 122, 140, 0.1)',
                borderRadius: '10px',
                fontWeight: 600,
                color: 'var(--accent)'
              }}>
                <span>📊</span> {packet.nodes.length} nodes
              </span>
              <span style={{ 
                display: 'inline-flex',
                alignItems: 'center',
                gap: '0.5rem',
                padding: '0.5rem 1rem',
                background: 'rgba(63, 163, 123, 0.1)',
                borderRadius: '10px',
                fontWeight: 600,
                color: 'var(--mint)'
              }}>
                <span>🔗</span> {packet.edges.length} edges
              </span>
              <span style={{ 
                display: 'inline-flex',
                alignItems: 'center',
                gap: '0.5rem',
                padding: '0.5rem 1rem',
                background: 'rgba(244, 162, 89, 0.1)',
                borderRadius: '10px',
                fontWeight: 600,
                color: 'var(--accent-2)'
              }}>
                <span>⚡</span> {packet.trace.duration_ms.toFixed(1)}ms
              </span>
            </div>
          </div>
          <button
            onClick={onClose}
            style={{
              background: 'linear-gradient(135deg, rgba(229, 107, 111, 0.15), rgba(244, 162, 89, 0.15))',
              border: '2px solid rgba(229, 107, 111, 0.3)',
              borderRadius: '12px',
              width: '48px',
              height: '48px',
              cursor: 'pointer',
              fontSize: '1.5rem',
              color: 'var(--accent-3)',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              transition: 'all 0.3s ease',
              fontWeight: 700
            }}
            onMouseEnter={(e) => {
              e.currentTarget.style.background = 'linear-gradient(135deg, rgba(229, 107, 111, 0.25), rgba(244, 162, 89, 0.25))'
              e.currentTarget.style.transform = 'rotate(90deg)'
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.background = 'linear-gradient(135deg, rgba(229, 107, 111, 0.15), rgba(244, 162, 89, 0.15))'
              e.currentTarget.style.transform = 'rotate(0deg)'
            }}
          >
            ✕
          </button>
        </div>

        {/* Graph Visualization */}
        <div ref={containerRef} style={{ 
          height: '60vh', 
          minHeight: '400px', 
          background: 'linear-gradient(135deg, rgba(31, 122, 140, 0.05), rgba(63, 163, 123, 0.05))', 
          position: 'relative', 
          overflow: 'hidden',
          margin: '1rem',
          borderRadius: '16px',
          border: '2px solid rgba(31, 122, 140, 0.15)',
          boxShadow: 'inset 0 2px 8px rgba(0,0,0,0.05)'
        }}>
          <ForceGraph2D
            ref={graphRef}
            graphData={getGraphData()}
            width={containerRef.current?.clientWidth || 800}
            height={600}
            nodeLabel={(node: any) => `${node.type}: ${node.name}\n💯 Score: ${node.score.toFixed(2)}`}
            nodeColor={getNodeColor}
            nodeVal={(node: any) => getNodeSize(node)}
            nodeCanvasObject={(node: any, ctx: any, globalScale: number) => {
              const label = node.name
              const fontSize = 12 / globalScale
              ctx.font = `${fontSize}px Sans-Serif`
              
              const nodeSize = getNodeSize(node)
              
              ctx.fillStyle = getNodeColor(node)
              ctx.beginPath()
              ctx.arc(node.x, node.y, nodeSize, 0, 2 * Math.PI, false)
              ctx.fill()

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
            backgroundColor="transparent"
            cooldownTicks={50}
            d3VelocityDecay={0.2}
            d3AlphaDecay={0.02}
            warmupTicks={50}
            enableNodeDrag={true}
            enableZoomInteraction={true}
            enablePanInteraction={true}
          />
          <div style={{ 
            position: 'absolute', 
            top: '1rem', 
            left: '1rem', 
            background: 'linear-gradient(135deg, rgba(255, 255, 255, 0.95), rgba(247, 239, 230, 0.95))', 
            padding: '1rem 1.25rem', 
            borderRadius: '12px', 
            fontSize: '0.875rem', 
            backdropFilter: 'blur(16px)', 
            border: '2px solid rgba(31, 122, 140, 0.2)',
            boxShadow: '0 8px 24px rgba(0,0,0,0.1)'
          }}>
            <div style={{ fontWeight: 700, color: 'var(--accent)', display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
              <span>💡</span> Scroll to zoom • Drag to pan
            </div>
          </div>
        </div>

        {/* Legend */}
        <div style={{ 
          padding: '1.5rem', 
          display: 'flex', 
          gap: '1rem', 
          flexWrap: 'wrap', 
          justifyContent: 'center', 
          borderBottom: '2px solid rgba(31, 122, 140, 0.15)',
          background: 'rgba(31, 122, 140, 0.03)'
        }}>
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
              cursor: 'default',
              animation: 'fadeIn 0.5s ease-out',
              animationDelay: `${idx * 0.1}s`,
              animationFillMode: 'backwards'
            }}
            onMouseEnter={(e) => {
              e.currentTarget.style.transform = 'translateY(-2px)'
              e.currentTarget.style.boxShadow = '0 6px 16px rgba(0,0,0,0.1)'
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.transform = 'translateY(0)'
              e.currentTarget.style.boxShadow = '0 4px 12px rgba(0,0,0,0.05)'
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

        {/* Trace Statistics */}
        <div style={{ padding: '2rem', background: 'rgba(31, 122, 140, 0.03)' }}>
          <div style={{ 
            fontSize: '1.25rem', 
            fontWeight: 700, 
            marginBottom: '1.5rem', 
            color: 'var(--ink)',
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
            }}
            onMouseEnter={(e) => {
              e.currentTarget.style.transform = 'translateY(-4px)'
              e.currentTarget.style.boxShadow = '0 8px 20px rgba(0,0,0,0.1)'
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.transform = 'translateY(0)'
              e.currentTarget.style.boxShadow = '0 4px 12px rgba(0,0,0,0.05)'
            }}>
              <div style={{ fontSize: '0.875rem', color: 'var(--muted)', marginBottom: '0.5rem', fontWeight: 600 }}>Candidates</div>
              <div style={{ fontSize: '2rem', fontWeight: 700, color: 'var(--accent)' }}>{packet.trace.candidate_count}</div>
            </div>
            <div style={{ 
              padding: '1.5rem', 
              background: 'linear-gradient(135deg, rgba(229, 107, 111, 0.1), rgba(229, 107, 111, 0.05))', 
              borderRadius: '16px', 
              border: '2px solid rgba(229, 107, 111, 0.2)',
              boxShadow: '0 4px 12px rgba(0,0,0,0.05)',
              transition: 'all 0.3s ease'
            }}
            onMouseEnter={(e) => {
              e.currentTarget.style.transform = 'translateY(-4px)'
              e.currentTarget.style.boxShadow = '0 8px 20px rgba(0,0,0,0.1)'
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.transform = 'translateY(0)'
              e.currentTarget.style.boxShadow = '0 4px 12px rgba(0,0,0,0.05)'
            }}>
              <div style={{ fontSize: '0.875rem', color: 'var(--muted)', marginBottom: '0.5rem', fontWeight: 600 }}>Pruned</div>
              <div style={{ fontSize: '2rem', fontWeight: 700, color: 'var(--accent-3)' }}>{packet.trace.pruned_count}</div>
            </div>
            <div style={{ 
              padding: '1.5rem', 
              background: 'linear-gradient(135deg, rgba(63, 163, 123, 0.1), rgba(63, 163, 123, 0.05))', 
              borderRadius: '16px', 
              border: '2px solid rgba(63, 163, 123, 0.2)',
              boxShadow: '0 4px 12px rgba(0,0,0,0.05)',
              transition: 'all 0.3s ease'
            }}
            onMouseEnter={(e) => {
              e.currentTarget.style.transform = 'translateY(-4px)'
              e.currentTarget.style.boxShadow = '0 8px 20px rgba(0,0,0,0.1)'
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.transform = 'translateY(0)'
              e.currentTarget.style.boxShadow = '0 4px 12px rgba(0,0,0,0.05)'
            }}>
              <div style={{ fontSize: '0.875rem', color: 'var(--muted)', marginBottom: '0.5rem', fontWeight: 600 }}>Final</div>
              <div style={{ fontSize: '2rem', fontWeight: 700, color: 'var(--mint)' }}>{packet.trace.final_count}</div>
            </div>
            <div style={{ 
              padding: '1.5rem', 
              background: 'linear-gradient(135deg, rgba(244, 162, 89, 0.1), rgba(244, 162, 89, 0.05))', 
              borderRadius: '16px', 
              border: '2px solid rgba(244, 162, 89, 0.2)',
              boxShadow: '0 4px 12px rgba(0,0,0,0.05)',
              transition: 'all 0.3s ease'
            }}
            onMouseEnter={(e) => {
              e.currentTarget.style.transform = 'translateY(-4px)'
              e.currentTarget.style.boxShadow = '0 8px 20px rgba(0,0,0,0.1)'
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.transform = 'translateY(0)'
              e.currentTarget.style.boxShadow = '0 4px 12px rgba(0,0,0,0.05)'
            }}>
              <div style={{ fontSize: '0.875rem', color: 'var(--muted)', marginBottom: '0.5rem', fontWeight: 600 }}>Duration</div>
              <div style={{ fontSize: '2rem', fontWeight: 700, color: 'var(--accent-2)' }}>{packet.trace.duration_ms.toFixed(1)}ms</div>
            </div>
          </div>
        </div>

        {/* Top Scoring Nodes */}
        <div style={{ padding: '2rem', borderTop: '2px solid rgba(31, 122, 140, 0.15)' }}>
          <div style={{ 
            fontSize: '1.25rem', 
            fontWeight: 700, 
            marginBottom: '1.5rem', 
            color: 'var(--ink)',
            display: 'flex',
            alignItems: 'center',
            gap: '0.5rem'
          }}>
            <span>🏆</span> Top Scoring Nodes
          </div>
          <div style={{ maxHeight: '300px', overflow: 'auto', paddingRight: '0.5rem' }}>
            {sortedScores.slice(0, 10).map((score, idx) => {
              const node = packet.nodes.find(n => n.id === score.node_id)
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
                  cursor: 'default',
                  animation: 'fadeIn 0.5s ease-out',
                  animationDelay: `${idx * 0.05}s`,
                  animationFillMode: 'backwards'
                }}
                onMouseEnter={(e) => {
                  e.currentTarget.style.transform = 'translateX(4px)'
                  e.currentTarget.style.boxShadow = '0 6px 16px rgba(31, 122, 140, 0.2)'
                }}
                onMouseLeave={(e) => {
                  e.currentTarget.style.transform = 'translateX(0)'
                  e.currentTarget.style.boxShadow = isTopThree ? '0 4px 12px rgba(31, 122, 140, 0.15)' : 'none'
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
                    {score.total_score.toFixed(2)}
                  </div>
                </div>
              )
            })}
          </div>
        </div>
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
    </div>
  )
}
