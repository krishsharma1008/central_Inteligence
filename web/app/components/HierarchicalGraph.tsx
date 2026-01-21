'use client'

import { useRef, useEffect, useState } from 'react'
import dagre from 'dagre'

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

interface HierarchicalGraphProps {
  nodes: GraphNode[]
  edges: GraphEdge[]
  scores?: Record<string, NodeScore>
  width?: number
  height?: number
  onNodeClick?: (node: any) => void
  highlightPaths?: boolean
  questionNodeId?: string
}

export default function HierarchicalGraph({
  nodes,
  edges,
  scores = {},
  width = 1200,
  height = 600,
  onNodeClick,
  highlightPaths = true,
  questionNodeId
}: HierarchicalGraphProps) {
  const svgRef = useRef<SVGSVGElement>(null)
  const [hoveredNode, setHoveredNode] = useState<string | null>(null)
  const [selectedNode, setSelectedNode] = useState<string | null>(null)

  useEffect(() => {
    if (!svgRef.current || !nodes.length) return

    // Clear previous content
    const svg = svgRef.current
    while (svg.firstChild) {
      svg.removeChild(svg.firstChild)
    }

    // Create dagre graph
    const g = new dagre.graphlib.Graph()
    g.setGraph({ 
      rankdir: 'TB',  // Top to bottom
      nodesep: 80,    // Horizontal spacing
      ranksep: 120,   // Vertical spacing
      marginx: 50,
      marginy: 50
    })
    g.setDefaultEdgeLabel(() => ({}))

    // Add nodes to dagre
    nodes.forEach(node => {
      const label = node.props.subject || node.props.display_name || node.props.email || node.props.filename || node.type
      g.setNode(node.id, { 
        label,
        width: 180, 
        height: 60,
        node: node,
        score: scores[node.id]?.total_score || 0
      })
    })

    // Add edges to dagre
    edges.forEach(edge => {
      g.setEdge(edge.src, edge.dst, { edge })
    })

    // Compute layout
    dagre.layout(g)

    // Find paths to highlight
    const pathEdges = new Set<string>()
    if (highlightPaths && questionNodeId) {
      const answerNodes = nodes
        .filter(n => n.type === 'Document' && (scores[n.id]?.total_score || 0) > 5)
        .map(n => n.id)
      
      const paths = findAllPaths(questionNodeId, answerNodes, edges)
      paths.forEach(path => {
        for (let i = 0; i < path.length - 1; i++) {
          const edgeId = edges.find(e => e.src === path[i] && e.dst === path[i + 1])?.id
          if (edgeId) pathEdges.add(edgeId)
        }
      })
    }

    // Get layout dimensions
    const graphWidth = (g.graph() as any).width || width
    const graphHeight = (g.graph() as any).height || height

    // Create SVG group
    const svgNS = "http://www.w3.org/2000/svg"
    const mainGroup = document.createElementNS(svgNS, "g")
    mainGroup.setAttribute("transform", `translate(${(width - graphWidth) / 2}, 20)`)
    svg.appendChild(mainGroup)

    // Draw edges first (so nodes appear on top)
    edges.forEach(edge => {
      const edgeData = g.edge(edge.src, edge.dst)
      if (!edgeData || !edgeData.points) return

      const isPathEdge = pathEdges.has(edge.id)
      const points = edgeData.points

      // Draw edge line
      const path = document.createElementNS(svgNS, "path")
      const pathData = points.map((p: any, i: number) => 
        `${i === 0 ? 'M' : 'L'} ${p.x} ${p.y}`
      ).join(' ')
      
      path.setAttribute("d", pathData)
      path.setAttribute("fill", "none")
      path.setAttribute("stroke", isPathEdge ? getPathEdgeColor(edge.type) : getEdgeColor(edge.type))
      path.setAttribute("stroke-width", isPathEdge ? "3" : "2")
      path.setAttribute("opacity", isPathEdge ? "0.9" : "0.4")
      path.setAttribute("marker-end", `url(#arrowhead-${isPathEdge ? 'highlight' : 'normal'})`)
      mainGroup.appendChild(path)

      // Add edge label
      if (points.length > 0) {
        const midPoint = points[Math.floor(points.length / 2)]
        const text = document.createElementNS(svgNS, "text")
        text.setAttribute("x", String(midPoint.x))
        text.setAttribute("y", String(midPoint.y - 5))
        text.setAttribute("text-anchor", "middle")
        text.setAttribute("font-size", "11")
        text.setAttribute("fill", isPathEdge ? "var(--accent)" : "var(--muted)")
        text.setAttribute("font-weight", isPathEdge ? "600" : "400")
        text.textContent = edge.type.replace(/_/g, ' ')
        mainGroup.appendChild(text)
      }
    })

    // Draw nodes
    nodes.forEach(node => {
      const nodeData = g.node(node.id)
      if (!nodeData) return

      const isQuestionNode = node.id === questionNodeId
      const isAnswerNode = node.type === 'Document' && (scores[node.id]?.total_score || 0) > 5
      const isHovered = hoveredNode === node.id
      const isSelected = selectedNode === node.id

      // Node group
      const nodeGroup = document.createElementNS(svgNS, "g")
      nodeGroup.setAttribute("transform", `translate(${nodeData.x}, ${nodeData.y})`)
      nodeGroup.style.cursor = "pointer"

      // Node rectangle
      const rect = document.createElementNS(svgNS, "rect")
      rect.setAttribute("x", String(-nodeData.width / 2))
      rect.setAttribute("y", String(-nodeData.height / 2))
      rect.setAttribute("width", String(nodeData.width))
      rect.setAttribute("height", String(nodeData.height))
      rect.setAttribute("rx", "12")
      rect.setAttribute("fill", getNodeColor(node.type))
      rect.setAttribute("stroke", isQuestionNode ? "#ff6b6b" : isAnswerNode ? "#3fa37b" : "rgba(255,255,255,0.2)")
      rect.setAttribute("stroke-width", isQuestionNode || isAnswerNode ? "3" : isHovered || isSelected ? "2" : "1")
      rect.setAttribute("opacity", "0.95")
      
      if (isQuestionNode || isAnswerNode) {
        rect.setAttribute("filter", "drop-shadow(0 0 8px rgba(31, 122, 140, 0.5))")
      }

      nodeGroup.appendChild(rect)

      // Node label
      const label = nodeData.label || node.id
      const text = document.createElementNS(svgNS, "text")
      text.setAttribute("x", "0")
      text.setAttribute("y", "-5")
      text.setAttribute("text-anchor", "middle")
      text.setAttribute("font-size", "13")
      text.setAttribute("font-weight", "600")
      text.setAttribute("fill", "#10131a")
      
      // Truncate long labels
      const maxLength = 25
      const truncated = label.length > maxLength ? label.substring(0, maxLength) + '...' : label
      text.textContent = truncated
      nodeGroup.appendChild(text)

      // Node type
      const typeText = document.createElementNS(svgNS, "text")
      typeText.setAttribute("x", "0")
      typeText.setAttribute("y", "10")
      typeText.setAttribute("text-anchor", "middle")
      typeText.setAttribute("font-size", "10")
      typeText.setAttribute("fill", "#5b646f")
      typeText.textContent = node.type
      nodeGroup.appendChild(typeText)

      // Score badge
      if (scores[node.id]) {
        const score = scores[node.id].total_score
        const scoreText = document.createElementNS(svgNS, "text")
        scoreText.setAttribute("x", String(nodeData.width / 2 - 25))
        scoreText.setAttribute("y", String(-nodeData.height / 2 + 15))
        scoreText.setAttribute("font-size", "10")
        scoreText.setAttribute("font-weight", "700")
        scoreText.setAttribute("fill", "#1f7a8c")
        scoreText.textContent = `${score.toFixed(1)}`
        nodeGroup.appendChild(scoreText)
      }

      // Event handlers
      nodeGroup.addEventListener('mouseenter', () => setHoveredNode(node.id))
      nodeGroup.addEventListener('mouseleave', () => setHoveredNode(null))
      nodeGroup.addEventListener('click', () => {
        setSelectedNode(node.id)
        if (onNodeClick) {
          onNodeClick({
            ...node,
            score: scores[node.id]
          })
        }
      })

      mainGroup.appendChild(nodeGroup)
    })

    // Add arrow markers
    const defs = document.createElementNS(svgNS, "defs")
    
    // Normal arrow
    const normalMarker = document.createElementNS(svgNS, "marker")
    normalMarker.setAttribute("id", "arrowhead-normal")
    normalMarker.setAttribute("markerWidth", "10")
    normalMarker.setAttribute("markerHeight", "10")
    normalMarker.setAttribute("refX", "9")
    normalMarker.setAttribute("refY", "3")
    normalMarker.setAttribute("orient", "auto")
    const normalPath = document.createElementNS(svgNS, "path")
    normalPath.setAttribute("d", "M0,0 L0,6 L9,3 z")
    normalPath.setAttribute("fill", "rgba(91, 100, 111, 0.6)")
    normalMarker.appendChild(normalPath)
    defs.appendChild(normalMarker)

    // Highlight arrow
    const highlightMarker = document.createElementNS(svgNS, "marker")
    highlightMarker.setAttribute("id", "arrowhead-highlight")
    highlightMarker.setAttribute("markerWidth", "10")
    highlightMarker.setAttribute("markerHeight", "10")
    highlightMarker.setAttribute("refX", "9")
    highlightMarker.setAttribute("refY", "3")
    highlightMarker.setAttribute("orient", "auto")
    const highlightPath = document.createElementNS(svgNS, "path")
    highlightPath.setAttribute("d", "M0,0 L0,6 L9,3 z")
    highlightPath.setAttribute("fill", "#1f7a8c")
    highlightMarker.appendChild(highlightPath)
    defs.appendChild(highlightMarker)

    svg.appendChild(defs)

  }, [nodes, edges, scores, width, height, hoveredNode, selectedNode, highlightPaths, questionNodeId, onNodeClick])

  return (
    <div style={{ width: '100%', height: '100%', overflow: 'auto', background: 'linear-gradient(135deg, rgba(31, 122, 140, 0.03), rgba(63, 163, 123, 0.03))' }}>
      <svg
        ref={svgRef}
        width={width}
        height={height}
        style={{ display: 'block', margin: '0 auto' }}
      />
    </div>
  )
}

// Helper functions
function getNodeColor(type: string): string {
  const colors: Record<string, string> = {
    Intent: '#ff6b6b',
    Conversation: '#4ecdc4',
    Document: '#45b7d1',
    User: '#96ceb4',
    Attachment: '#ffeaa7',
    Rule: '#dfe6e9'
  }
  return colors[type] || '#95a5a6'
}

function getEdgeColor(type: string): string {
  const colors: Record<string, string> = {
    PART_OF: 'rgba(78, 205, 196, 0.4)',
    HAS_ATTACHMENT: 'rgba(255, 234, 167, 0.4)',
    SENT_BY: 'rgba(150, 206, 180, 0.4)',
    SENT_TO: 'rgba(150, 206, 180, 0.3)',
    FOLLOWS: 'rgba(69, 183, 209, 0.4)',
    SEEKS_ANSWER_TO: 'rgba(255, 107, 107, 0.4)'
  }
  return colors[type] || 'rgba(149, 165, 166, 0.3)'
}

function getPathEdgeColor(type: string): string {
  return '#1f7a8c'
}

function findAllPaths(
  start: string,
  ends: string[],
  edges: GraphEdge[]
): string[][] {
  const paths: string[][] = []
  const visited = new Set<string>()
  
  function dfs(current: string, path: string[]) {
    if (ends.includes(current)) {
      paths.push([...path])
      return
    }
    
    visited.add(current)
    
    const outgoingEdges = edges.filter(e => e.src === current)
    for (const edge of outgoingEdges) {
      if (!visited.has(edge.dst)) {
        dfs(edge.dst, [...path, edge.dst])
      }
    }
    
    visited.delete(current)
  }
  
  dfs(start, [start])
  return paths
}
