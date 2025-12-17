'use client'

import { useState } from 'react'

const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'

interface Citation {
  id: string
  subject: string
  sender: string
  sender_email: string
  received_time: string
  snippet: string
}

interface QueryResponse {
  success: boolean
  answer: string
  citations: Citation[]
  retrieved_emails: any[]
}

export default function Home() {
  const [question, setQuestion] = useState('')
  const [loading, setLoading] = useState(false)
  const [response, setResponse] = useState<QueryResponse | null>(null)
  const [error, setError] = useState<string | null>(null)

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    
    if (!question.trim()) {
      return
    }

    setLoading(true)
    setError(null)
    setResponse(null)

    try {
      const res = await fetch(`${API_BASE_URL}/query`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          question: question.trim(),
          top_k: 8,
        }),
      })

      if (!res.ok) {
        throw new Error(`API error: ${res.status}`)
      }

      const data: QueryResponse = await res.json()
      setResponse(data)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'An error occurred')
    } finally {
      setLoading(false)
    }
  }

  return (
    <main style={{ maxWidth: '1200px', margin: '0 auto', padding: '40px 20px' }}>
      <header style={{ marginBottom: '40px', textAlign: 'center' }}>
        <h1 style={{ fontSize: '32px', marginBottom: '10px', color: '#111' }}>
          Email RAG Search
        </h1>
        <p style={{ color: '#666', fontSize: '16px' }}>
          Ask questions about your company emails
        </p>
      </header>

      <form onSubmit={handleSubmit} style={{ marginBottom: '40px' }}>
        <div style={{ display: 'flex', gap: '10px' }}>
          <input
            type="text"
            value={question}
            onChange={(e) => setQuestion(e.target.value)}
            placeholder="Ask a question about your emails..."
            style={{
              flex: 1,
              padding: '15px 20px',
              fontSize: '16px',
              border: '1px solid #ddd',
              borderRadius: '8px',
              outline: 'none',
            }}
            disabled={loading}
          />
          <button
            type="submit"
            disabled={loading || !question.trim()}
            style={{
              padding: '15px 30px',
              fontSize: '16px',
              fontWeight: '600',
              backgroundColor: loading ? '#ccc' : '#0070f3',
              color: 'white',
              border: 'none',
              borderRadius: '8px',
              cursor: loading ? 'not-allowed' : 'pointer',
            }}
          >
            {loading ? 'Searching...' : 'Search'}
          </button>
        </div>
      </form>

      {error && (
        <div
          style={{
            padding: '20px',
            backgroundColor: '#fee',
            border: '1px solid #fcc',
            borderRadius: '8px',
            marginBottom: '20px',
            color: '#c33',
          }}
        >
          <strong>Error:</strong> {error}
        </div>
      )}

      {response && (
        <div>
          <div
            style={{
              padding: '30px',
              backgroundColor: 'white',
              borderRadius: '12px',
              boxShadow: '0 2px 8px rgba(0,0,0,0.1)',
              marginBottom: '30px',
            }}
          >
            <h2 style={{ fontSize: '20px', marginBottom: '15px', color: '#111' }}>
              Answer
            </h2>
            <p style={{ lineHeight: '1.6', color: '#333', whiteSpace: 'pre-wrap' }}>
              {response.answer}
            </p>
          </div>

          {response.citations && response.citations.length > 0 && (
            <div>
              <h2 style={{ fontSize: '20px', marginBottom: '20px', color: '#111' }}>
                Sources ({response.citations.length})
              </h2>
              <div style={{ display: 'grid', gap: '15px' }}>
                {response.citations.map((citation, index) => (
                  <a
                    key={citation.id || index}
                    href={`/email/${encodeURIComponent(citation.id)}`}
                    style={{
                      display: 'block',
                      padding: '20px',
                      backgroundColor: 'white',
                      borderRadius: '8px',
                      boxShadow: '0 1px 4px rgba(0,0,0,0.1)',
                      textDecoration: 'none',
                      color: 'inherit',
                      transition: 'box-shadow 0.2s',
                    }}
                    onMouseEnter={(e) => {
                      e.currentTarget.style.boxShadow = '0 4px 12px rgba(0,0,0,0.15)'
                    }}
                    onMouseLeave={(e) => {
                      e.currentTarget.style.boxShadow = '0 1px 4px rgba(0,0,0,0.1)'
                    }}
                  >
                    <div style={{ marginBottom: '10px' }}>
                      <strong style={{ fontSize: '16px', color: '#111' }}>
                        {citation.subject || 'No Subject'}
                      </strong>
                    </div>
                    <div style={{ fontSize: '14px', color: '#666', marginBottom: '8px' }}>
                      <span>From: {citation.sender || citation.sender_email}</span>
                      {citation.received_time && (
                        <span style={{ marginLeft: '15px' }}>
                          Date: {new Date(citation.received_time).toLocaleDateString()}
                        </span>
                      )}
                    </div>
                    {citation.snippet && (
                      <p style={{ fontSize: '14px', color: '#888', marginTop: '10px' }}>
                        {citation.snippet}
                      </p>
                    )}
                  </a>
                ))}
              </div>
            </div>
          )}
        </div>
      )}
    </main>
  )
}



