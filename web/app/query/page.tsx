'use client'

import Link from 'next/link'
import { FormEvent, useState } from 'react'

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

interface QueryResponse {
  success: boolean
  answer: string
  citations: Citation[]
  retrieved_emails: RetrievedEmail[]
}

export default function QueryPage() {
  const [question, setQuestion] = useState('')
  const [topK, setTopK] = useState(8)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [response, setResponse] = useState<QueryResponse | null>(null)

  const runQuery = async () => {
    if (!question.trim()) {
      setError('Please enter a question to search your emails.')
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

  const handleSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault()
    if (loading) return
    await runQuery()
  }

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
              Ask emails
            </Link>
            <Link className="nav-link" href="/graph">
              Context Graph
            </Link>
          </div>
        </nav>

        <section className="card query-shell">
          <div className="section-title">Ask your mailbox</div>
          <p className="hero-subtitle">
            Submit a natural-language question.{" "}
            <span className="query-hint">Press Enter to send, Shift + Enter for a new line.</span>
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
                placeholder="Ask anything about your synced inbox…"
                rows={4}
                className="query-textarea"
              />

              <div className="query-sendbar">
                <div className="query-topk">
                  <label className="query-label" htmlFor="topk">
                    Results: {topK}
                  </label>
                  <input
                    id="topk"
                    type="range"
                    min={1}
                    max={20}
                    value={topK}
                    onChange={(event) => setTopK(Number(event.target.value))}
                  />
                </div>

                <button className="query-button" disabled={loading} type="submit">
                  {loading ? 'Searching…' : 'Send →'}
                </button>
              </div>
            </div>
          </form>

          {error && <p className="query-error">{error}</p>}
        </section>

        {response && (
          <section className="query-result">
            <article className="card answer-card fade-in">
              <header>
                <span className="chip">Answer</span>
                <h2>Your synthesized response</h2>
              </header>
              <p className="answer-text">{response.answer}</p>
            </article>

            <article className="card citations-card fade-in" style={{ animationDelay: '0.1s' }}>
              <div className="section-title">Citations</div>
              {response.citations.length === 0 ? (
                <p className="muted">No citations returned.</p>
              ) : (
                <div className="citations-grid">
                  {response.citations.map((citation) => (
                    <div key={citation.id} className="citation-card">
                      <div className="citation-subject">{citation.subject}</div>
                      <div className="citation-meta">
                        {citation.sender_name && <span>{citation.sender_name}</span>}
                        {citation.received_time && (
                          <span>{new Date(citation.received_time).toLocaleString()}</span>
                        )}
                      </div>
                      <div className="citation-snippet">{citation.snippet}</div>
                    </div>
                  ))}
                </div>
              )}
            </article>
          </section>
        )}

        {response && response.retrieved_emails.length > 0 && (
          <section className="card fade-in emails-section" style={{ animationDelay: '0.2s' }}>
            <div className="section-title">Retrieved emails</div>
            <p className="muted">
              The following messages were used to ground the answer. Open the raw emails in your
              mailbox for full context.
            </p>
            <div className="email-grid">
              {response.retrieved_emails.map((email) => (
                <article key={email.id} className="email-card">
                  <div className="email-header">
                    <div className="email-subject">{email.subject || 'No Subject'}</div>
                    <div className="email-meta">
                      <span>{email.sender_name}</span>
                      <span>
                        {email.received_time
                          ? new Date(email.received_time).toLocaleString()
                          : 'Unknown'}
                      </span>
                    </div>
                  </div>
                  <p className="email-body">
                    {email.body?.slice(0, 240) || 'No preview available.'}
                  </p>
                </article>
              ))}
            </div>
          </section>
        )}
      </div>
    </main>
  )
}
