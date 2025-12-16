'use client'

import { useState, useEffect } from 'react'
import { useParams, useRouter } from 'next/navigation'

const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'

interface Email {
  id: string
  account: string
  folder: string
  subject: string
  sender_name: string
  sender_email: string
  received_time: string
  sent_time: string
  recipients: string
  body: string
  attachments: string
  categories: string
  is_task: boolean
  unread: boolean
}

export default function EmailDetailPage() {
  const params = useParams()
  const router = useRouter()
  const [email, setEmail] = useState<Email | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    const fetchEmail = async () => {
      try {
        const res = await fetch(`${API_BASE_URL}/emails/${params.id}`)
        
        if (!res.ok) {
          throw new Error(`API error: ${res.status}`)
        }

        const data = await res.json()
        setEmail(data.email)
      } catch (err) {
        setError(err instanceof Error ? err.message : 'An error occurred')
      } finally {
        setLoading(false)
      }
    }

    if (params.id) {
      fetchEmail()
    }
  }, [params.id])

  if (loading) {
    return (
      <main style={{ maxWidth: '900px', margin: '0 auto', padding: '40px 20px' }}>
        <p>Loading email...</p>
      </main>
    )
  }

  if (error || !email) {
    return (
      <main style={{ maxWidth: '900px', margin: '0 auto', padding: '40px 20px' }}>
        <div
          style={{
            padding: '20px',
            backgroundColor: '#fee',
            border: '1px solid #fcc',
            borderRadius: '8px',
            color: '#c33',
          }}
        >
          <strong>Error:</strong> {error || 'Email not found'}
        </div>
        <button
          onClick={() => router.back()}
          style={{
            marginTop: '20px',
            padding: '10px 20px',
            backgroundColor: '#0070f3',
            color: 'white',
            border: 'none',
            borderRadius: '6px',
            cursor: 'pointer',
          }}
        >
          Go Back
        </button>
      </main>
    )
  }

  return (
    <main style={{ maxWidth: '900px', margin: '0 auto', padding: '40px 20px' }}>
      <button
        onClick={() => router.back()}
        style={{
          padding: '10px 20px',
          marginBottom: '20px',
          backgroundColor: '#f0f0f0',
          border: '1px solid #ddd',
          borderRadius: '6px',
          cursor: 'pointer',
          fontSize: '14px',
        }}
      >
        ← Back to Search
      </button>

      <div
        style={{
          backgroundColor: 'white',
          borderRadius: '12px',
          boxShadow: '0 2px 8px rgba(0,0,0,0.1)',
          padding: '30px',
        }}
      >
        <h1 style={{ fontSize: '24px', marginBottom: '20px', color: '#111' }}>
          {email.subject || 'No Subject'}
        </h1>

        <div style={{ marginBottom: '20px', paddingBottom: '20px', borderBottom: '1px solid #eee' }}>
          <div style={{ marginBottom: '10px', color: '#666' }}>
            <strong>From:</strong> {email.sender_name} &lt;{email.sender_email}&gt;
          </div>
          <div style={{ marginBottom: '10px', color: '#666' }}>
            <strong>To:</strong> {email.recipients}
          </div>
          <div style={{ marginBottom: '10px', color: '#666' }}>
            <strong>Date:</strong>{' '}
            {email.received_time
              ? new Date(email.received_time).toLocaleString()
              : 'Unknown'}
          </div>
          <div style={{ marginBottom: '10px', color: '#666' }}>
            <strong>Folder:</strong> {email.folder}
          </div>
          {email.categories && (
            <div style={{ marginBottom: '10px', color: '#666' }}>
              <strong>Categories:</strong> {email.categories}
            </div>
          )}
        </div>

        <div style={{ lineHeight: '1.6', color: '#333', whiteSpace: 'pre-wrap' }}>
          {email.body}
        </div>

        {email.attachments && (
          <div
            style={{
              marginTop: '20px',
              paddingTop: '20px',
              borderTop: '1px solid #eee',
              color: '#666',
            }}
          >
            <strong>Attachments:</strong> {email.attachments}
          </div>
        )}
      </div>
    </main>
  )
}

