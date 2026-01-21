'use client'

import { useEffect } from 'react'
import { useRouter } from 'next/navigation'

export default function GraphRedirect() {
  const router = useRouter()

  useEffect(() => {
    // Redirect to the unified query page
    router.push('/query')
  }, [router])

  return (
    <main className="dashboard">
      <div className="dashboard-bg">
        <div className="orb orb-a" />
        <div className="orb orb-b" />
        <div className="orb orb-c" />
        <div className="grid-overlay" />
      </div>

      <div className="dashboard-content">
        <section className="card">
          <div style={{
            textAlign: 'center',
            padding: '3rem',
            fontSize: '1.125rem',
            color: 'var(--muted)'
          }}>
            🔄 Redirecting to unified Ask & Explore page...
          </div>
        </section>
      </div>
    </main>
  )
}
