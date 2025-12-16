import type { Metadata } from 'next'
import './globals.css'

export const metadata: Metadata = {
  title: 'Email RAG Search',
  description: 'Search and ask questions about your company emails',
}

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  )
}

