import Link from 'next/link'

const kpis = [
  {
    label: 'Engagement change',
    value: '-80%',
    detail: 'Jan-Jun',
    tone: 'down',
    note: 'Account engagement',
  },
  {
    label: 'iOS rating',
    value: '4.4',
    detail: 'App Store',
    tone: 'up',
    note: 'Stabilized after handover',
  },
  {
    label: 'Android rating',
    value: '<2.0',
    detail: 'Play Store',
    tone: 'critical',
    note: 'Below target',
  },
  {
    label: 'Productivity gains',
    value: '+35%',
    detail: 'Pilot results',
    tone: 'up',
    note: 'Copilot and Windsurf',
  },
  {
    label: 'GTM runway',
    value: '7-8 mo',
    detail: 'Hospitality + airline',
    tone: 'neutral',
    note: 'Joint GTM effort',
  },
  {
    label: 'Ownership shift',
    value: 'Post-Feb',
    detail: 'In-house',
    tone: 'neutral',
    note: 'Support scaled down',
  },
]

const ratingBars = [
  {
    label: 'iOS rating',
    value: '4.4 / 5',
    width: '88%',
    tone: 'up',
    note: 'Improving after stabilization',
  },
  {
    label: 'Android rating',
    value: '<2.0 / 5',
    width: '35%',
    tone: 'critical',
    note: 'Workstream still in progress',
  },
]

const signals = [
  { label: 'Customer confidence', value: 'Lowered', tone: 'down' },
  { label: 'Stabilization', value: 'In progress', tone: 'neutral' },
  { label: 'iOS health', value: 'Improving', tone: 'up' },
  { label: 'Android health', value: 'Critical', tone: 'critical' },
]

const risks = [
  'High penalties tied to app quality issues',
  'Code quality concerns in key modules (Gym Check-ins)',
  'Android rating remains below 2',
  'Customer confidence impacted by build quality',
]

const actions = [
  'Dedicated FTE stabilization workstream in place',
  'War room support continued through February',
  'Review prompt strategy to lift positive feedback',
  'Categorize reviews into workflow vs bug issues',
]

const opportunities = [
  'US and Asia implementation opportunities',
  'Hospitality and airline GTM alignment',
  'KSA region support and local leadership',
  'Independent implementation partner role',
]

const timeline = [
  {
    date: 'Jan 2025',
    title: 'Joint go-live with shared QA signoffs',
    detail: 'Capillary + Zapcom delivery model',
  },
  {
    date: 'Feb 2025',
    title: 'Ownership fully moved in-house',
    detail: 'Zapcom support scaled down',
  },
  {
    date: 'Jun 22, 2025',
    title: 'Engagement flagged as down 80%',
    detail: 'Request for strategic partnership review',
  },
  {
    date: 'Jun 26, 2025',
    title: 'Capillary feedback on penalties and ratings',
    detail: 'iOS 4.4, Android below 2',
  },
  {
    date: 'Jul 3, 2025',
    title: 'Follow-up with remediation suggestions',
    detail: 'Review prompts and app insights',
  },
]

export default function Home() {
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
            <Link className="nav-link active" href="/">
              Capillary relationship
            </Link>
            <Link className="nav-link" href="/csat-response">
              CSAT response
            </Link>
          </div>
        </nav>

        <header className="hero">
          <div className="hero-text fade-in">
            <span className="eyebrow">CSP customer satisfaction</span>
            <h1>Capillary relationship health dashboard</h1>
            <p className="hero-subtitle">
              Snapshot of customer satisfaction and project health derived from
              the Capillary relationship email thread.
            </p>
            <div className="pill-row">
              <span className="pill">US + Asia opportunity</span>
              <span className="pill">Optum app focus</span>
              <span className="pill">Hospitality + airline GTM</span>
            </div>
          </div>
          <div className="card hero-card fade-in" style={{ animationDelay: '0.1s' }}>
            <div className="hero-card-header">Customer satisfaction snapshot</div>
            <div className="hero-metrics">
              <div>
                <div className="hero-metric-value">4.4</div>
                <div className="hero-metric-label">iOS rating</div>
              </div>
              <div>
                <div className="hero-metric-value">&lt;2.0</div>
                <div className="hero-metric-label">Android rating</div>
              </div>
              <div>
                <div className="hero-metric-value">-80%</div>
                <div className="hero-metric-label">Engagement</div>
              </div>
            </div>
            <p className="hero-note">
              Confidence has been lowered by quality issues, while stabilization
              efforts are underway.
            </p>
          </div>
        </header>

        <section className="kpi-grid">
          {kpis.map((kpi, index) => (
            <div
              key={kpi.label}
              className={`kpi-card tone-${kpi.tone} fade-in`}
              style={{ animationDelay: `${0.15 + index * 0.05}s` }}
            >
              <div className="kpi-label">{kpi.label}</div>
              <div className="kpi-value">{kpi.value}</div>
              <div className="kpi-detail">{kpi.detail}</div>
              <div className="kpi-note">{kpi.note}</div>
            </div>
          ))}
        </section>

        <section className="grid-two">
          <div className="card fade-in" style={{ animationDelay: '0.2s' }}>
            <div className="section-title">App store health</div>
            <div className="rating-list">
              {ratingBars.map((rating) => (
                <div key={rating.label} className="rating-item">
                  <div className="rating-header">
                    <span>{rating.label}</span>
                    <span className={`rating-value tone-${rating.tone}`}>
                      {rating.value}
                    </span>
                  </div>
                  <div className="rating-track">
                    <div
                      className={`rating-fill tone-${rating.tone}`}
                      style={{ width: rating.width }}
                    />
                  </div>
                  <div className="rating-note">{rating.note}</div>
                </div>
              ))}
            </div>
          </div>
          <div className="card fade-in" style={{ animationDelay: '0.25s' }}>
            <div className="section-title">Engagement momentum</div>
            <div className="sparkline">
              <svg viewBox="0 0 260 90" role="img" aria-label="Engagement trend">
                <defs>
                  <linearGradient id="sparkGradient" x1="0" y1="0" x2="1" y2="0">
                    <stop offset="0%" stopColor="#f4a259" />
                    <stop offset="100%" stopColor="#e56b6f" />
                  </linearGradient>
                </defs>
                <path
                  d="M10 20 L60 30 L110 55 L160 60 L210 72 L250 82"
                  fill="none"
                  stroke="url(#sparkGradient)"
                  strokeWidth="4"
                  strokeLinecap="round"
                />
                <circle cx="10" cy="20" r="4" fill="#f4a259" />
                <circle cx="250" cy="82" r="4" fill="#e56b6f" />
              </svg>
              <div className="sparkline-caption">
                Engagement dropped 80% from Jan to Jun, signaling urgent recovery
                work.
              </div>
            </div>
            <div className="signal-grid">
              {signals.map((signal) => (
                <div key={signal.label} className={`signal tone-${signal.tone}`}>
                  <span className="signal-label">{signal.label}</span>
                  <span className="signal-value">{signal.value}</span>
                </div>
              ))}
            </div>
          </div>
        </section>

        <section className="grid-three">
          <div className="card fade-in" style={{ animationDelay: '0.3s' }}>
            <div className="section-title">Top risks</div>
            <ul className="list">
              {risks.map((item) => (
                <li key={item}>{item}</li>
              ))}
            </ul>
          </div>
          <div className="card fade-in" style={{ animationDelay: '0.35s' }}>
            <div className="section-title">Actions in motion</div>
            <ul className="list">
              {actions.map((item) => (
                <li key={item}>{item}</li>
              ))}
            </ul>
          </div>
          <div className="card fade-in" style={{ animationDelay: '0.4s' }}>
            <div className="section-title">Opportunity signals</div>
            <ul className="list">
              {opportunities.map((item) => (
                <li key={item}>{item}</li>
              ))}
            </ul>
          </div>
        </section>

        <section className="card timeline fade-in" style={{ animationDelay: '0.45s' }}>
          <div className="section-title">Key moments</div>
          <div className="timeline-list">
            {timeline.map((event) => (
              <div key={event.date} className="timeline-item">
                <div className="timeline-date">{event.date}</div>
                <div>
                  <div className="timeline-title">{event.title}</div>
                  <div className="timeline-detail">{event.detail}</div>
                </div>
              </div>
            ))}
          </div>
        </section>

        <footer className="footer">
          Source: Capillary relationship email thread (June-July 2025)
        </footer>
      </div>
    </main>
  )
}
