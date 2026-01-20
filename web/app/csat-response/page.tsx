import Link from 'next/link'

const csatKpis = [
  {
    label: 'Overall satisfaction',
    value: '1 / 5',
    detail: 'Very poor',
    tone: 'critical',
    note: 'CSAT response',
  },
  {
    label: 'Recommend likelihood',
    value: '1 / 10',
    detail: 'Not at all likely',
    tone: 'critical',
    note: 'Promoter score signal',
  },
  {
    label: 'Would choose again',
    value: 'No',
    detail: 'Client response',
    tone: 'critical',
    note: 'Retention risk',
  },
  {
    label: 'Feedback delay',
    value: '~4 months',
    detail: 'Mar to Jul',
    tone: 'down',
    note: 'Escalation gap',
  },
  {
    label: 'Respondent mix',
    value: '3 leaders',
    detail: '1 negative',
    tone: 'neutral',
    note: 'Mixed sentiment',
  },
  {
    label: 'Recovery plan',
    value: '5 steps',
    detail: 'SLT-led',
    tone: 'up',
    note: 'Damage control path',
  },
]

const scoreBars = [
  {
    label: 'Overall satisfaction',
    value: '1 / 5',
    width: '20%',
    tone: 'critical',
    note: 'Very poor CSAT score',
  },
  {
    label: 'Recommend likelihood',
    value: '1 / 10',
    width: '10%',
    tone: 'critical',
    note: 'Not at all likely to recommend',
  },
]

const sentiment = [
  {
    label: 'Negative',
    value: '1',
    tone: 'critical',
    note: 'Nate Steffan',
  },
  {
    label: 'Positive',
    value: '1',
    tone: 'up',
    note: 'Prateek Yadav',
  },
  {
    label: 'Extremely positive',
    value: '1',
    tone: 'up',
    note: 'Bill Swift',
  },
]

const strengths = ['Responsiveness to requests', 'Engagement']

const criticalConcerns = [
  'Quality of services and delivery (automation/testing)',
  'Team competency and skill gaps',
  'Collaboration, communication, problem-solving',
  'Responsiveness and on-time delivery',
  'Solutioning methodology and resolution speed',
  'Resource capability and talent acquisition',
  'Project management and velocity',
  'Adaptability to changing needs',
]

const projectIssues = [
  'Resource competency concerns raised in July went unaddressed',
  'Front-end tech lead had severe communication and quality issues',
  'AI integration commitments had no visible follow-through',
  'Capillary leadership ran a multi-week war room',
  'Rushed build led to rework and quality issues',
]

const actionPlan = [
  'Call Nate for a vibe check using verified contact numbers',
  'Kishore email acknowledging issues and inviting a short call',
  'Prasanth note with improvements and a bounded pilot or PoC',
  'SLT-led call to align on pain points and corrective measures',
  'Fortnightly cadence with Nate independent of projects',
]

const timeline = [
  {
    date: 'Mar 19, 2025',
    title: 'Critical CSAT response submitted',
    detail: 'Overall satisfaction 1/5, recommendation 1/10',
  },
  {
    date: 'Jul 10, 2025',
    title: 'Internal escalation on missed sharing',
    detail: 'SLT visibility gap discovered',
  },
  {
    date: 'Jul 24, 2025',
    title: 'Action plan proposed by CSP',
    detail: 'Call, email, pilot, SLT engagement',
  },
  {
    date: 'Jul 24, 2025',
    title: 'CSAT summary shared',
    detail: '1 negative, 1 positive, 1 extremely positive',
  },
]

export default function CsatResponsePage() {
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
            <Link className="nav-link active" href="/csat-response">
              CSAT response
            </Link>
            <Link className="nav-link" href="/query">
              Ask emails
            </Link>
          </div>
        </nav>

        <header className="hero">
          <div className="hero-text fade-in">
            <span className="eyebrow">Critical client feedback</span>
            <h1>CSAT response health dashboard</h1>
            <p className="hero-subtitle">
              Visibility into CSAT severity, sentiment mix, and recovery steps
              from the Nate Steffan feedback thread.
            </p>
            <div className="pill-row">
              <span className="pill">Overall satisfaction 1/5</span>
              <span className="pill">Recommendation 1/10</span>
              <span className="pill">SLT recovery plan</span>
            </div>
          </div>
          <div className="card hero-card fade-in" style={{ animationDelay: '0.1s' }}>
            <div className="hero-card-header">Client health snapshot</div>
            <div className="hero-metrics">
              <div>
                <div className="hero-metric-value">1/5</div>
                <div className="hero-metric-label">Satisfaction</div>
              </div>
              <div>
                <div className="hero-metric-value">1/10</div>
                <div className="hero-metric-label">Recommend</div>
              </div>
              <div>
                <div className="hero-metric-value">No</div>
                <div className="hero-metric-label">Choose again</div>
              </div>
            </div>
            <p className="hero-note">
              Critical CSAT signals and delayed escalation require immediate
              leadership intervention.
            </p>
          </div>
        </header>

        <section className="kpi-grid">
          {csatKpis.map((kpi, index) => (
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
            <div className="section-title">CSAT scores</div>
            <div className="rating-list">
              {scoreBars.map((score) => (
                <div key={score.label} className="rating-item">
                  <div className="rating-header">
                    <span>{score.label}</span>
                    <span className={`rating-value tone-${score.tone}`}>
                      {score.value}
                    </span>
                  </div>
                  <div className="rating-track">
                    <div
                      className={`rating-fill tone-${score.tone}`}
                      style={{ width: score.width }}
                    />
                  </div>
                  <div className="rating-note">{score.note}</div>
                </div>
              ))}
            </div>
          </div>
          <div className="card fade-in" style={{ animationDelay: '0.25s' }}>
            <div className="section-title">Sentiment mix</div>
            <div className="sentiment-grid">
              {sentiment.map((item) => (
                <div key={item.label} className={`sentiment-card tone-${item.tone}`}>
                  <div className="sentiment-label">{item.label}</div>
                  <div className="sentiment-value">{item.value}</div>
                  <div className="sentiment-note">{item.note}</div>
                </div>
              ))}
            </div>
            <div className="sparkline-caption">
              CSAT sentiment is mixed but anchored by a highly negative response
              from Nate Steffan.
            </div>
          </div>
        </section>

        <section className="grid-three">
          <div className="card fade-in" style={{ animationDelay: '0.3s' }}>
            <div className="section-title">What went well</div>
            <ul className="list">
              {strengths.map((item) => (
                <li key={item}>{item}</li>
              ))}
            </ul>
          </div>
          <div className="card fade-in" style={{ animationDelay: '0.35s' }}>
            <div className="section-title">Critical concerns</div>
            <ul className="list">
              {criticalConcerns.map((item) => (
                <li key={item}>{item}</li>
              ))}
            </ul>
          </div>
          <div className="card fade-in" style={{ animationDelay: '0.4s' }}>
            <div className="section-title">Optum delivery issues</div>
            <ul className="list">
              {projectIssues.map((item) => (
                <li key={item}>{item}</li>
              ))}
            </ul>
          </div>
        </section>

        <section className="grid-two">
          <div className="card fade-in" style={{ animationDelay: '0.45s' }}>
            <div className="section-title">Recovery actions</div>
            <ul className="list">
              {actionPlan.map((item) => (
                <li key={item}>{item}</li>
              ))}
            </ul>
          </div>
          <div className="card fade-in" style={{ animationDelay: '0.5s' }}>
            <div className="section-title">Leadership signals</div>
            <div className="signal-grid">
              <div className="signal tone-critical">
                <span className="signal-label">Trust impact</span>
                <span className="signal-value">Severe</span>
              </div>
              <div className="signal tone-down">
                <span className="signal-label">Response timing</span>
                <span className="signal-value">Delayed</span>
              </div>
              <div className="signal tone-neutral">
                <span className="signal-label">SLT involvement</span>
                <span className="signal-value">Required</span>
              </div>
              <div className="signal tone-up">
                <span className="signal-label">Recovery path</span>
                <span className="signal-value">Defined</span>
              </div>
            </div>
          </div>
        </section>

        <section className="card timeline fade-in" style={{ animationDelay: '0.55s' }}>
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
          Source: CSAT response thread for Nate Steffan (March-July 2025)
        </footer>
      </div>
    </main>
  )
}
