const LABELS = {
  QUEUED: "Queued",
  RUNNING: "Running",
  SUCCESS: "Complete",
  FAILED: "Failed",
  CANCELLED: "Cancelled",
};

export default function JobProgress({ job, onCancel }) {
  if (!job) return null;

  const statusClass = job.status.toLowerCase();
  const showPulse = job.status === "QUEUED" || job.status === "RUNNING";
  const cancellable = job.status === "QUEUED" || job.status === "RUNNING";

  return (
    <div className="panel">
      <div className="status-row">
        <h2 style={{ margin: 0 }}>
          <span className="diamond small" />
          Job {job.id.slice(0, 8)}
        </h2>
        <span className={`status-pill ${statusClass}`}>
          {showPulse && <span className="pulse-dot" />}
          {LABELS[job.status] || job.status}
        </span>
      </div>

      <div className="progress-track">
        <div className="progress-fill" style={{ width: `${job.progress}%` }} />
      </div>
      <div style={{ fontSize: 12, color: "var(--muted)" }}>{job.progress}% complete</div>

      {job.regex_pattern && (
        <div className="regex-line">
          <span className="label">pattern</span>
          {job.regex_pattern}
          <span style={{ color: "var(--muted)" }}> &nbsp;({job.regex_source})</span>
        </div>
      )}

      {job.status === "FAILED" && job.error_message && (
        <div className="error-banner" style={{ marginTop: 14 }}>
          {job.error_message}
        </div>
      )}

      {job.status === "SUCCESS" && (
        <div className="meta-row">
          <span>
            rows: <b>{job.row_count?.toLocaleString()}</b>
          </span>
          <span>
            matched: <b>{job.matched_count?.toLocaleString()}</b>
          </span>
        </div>
      )}

      {cancellable && (
        <div style={{ marginTop: 18 }}>
          <button className="ghost" onClick={() => onCancel(job.id)}>
            Cancel job
          </button>
        </div>
      )}
    </div>
  );
}
