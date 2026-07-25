import { useEffect, useRef, useState } from "react";
import UploadForm from "./components/UploadForm.jsx";
import JobProgress from "./components/JobProgress.jsx";
import ResultTable from "./components/ResultTable.jsx";
import { createJob, getJobStatus, cancelJob } from "./api";

const POLL_INTERVAL_MS = 1500;
const TERMINAL_STATES = ["SUCCESS", "FAILED", "CANCELLED"];

export default function App() {
  const [job, setJob] = useState(null);
  const [submitting, setSubmitting] = useState(false);
  const [submitError, setSubmitError] = useState(null);
  const pollRef = useRef(null);

  useEffect(() => {
    return () => clearInterval(pollRef.current);
  }, []);

  const startPolling = (jobId) => {
    clearInterval(pollRef.current);
    pollRef.current = setInterval(async () => {
      try {
        const updated = await getJobStatus(jobId);
        setJob(updated);
        if (TERMINAL_STATES.includes(updated.status)) {
          clearInterval(pollRef.current);
        }
      } catch (err) {
        clearInterval(pollRef.current);
        setSubmitError(err.message);
      }
    }, POLL_INTERVAL_MS);
  };

  const handleSubmit = async (formValues) => {
    setSubmitting(true);
    setSubmitError(null);
    setJob(null);
    try {
      const created = await createJob(formValues);
      setJob(created);
      startPolling(created.id);
    } catch (err) {
      setSubmitError(err.message);
    } finally {
      setSubmitting(false);
    }
  };

  const handleCancel = async (jobId) => {
    try {
      const updated = await cancelJob(jobId);
      setJob(updated);
      clearInterval(pollRef.current);
    } catch (err) {
      setSubmitError(err.message);
    }
  };

  return (
    <div className="app-shell">
      <header className="hero">
        <span className="diamond" />
        <h1>Pattern Engine</h1>
      </header>
      <p className="hero-sub">
        Upload a CSV or Excel file, describe what to find in plain English, and let a
        Celery + Spark pipeline replace it across every row &mdash; without blocking, no
        matter how large the file is.
      </p>

      {submitError && <div className="error-banner">{submitError}</div>}

      <UploadForm onSubmit={handleSubmit} submitting={submitting} />

      {job && <JobProgress job={job} onCancel={handleCancel} />}

      {job && job.status === "SUCCESS" && <ResultTable jobId={job.id} />}
    </div>
  );
}
