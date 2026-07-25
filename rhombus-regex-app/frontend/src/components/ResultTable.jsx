import { useEffect, useState } from "react";
import { getJobResult } from "../api";

const PAGE_SIZE = 25;

export default function ResultTable({ jobId }) {
  const [page, setPage] = useState(1);
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);

  useEffect(() => {
    let cancelled = false;
    getJobResult(jobId, page, PAGE_SIZE)
      .then((res) => {
        if (!cancelled) setData(res);
      })
      .catch((err) => {
        if (!cancelled) setError(err.message);
      });
    return () => {
      cancelled = true;
    };
  }, [jobId, page]);

  if (error) return <div className="error-banner">{error}</div>;
  if (!data) return <div className="empty-state">Loading results…</div>;
  if (data.rows.length === 0) return <div className="empty-state">No rows to show.</div>;

  return (
    <div className="panel">
      <h2>
        <span className="diamond small" />
        Processed data
      </h2>
      <table className="results">
        <thead>
          <tr>
            {data.columns.map((col) => (
              <th key={col}>{col}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {data.rows.map((row, i) => (
            <tr key={i}>
              {data.columns.map((col) => (
                <td key={col}>{String(row[col] ?? "")}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>

      <div className="pagination">
        <button disabled={page <= 1} onClick={() => setPage((p) => p - 1)}>
          ← Prev
        </button>
        <span>
          Page {data.page} of {data.total_pages || 1} &middot; {data.row_count?.toLocaleString()}{" "}
          total rows
        </span>
        <button disabled={page >= data.total_pages} onClick={() => setPage((p) => p + 1)}>
          Next →
        </button>
      </div>
    </div>
  );
}
