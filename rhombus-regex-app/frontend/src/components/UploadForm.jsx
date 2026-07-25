import { useState } from "react";

export default function UploadForm({ onSubmit, submitting }) {
  const [file, setFile] = useState(null);
  const [dragActive, setDragActive] = useState(false);
  const [nlPrompt, setNlPrompt] = useState("Find email addresses");
  const [targetColumn, setTargetColumn] = useState("Email");
  const [replacementValue, setReplacementValue] = useState("REDACTED");

  const handleDrop = (e) => {
    e.preventDefault();
    setDragActive(false);
    if (e.dataTransfer.files?.[0]) setFile(e.dataTransfer.files[0]);
  };

  const handleSubmit = (e) => {
    e.preventDefault();
    if (!file) return;
    onSubmit({ file, nlPrompt, targetColumn, replacementValue });
  };

  return (
    <form className="panel" onSubmit={handleSubmit}>
      <h2>
        <span className="diamond small" />
        New job
      </h2>

      <div className="field">
        <label>Data file (.csv, .xlsx, .xls)</label>
        <div
          className={`dropzone ${dragActive ? "active" : ""}`}
          onDragOver={(e) => {
            e.preventDefault();
            setDragActive(true);
          }}
          onDragLeave={() => setDragActive(false)}
          onDrop={handleDrop}
          onClick={() => document.getElementById("file-input").click()}
        >
          <input
            id="file-input"
            type="file"
            accept=".csv,.xlsx,.xls"
            onChange={(e) => setFile(e.target.files?.[0] ?? null)}
          />
          {file ? (
            <div className="filename">{file.name}</div>
          ) : (
            <div>Drop a file here, or click to browse</div>
          )}
        </div>
      </div>

      <div className="field">
        <label>Describe the pattern in plain English</label>
        <textarea
          rows={2}
          value={nlPrompt}
          onChange={(e) => setNlPrompt(e.target.value)}
          placeholder="e.g. find phone numbers"
        />
        <div className="hint">
          An LLM converts this into a regex pattern (cached, so repeat requests are instant).
        </div>
      </div>

      <div className="field">
        <label>Target column</label>
        <input
          type="text"
          value={targetColumn}
          onChange={(e) => setTargetColumn(e.target.value)}
          placeholder="Column name exactly as it appears in the file"
        />
      </div>

      <div className="field">
        <label>Replace matches with</label>
        <input
          type="text"
          value={replacementValue}
          onChange={(e) => setReplacementValue(e.target.value)}
        />
      </div>

      <button className="primary" type="submit" disabled={!file || submitting}>
        {submitting ? "Submitting…" : "Run pattern replacement"}
      </button>
    </form>
  );
}
