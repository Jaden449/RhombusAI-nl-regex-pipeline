const BASE_URL = import.meta.env.VITE_API_BASE_URL || "http://localhost:8000/api";

async function handle(response) {
  if (!response.ok) {
    let detail = response.statusText;
    try {
      const body = await response.json();
      detail = body.detail || JSON.stringify(body);
    } catch {
      /* no-op: response wasn't JSON */
    }
    throw new Error(detail);
  }
  return response.json();
}

export async function createJob({ file, nlPrompt, targetColumn, replacementValue }) {
  const formData = new FormData();
  formData.append("input_file", file);
  formData.append("nl_prompt", nlPrompt);
  formData.append("target_column", targetColumn);
  formData.append("replacement_value", replacementValue);

  const response = await fetch(`${BASE_URL}/jobs/`, {
    method: "POST",
    body: formData,
  });
  return handle(response);
}

export async function getJobStatus(jobId) {
  const response = await fetch(`${BASE_URL}/jobs/${jobId}/status/`);
  return handle(response);
}

export async function getJobResult(jobId, page = 1, pageSize = 25) {
  const response = await fetch(
    `${BASE_URL}/jobs/${jobId}/result/?page=${page}&page_size=${pageSize}`
  );
  return handle(response);
}

export async function cancelJob(jobId) {
  const response = await fetch(`${BASE_URL}/jobs/${jobId}/cancel/`, {
    method: "POST",
  });
  return handle(response);
}
