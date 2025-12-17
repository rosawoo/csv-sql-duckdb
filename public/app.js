const statusEl = document.getElementById('status');
const uploadForm = document.getElementById('uploadForm');
const csvFile = document.getElementById('csvFile');
const uploadBtn = document.getElementById('uploadBtn');
const runBtn = document.getElementById('runBtn');
const queryEl = document.getElementById('query');
const noLimitEl = document.getElementById('noLimit');
const uploadMeta = document.getElementById('uploadMeta');
const errorEl = document.getElementById('error');
const tableWrap = document.getElementById('tableWrap');

function setStatus(msg) {
  statusEl.textContent = msg || '';
}

function showError(msg) {
  if (!msg) {
    errorEl.hidden = true;
    errorEl.textContent = '';
    return;
  }
  errorEl.hidden = false;
  errorEl.textContent = msg;
}

function setLoading(isLoading) {
  uploadBtn.disabled = isLoading;
  runBtn.disabled = isLoading;
  if (isLoading) {
    document.body.style.cursor = 'progress';
  } else {
    document.body.style.cursor = 'default';
  }
}

function renderTable(columns, rows) {
  if (!columns || columns.length === 0) {
    tableWrap.innerHTML = '<div style="padding:12px;color:rgba(255,255,255,0.7)">No rows returned.</div>';
    return;
  }

  const thead = `<thead><tr>${columns.map(c => `<th>${escapeHtml(String(c))}</th>`).join('')}</tr></thead>`;
  const tbody = `<tbody>${rows.map(r => `<tr>${r.map(v => `<td>${escapeHtml(formatValue(v))}</td>`).join('')}</tr>`).join('')}</tbody>`;
  tableWrap.innerHTML = `<table>${thead}${tbody}</table>`;
}

function escapeHtml(s) {
  return s
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#039;');
}

function formatValue(v) {
  if (v === null || v === undefined) return '';
  if (typeof v === 'object') return JSON.stringify(v);
  return String(v);
}

async function health() {
  try {
    const res = await fetch('/health');
    if (!res.ok) throw new Error('health not ok');
    const data = await res.json();
    setStatus(`Ready. DuckDB: ${data.duckdb}. Table loaded: ${data.hasTable ? 'yes' : 'no'}`);
  } catch {
    setStatus('Ready.');
  }
}

uploadForm.addEventListener('submit', async (e) => {
  e.preventDefault();
  showError(null);

  if (!csvFile.files || csvFile.files.length === 0) {
    showError('Pick a CSV file first.');
    return;
  }

  const file = csvFile.files[0];
  const form = new FormData();
  form.append('file', file);

  setLoading(true);
  setStatus('Uploading and importing CSV...');
  uploadMeta.textContent = '';
  tableWrap.innerHTML = '';

  try {
    const res = await fetch('/upload', {
      method: 'POST',
      body: form,
    });

    if (!res.ok) {
      const text = await res.text();
      throw new Error(text || 'Upload failed');
    }

    const data = await res.json();
    uploadMeta.textContent = `Imported ${data.rowCount ?? '?'} rows.`;
    setStatus('Upload complete. Ready for queries.');
  } catch (err) {
    showError(String(err?.message ?? err));
    setStatus('Upload failed.');
  } finally {
    setLoading(false);
    await health();
  }
});

runBtn.addEventListener('click', async () => {
  showError(null);
  tableWrap.innerHTML = '';

  const q = queryEl.value || '';
  if (!q.trim()) {
    showError('Enter a SQL query.');
    return;
  }

  setLoading(true);
  setStatus('Running query...');

  try {
    const res = await fetch('/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ query: q, noLimit: !!noLimitEl.checked }),
    });

    if (!res.ok) {
      const text = await res.text();
      throw new Error(text || 'Query failed');
    }

    const data = await res.json();
    renderTable(data.columns, data.rows);
    setStatus(`Query complete. Returned ${data.rows?.length ?? 0} row(s).`);
  } catch (err) {
    showError(String(err?.message ?? err));
    setStatus('Query failed.');
  } finally {
    setLoading(false);
  }
});

health();
