const statusEl = document.getElementById('status');
const uploadForm = document.getElementById('uploadForm');
const csvFile = document.getElementById('csvFile');
const uploadBtn = document.getElementById('uploadBtn');
const runBtn = document.getElementById('runBtn');
const queryEl = document.getElementById('query');
const pageSizeEl = document.getElementById('pageSize');
const prevBtn = document.getElementById('prevBtn');
const nextBtn = document.getElementById('nextBtn');
const pageInfoEl = document.getElementById('pageInfo');
const uploadMeta = document.getElementById('uploadMeta');
const errorEl = document.getElementById('error');
const resultsMetaEl = document.getElementById('resultsMeta');
const tableWrap = document.getElementById('tableWrap');

let currentQuery = '';
let currentPage = 1;

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

function setResultsMeta(msg) {
  if (!resultsMetaEl) return;
  if (!msg) {
    resultsMetaEl.hidden = true;
    resultsMetaEl.textContent = '';
    return;
  }
  resultsMetaEl.hidden = false;
  resultsMetaEl.textContent = msg;
}

function setLoading(isLoading) {
  uploadBtn.disabled = isLoading;
  runBtn.disabled = isLoading;
  if (prevBtn) prevBtn.disabled = isLoading || currentPage <= 1;
  if (nextBtn) nextBtn.disabled = isLoading;
  if (isLoading) {
    document.body.style.cursor = 'progress';
  } else {
    document.body.style.cursor = 'default';
  }
}

function setPagerState({ page, hasNext }) {
  currentPage = page;
  if (pageInfoEl) pageInfoEl.textContent = `Page ${page}`;
  if (prevBtn) prevBtn.disabled = page <= 1;
  if (nextBtn) nextBtn.disabled = !hasNext;
}

function getPageSize() {
  const raw = pageSizeEl ? Number(pageSizeEl.value) : 100;
  if (!Number.isFinite(raw) || raw <= 0) return 100;
  return raw;
}

async function runQueryPage(page) {
  showError(null);
  tableWrap.innerHTML = '';
  setResultsMeta(null);

  if (!currentQuery.trim()) {
    showError('Enter a SQL query.');
    return;
  }

  setLoading(true);
  setStatus('Running query...');

  try {
    const res = await fetch('/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ query: currentQuery, page, pageSize: getPageSize() }),
    });

    if (!res.ok) {
      const text = await res.text();
      throw new Error(text || 'Query failed');
    }

    const data = await res.json();
    renderTable(data.columns, data.rows);
    const rowCount = data.rows?.length ?? 0;
    const colCount = data.columns?.length ?? 0;
    setPagerState({ page: data.page ?? page, hasNext: !!data.hasNext });
    setResultsMeta(`Columns: ${colCount} · Rows: ${rowCount} · Page size: ${data.pageSize ?? getPageSize()}`);
    setStatus(`Query complete. Returned ${rowCount} row(s).`);
  } catch (err) {
    showError(String(err?.message ?? err));
    setStatus('Query failed.');
  } finally {
    setLoading(false);
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
  const s = String(v);
  if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/.test(s)) {
    return s.slice(0, 10);
  }
  return s;
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
  setResultsMeta(null);
  setPagerState({ page: 1, hasNext: false });

  if (!csvFile.files || csvFile.files.length === 0) {
    showError('Pick a CSV file first.');
    return;
  }

  const file = csvFile.files[0];

  setLoading(true);
  setStatus('Preparing upload...');
  uploadMeta.textContent = '';
  tableWrap.innerHTML = '';
  setResultsMeta(null);

  try {
    // Prefer direct-to-object-storage uploads (S3/R2) if configured.
    let presignRes = await fetch('/upload/presign', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ filename: file.name, contentType: file.type || 'text/csv' }),
    });

    if (presignRes.status === 501) {
      // Fallback: upload through app server.
      const form = new FormData();
      form.append('file', file);
      setStatus('Uploading and importing CSV...');
      const res = await fetch('/upload', { method: 'POST', body: form });
      if (!res.ok) {
        const text = await res.text();
        throw new Error(text || 'Upload failed');
      }
      const data = await res.json();
      uploadMeta.textContent = `Imported ${data.rowCount ?? '?'} rows.`;
      setStatus('Upload complete. Ready for queries.');
      return;
    }

    if (!presignRes.ok) {
      const text = await presignRes.text();
      throw new Error(text || 'Failed to prepare upload');
    }

    const presign = await presignRes.json();
    if (!presign.uploadUrl || !presign.key) {
      throw new Error('Invalid presign response');
    }

    setStatus('Uploading to object storage...');
    const putRes = await fetch(presign.uploadUrl, {
      method: 'PUT',
      headers: { 'Content-Type': file.type || 'text/csv' },
      body: file,
    });

    if (!putRes.ok) {
      const text = await putRes.text();
      throw new Error(text || `Storage upload failed (${putRes.status})`);
    }

    setStatus('Importing from object storage...');
    const importRes = await fetch('/upload/import', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ key: presign.key }),
    });

    if (!importRes.ok) {
      const text = await importRes.text();
      throw new Error(text || 'Import failed');
    }

    const data = await importRes.json();
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
  currentQuery = queryEl.value || '';
  currentPage = 1;
  await runQueryPage(currentPage);
});

if (prevBtn) {
  prevBtn.addEventListener('click', async () => {
    if (currentPage <= 1) return;
    await runQueryPage(currentPage - 1);
  });
}

if (nextBtn) {
  nextBtn.addEventListener('click', async () => {
    await runQueryPage(currentPage + 1);
  });
}

if (pageSizeEl) {
  pageSizeEl.addEventListener('change', async () => {
    currentPage = 1;
    if (currentQuery.trim()) {
      await runQueryPage(currentPage);
    } else {
      setPagerState({ page: 1, hasNext: false });
    }
  });
}

health();
