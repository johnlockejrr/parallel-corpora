let corpora = [];
let activeCorpus = null;
let rows = [];
let pendingSaveTimers = new Map(); // rowId -> timer
let deleteTargetRowId = null;
let deleteTargetCorpusId = null;
let sortBy = 'id';
let sortDir = 'asc';
let pageSize = 500;
let pageOffset = 0;
let hasMore = false;
let createMode = 'manual'; // 'manual' | 'import'
let importFile = null;
let importHeader = [];
let importDelimiter = '';

document.addEventListener('DOMContentLoaded', () => {
  setupEvents();
  loadCorpora();
  setCreateMode('manual');
});

function setupEvents() {
  const searchInput = document.getElementById('searchInput');
  searchInput.addEventListener('input', debounce(() => {
    if (!activeCorpus) return;
    pageOffset = 0;
    loadRows(activeCorpus.id, searchInput.value);
  }, 180));

  document.getElementById('newCorpusBtn').addEventListener('click', () => openModal('newCorpusModal'));
  document.getElementById('createCorpusBtn').addEventListener('click', createCorpusFromModal);
  document.getElementById('importCorpusBtn').addEventListener('click', importCorpusFromModal);
  document.getElementById('createModeManualBtn').addEventListener('click', () => setCreateMode('manual'));
  document.getElementById('createModeImportBtn').addEventListener('click', () => setCreateMode('import'));
  document.getElementById('importFile').addEventListener('change', onImportFileSelected);

  document.getElementById('newRowBtn').addEventListener('click', createNewRow);
  document.getElementById('exportBtn').addEventListener('click', exportActiveCorpus);
  document.getElementById('sortBySelect').addEventListener('change', (e) => {
    sortBy = e.target.value || 'id';
    pageOffset = 0;
    if (activeCorpus) loadRows(activeCorpus.id, document.getElementById('searchInput').value);
  });
  document.getElementById('sortDirBtn').addEventListener('click', () => {
    sortDir = sortDir === 'asc' ? 'desc' : 'asc';
    updateSortDirButton();
    pageOffset = 0;
    if (activeCorpus) loadRows(activeCorpus.id, document.getElementById('searchInput').value);
  });
  document.getElementById('pageSizeSelect').addEventListener('change', (e) => {
    pageSize = Number(e.target.value) || 500;
    pageOffset = 0;
    if (activeCorpus) loadRows(activeCorpus.id, document.getElementById('searchInput').value);
  });
  document.getElementById('prevPageBtn').addEventListener('click', () => {
    pageOffset = Math.max(0, pageOffset - pageSize);
    if (activeCorpus) loadRows(activeCorpus.id, document.getElementById('searchInput').value);
  });
  document.getElementById('nextPageBtn').addEventListener('click', () => {
    if (!hasMore) return;
    pageOffset = pageOffset + pageSize;
    if (activeCorpus) loadRows(activeCorpus.id, document.getElementById('searchInput').value);
  });

  document.querySelectorAll('[data-close="modal"]').forEach(el => {
    el.addEventListener('click', closeAllModals);
  });
  document.querySelectorAll('.close').forEach(el => el.addEventListener('click', closeAllModals));
  window.addEventListener('click', (e) => {
    if (e.target.classList.contains('modal')) closeAllModals();
  });

  document.getElementById('confirmDeleteBtn').addEventListener('click', confirmDeleteRow);
  document.getElementById('confirmDeleteCorpusBtn').addEventListener('click', confirmDeleteCorpus);
  updateSortDirButton();
  updatePagingControls();
}

function exportActiveCorpus() {
  if (!activeCorpus) return;
  const url = new URL(`/api/corpora/${activeCorpus.id}/export`, window.location.origin);
  url.searchParams.set('delimiter', '|');
  // Force download navigation
  window.location.href = url.toString();
}

function updatePagingControls() {
  const prevBtn = document.getElementById('prevPageBtn');
  const nextBtn = document.getElementById('nextPageBtn');
  const info = document.getElementById('pageInfo');
  if (!prevBtn || !nextBtn || !info) return;
  prevBtn.disabled = !activeCorpus || pageOffset <= 0;
  nextBtn.disabled = !activeCorpus || !hasMore;
  if (!activeCorpus) {
    info.textContent = '—';
    return;
  }
  const start = pageOffset + 1;
  const end = pageOffset + (rows ? rows.length : 0);
  info.textContent = end >= start ? `${start}-${end}${hasMore ? '+' : ''}` : '0';
}

function setCreateMode(mode) {
  createMode = mode;
  const manualBtn = document.getElementById('createModeManualBtn');
  const importBtn = document.getElementById('createModeImportBtn');
  const manualBlock = document.getElementById('manualCreateBlock');
  const importBlock = document.getElementById('importCreateBlock');
  const createBtn = document.getElementById('createCorpusBtn');
  const importCreateBtn = document.getElementById('importCorpusBtn');

  if (mode === 'import') {
    manualBtn.classList.remove('active');
    importBtn.classList.add('active');
    manualBlock.style.display = 'none';
    importBlock.style.display = 'block';
    createBtn.style.display = 'none';
    importCreateBtn.style.display = 'inline-block';
  } else {
    manualBtn.classList.add('active');
    importBtn.classList.remove('active');
    manualBlock.style.display = 'block';
    importBlock.style.display = 'none';
    createBtn.style.display = 'inline-block';
    importCreateBtn.style.display = 'none';
  }
}

async function onImportFileSelected(e) {
  importFile = e.target.files && e.target.files[0] ? e.target.files[0] : null;
  importHeader = [];
  importDelimiter = '';

  const previewEl = document.getElementById('importPreview');
  previewEl.style.display = 'none';

  if (!importFile) return;

  // Basic client-side size guard (server also enforces a limit).
  const maxBytes = 10 * 1024 * 1024;
  if (importFile.size > maxBytes) {
    alert('File is too large for browser import preview. Please use a smaller file or import via script/API.');
    return;
  }

  const form = new FormData();
  form.append('file', importFile);

  const res = await fetch('/api/corpora/import/preview', { method: 'POST', body: form });
  const data = await res.json();
  if (!data.success) {
    alert(data.error || 'Failed to preview file');
    return;
  }

  importHeader = data.header || [];
  importDelimiter = data.delimiter || '';

  document.getElementById('importDelimiter').textContent = importDelimiter === '\t' ? '\\t' : (importDelimiter || '—');
  document.getElementById('importRowCount').textContent = data.row_count_capped ? `${data.row_count}+` : String(data.row_count);

  // Key-field select
  const keySel = document.getElementById('importKeyField');
  keySel.innerHTML = '';
  const optNone = document.createElement('option');
  optNone.value = '';
  optNone.textContent = '(no key field)';
  keySel.appendChild(optNone);
  importHeader.forEach(h => {
    const opt = document.createElement('option');
    opt.value = h;
    opt.textContent = h;
    keySel.appendChild(opt);
  });
  if (importHeader.includes('key')) keySel.value = 'key';

  // Columns checklist
  const colsWrap = document.getElementById('importColumnsList');
  colsWrap.innerHTML = '';
  importHeader.forEach(h => {
    const item = document.createElement('label');
    item.className = 'import-col-item';
    const cb = document.createElement('input');
    cb.type = 'checkbox';
    cb.value = h;
    cb.checked = h !== keySel.value;
    const span = document.createElement('span');
    span.textContent = h;
    item.appendChild(cb);
    item.appendChild(span);
    colsWrap.appendChild(item);
  });

  keySel.addEventListener('change', () => {
    const keyField = keySel.value;
    colsWrap.querySelectorAll('input[type="checkbox"]').forEach(cb => {
      if (cb.value === keyField && keyField) cb.checked = false;
    });
  });

  previewEl.style.display = 'block';
}

function updateSortDirButton() {
  const btn = document.getElementById('sortDirBtn');
  if (!btn) return;
  btn.textContent = sortDir === 'asc' ? '↑ Asc' : '↓ Desc';
}

async function loadCorpora() {
  const res = await fetch('/api/corpora');
  const data = await res.json();
  if (!data.success) return;
  corpora = data.corpora || [];
  renderCorpora();

  if (!activeCorpus && corpora.length > 0) {
    selectCorpus(corpora[0].id);
  }
}

function renderCorpora() {
  const list = document.getElementById('corporaList');
  if (!corpora.length) {
    list.innerHTML = '<div class="loading" style="padding:1.5rem 1rem;">No corpora yet.</div>';
    return;
  }

  list.innerHTML = corpora.map(c => {
    const cols = (c.columns || []).join(', ');
    const isActive = activeCorpus && activeCorpus.id === c.id;
    return `
      <div class="corpus-item ${isActive ? 'active' : ''}" onclick="selectCorpus(${c.id})">
        <div class="corpus-name">
          ${escapeHtml(c.name)}
          <button class="btn btn-danger corpus-del-btn" data-corpus-del-id="${c.id}" title="Delete corpus">Delete</button>
        </div>
        <div class="corpus-meta">${escapeHtml(cols || '—')}</div>
      </div>
    `;
  }).join('');

  list.querySelectorAll('[data-corpus-del-id]').forEach(btn => {
    btn.addEventListener('click', (e) => {
      e.preventDefault();
      e.stopPropagation();
      requestDeleteCorpus(Number(btn.getAttribute('data-corpus-del-id')));
    });
  });
}

async function selectCorpus(corpusId) {
  const res = await fetch(`/api/corpora/${corpusId}`);
  const data = await res.json();
  if (!data.success) return;

  activeCorpus = data.corpus;
  document.getElementById('activeCorpusName').textContent = activeCorpus.name || '—';
  document.getElementById('newRowBtn').disabled = false;

  renderCorpora();

  document.getElementById('emptyState').style.display = 'none';
  document.getElementById('gridTable').style.display = 'table';

  pageOffset = 0;
  hasMore = false;
  await loadRows(activeCorpus.id, document.getElementById('searchInput').value);
}

async function loadRows(corpusId, q) {
  const url = new URL(`/api/corpora/${corpusId}/rows`, window.location.origin);
  if (q && q.trim()) url.searchParams.set('q', q.trim());
  url.searchParams.set('limit', String(pageSize));
  url.searchParams.set('offset', String(pageOffset));
  url.searchParams.set('sort_by', sortBy);
  url.searchParams.set('sort_dir', sortDir);

  const res = await fetch(url.toString());
  const data = await res.json();
  if (!data.success) return;

  rows = data.rows || [];
  hasMore = !!data.has_more;
  // This is count for the current page.
  document.getElementById('rowCount').textContent = String(rows.length);
  renderGrid();
  updatePagingControls();
}

function renderGrid() {
  if (!activeCorpus) return;
  const cols = (activeCorpus.columns || []).map(String);

  const head = document.getElementById('gridHead');
  head.innerHTML = `
    <tr>
      <th style="width: 90px;">ID</th>
      <th style="width: 180px;">Key</th>
      ${cols.map(c => `<th>${escapeHtml(c)}</th>`).join('')}
      <th style="width: 110px; text-align:right;">Actions</th>
    </tr>
  `;

  const body = document.getElementById('gridBody');
  if (!rows.length) {
    body.innerHTML = `<tr><td colspan="${2 + cols.length}" class="loading">No rows match your search.</td></tr>`;
    return;
  }

  body.innerHTML = rows.map(r => {
    const cells = r.cells || {};
    return `
      <tr data-row-id="${r.id}">
        <td><div class="cell mono" contenteditable="false" spellcheck="false">${escapeHtml(String(r.id))}</div></td>
        <td><div class="cell mono" contenteditable="true" spellcheck="false" data-field="key">${escapeHtml(r.key || '')}</div></td>
        ${cols.map(c => `
          <td><div class="cell" contenteditable="true" spellcheck="false" data-col="${escapeHtmlAttr(c)}">${escapeHtml((cells[c] ?? '').toString())}</div></td>
        `).join('')}
        <td>
          <div class="row-actions">
            <button class="btn btn-danger" onclick="requestDeleteRow(${r.id})">Delete</button>
          </div>
        </td>
      </tr>
    `;
  }).join('');

  // Bind edit handlers after render.
  body.querySelectorAll('.cell[contenteditable="true"]').forEach(el => {
    el.addEventListener('input', (e) => scheduleSaveForCell(e.target));
    el.addEventListener('blur', (e) => flushSaveForCell(e.target));
    // Ensure copy/cut grabs the real underlying text (not layout/wrapping).
    el.addEventListener('copy', (e) => forcePlainTextClipboard(e, e.target));
    el.addEventListener('cut', (e) => forcePlainTextClipboard(e, e.target));
    el.addEventListener('keydown', (e) => {
      if (e.key === 'Enter' && !e.shiftKey) {
        e.preventDefault();
        e.target.blur();
      }
    });
  });
}

function scheduleSaveForCell(cellEl) {
  const tr = cellEl.closest('tr');
  if (!tr) return;
  const rowId = Number(tr.getAttribute('data-row-id'));
  if (!rowId) return;

  if (pendingSaveTimers.has(rowId)) clearTimeout(pendingSaveTimers.get(rowId));
  pendingSaveTimers.set(rowId, setTimeout(() => saveRowEdits(rowId), 450));
}

function flushSaveForCell(cellEl) {
  const tr = cellEl.closest('tr');
  if (!tr) return;
  const rowId = Number(tr.getAttribute('data-row-id'));
  if (!rowId) return;

  if (pendingSaveTimers.has(rowId)) {
    clearTimeout(pendingSaveTimers.get(rowId));
    pendingSaveTimers.delete(rowId);
  }
  saveRowEdits(rowId);
}

async function saveRowEdits(rowId) {
  if (!activeCorpus) return;
  const tr = document.querySelector(`tr[data-row-id="${rowId}"]`);
  if (!tr) return;

  const keyEl = tr.querySelector('[data-field="key"]');
  const cellsEls = tr.querySelectorAll('[data-col]');

  const payload = {
    key: keyEl ? keyEl.textContent.trim() : '',
    cells: {},
  };

  cellsEls.forEach(el => {
    const col = el.getAttribute('data-col');
    // Preserve exactly what user typed/pasted (including leading/trailing whitespace and newlines).
    payload.cells[col] = (el.textContent ?? '');
  });

  const res = await fetch(`/api/corpora/${activeCorpus.id}/rows/${rowId}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  const data = await res.json();
  if (!data.success) {
    console.error('Save failed', data.error);
    return;
  }

  // Update local cache row to keep render consistent if needed.
  const idx = rows.findIndex(r => r.id === rowId);
  if (idx >= 0) rows[idx] = data.row;
}

async function createCorpusFromModal() {
  const name = document.getElementById('newCorpusName').value.trim();
  const colsStr = document.getElementById('newCorpusColumns').value.trim();
  const description = document.getElementById('newCorpusDesc').value.trim();
  const columns = colsStr ? colsStr.split(',').map(s => s.trim()).filter(Boolean) : [];

  if (!name) { alert('Corpus name is required'); return; }
  if (!columns.length) { alert('At least one column key is required'); return; }

  const res = await fetch('/api/corpora', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ name, columns, description }),
  });
  const data = await res.json();
  if (!data.success) {
    alert(data.error || 'Failed to create corpus');
    return;
  }
  closeAllModals();
  document.getElementById('newCorpusName').value = '';
  document.getElementById('newCorpusColumns').value = '';
  document.getElementById('newCorpusDesc').value = '';

  await loadCorpora();
  await selectCorpus(data.corpus.id);
}

async function importCorpusFromModal() {
  const name = document.getElementById('newCorpusName').value.trim();
  const description = document.getElementById('newCorpusDesc').value.trim();
  if (!importFile) { alert('Please choose a file'); return; }

  const keyField = document.getElementById('importKeyField').value || '';
  const selectedCols = Array.from(document.querySelectorAll('#importColumnsList input[type="checkbox"]:checked')).map(cb => cb.value);
  if (!selectedCols.length) { alert('Please select at least one column to import'); return; }

  const form = new FormData();
  form.append('file', importFile);
  if (name) form.append('name', name);
  if (description) form.append('description', description);
  if (keyField) form.append('key_field', keyField);
  form.append('columns', selectedCols.join(','));

  const btn = document.getElementById('importCorpusBtn');
  const oldText = btn.textContent;
  btn.textContent = 'Importing…';
  btn.disabled = true;
  try {
    const res = await fetch('/api/corpora/import', { method: 'POST', body: form });
    const data = await res.json();
    if (!data.success) {
      alert(data.error || 'Import failed');
      return;
    }
    closeAllModals();
    // reset import UI
    document.getElementById('importFile').value = '';
    document.getElementById('importPreview').style.display = 'none';
    importFile = null;
    importHeader = [];
    importDelimiter = '';

    await loadCorpora();
    await selectCorpus(data.corpus.id);
  } finally {
    btn.textContent = oldText;
    btn.disabled = false;
  }
}

async function createNewRow() {
  if (!activeCorpus) return;
  const cells = {};
  (activeCorpus.columns || []).forEach(c => cells[c] = '');

  const res = await fetch(`/api/corpora/${activeCorpus.id}/rows`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ key: '', cells }),
  });
  const data = await res.json();
  if (!data.success) {
    alert(data.error || 'Failed to create row');
    return;
  }

  // Bring user to first page after creating.
  pageOffset = 0;
  await loadRows(activeCorpus.id, document.getElementById('searchInput').value);

  // Focus first language cell (key can be set later).
  const tr = document.querySelector(`tr[data-row-id="${data.row.id}"]`);
  const firstCell = tr ? tr.querySelector('.cell[data-col]') : null;
  if (firstCell) firstCell.focus();
}

function requestDeleteRow(rowId) {
  deleteTargetRowId = rowId;
  const row = rows.find(r => r.id === rowId);
  document.getElementById('deleteTarget').textContent = row ? `${row.key}` : `Row ${rowId}`;
  openModal('deleteModal');
}

async function confirmDeleteRow() {
  if (!deleteTargetRowId) return;
  if (!activeCorpus) return;
  const res = await fetch(`/api/corpora/${activeCorpus.id}/rows/${deleteTargetRowId}`, { method: 'DELETE' });
  const data = await res.json();
  if (!data.success) {
    alert(data.error || 'Delete failed');
    return;
  }
  closeAllModals();
  deleteTargetRowId = null;
  if (activeCorpus) await loadRows(activeCorpus.id, document.getElementById('searchInput').value);
}

function requestDeleteCorpus(corpusId) {
  deleteTargetCorpusId = corpusId;
  const c = corpora.find(x => x.id === corpusId);
  document.getElementById('deleteCorpusTarget').textContent = c ? c.name : `Corpus ${corpusId}`;
  openModal('deleteCorpusModal');
}

async function confirmDeleteCorpus() {
  if (!deleteTargetCorpusId) return;
  const res = await fetch(`/api/corpora/${deleteTargetCorpusId}`, { method: 'DELETE' });
  const data = await res.json();
  if (!data.success) {
    alert(data.error || 'Delete failed');
    return;
  }
  closeAllModals();
  const deletedId = deleteTargetCorpusId;
  deleteTargetCorpusId = null;

  if (activeCorpus && activeCorpus.id === deletedId) {
    activeCorpus = null;
    rows = [];
    document.getElementById('activeCorpusName').textContent = '—';
    document.getElementById('rowCount').textContent = '—';
    document.getElementById('newRowBtn').disabled = true;
    document.getElementById('gridTable').style.display = 'none';
    document.getElementById('emptyState').style.display = 'block';
    document.getElementById('emptyState').textContent = 'Select (or create) a corpus to begin.';
  }

  await loadCorpora();
}

function openModal(id) {
  const m = document.getElementById(id);
  if (m) m.style.display = 'block';
}

function closeAllModals() {
  document.querySelectorAll('.modal').forEach(m => (m.style.display = 'none'));
}

function escapeHtml(text) {
  const div = document.createElement('div');
  div.textContent = String(text ?? '');
  return div.innerHTML;
}

function escapeHtmlAttr(text) {
  return String(text ?? '').replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

function forcePlainTextClipboard(e, el) {
  if (!e || !e.clipboardData || !el) return;
  const text = (el.textContent ?? '');
  e.clipboardData.setData('text/plain', text);
  e.preventDefault();
}

function debounce(fn, ms) {
  let t = null;
  return (...args) => {
    if (t) clearTimeout(t);
    t = setTimeout(() => fn(...args), ms);
  };
}

