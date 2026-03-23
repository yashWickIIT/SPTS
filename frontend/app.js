function _applyUserTag(username, role) {
  const tag = document.getElementById('userTag');
  const nameEl = document.getElementById('usernameLabel');
  const badgeEl = document.getElementById('roleBadge');
  const avatarEl = document.getElementById('userAvatar');
  if (!tag || !nameEl || !badgeEl) return;
  const safeRole = (role || 'analyst').toLowerCase();
  const safeName = username || '';
  if (nameEl) nameEl.textContent = safeName;
  if (avatarEl) avatarEl.textContent = safeName.charAt(0) || '?';
  if (badgeEl) { badgeEl.textContent = safeRole; badgeEl.className = `role-pill badge-role-${safeRole}`; }
  tag.style.display = 'flex';
}

const GOOGLE_FORM_URL = 'https://forms.gle/h1EBnwWBxag1znNX8';
let isGoogleFormInitialized = false;

function _syncRunButtonState() {
  const runBtn = document.getElementById('runBtn');
  const queryInput = document.getElementById('query');
  if (!runBtn || !queryInput) return;
  runBtn.disabled = !queryInput.value.trim();
}

function _initGoogleForm() {
  const frame = document.getElementById('googleFormFrame');
  const hint = document.getElementById('googleFormHint');
  const openLink = document.getElementById('googleFormOpenLink');
  if (!frame || !hint || !openLink) return;

  if (!GOOGLE_FORM_URL?.startsWith('https://forms.gle/')) {
    hint.textContent = 'Google Form is not configured yet.';
    hint.style.color = 'var(--danger)';
    frame.style.display = 'none';
    openLink.style.display = 'none';
    return;
  }

  openLink.href = GOOGLE_FORM_URL;

  frame.style.display = 'none';
  openLink.style.display = 'inline-flex';
}

document.addEventListener('DOMContentLoaded', async () => {
  const token = localStorage.getItem('spts_token');
  if (!token) {
      globalThis.location.href = '/static/login.html';
      return;
  }

  const role = localStorage.getItem('spts_role');
  const username = localStorage.getItem('spts_username');
  const adminBtn = document.getElementById('adminBtn');
  if (adminBtn && role === 'admin') {
    adminBtn.style.display = 'inline-block';
  }
  // Show tag immediately from localStorage while /me loads
  if (username || role) _applyUserTag(username, role);

  if (!isGoogleFormInitialized) {
    _initGoogleForm();
    isGoogleFormInitialized = true;
  }

  const queryInput = document.getElementById('query');
  if (queryInput) {
    queryInput.addEventListener('input', _syncRunButtonState);
  }
  _syncRunButtonState();

  try {
    const response = await fetch('/me', {
      headers: { Authorization: `Bearer ${token}` },
    });

    if (response.status === 401) {
      logout();
      return;
    }

    if (response.ok) {
      const me = await response.json();
      const freshRole = me.role || role || 'analyst';
      const freshUsername = me.username || username || '';
      localStorage.setItem('spts_role', freshRole);
      localStorage.setItem('spts_username', freshUsername);
      if (adminBtn && freshRole.toLowerCase() === 'admin') {
        adminBtn.style.display = 'inline-block';
      }
      _applyUserTag(freshUsername, freshRole);
    }
  } catch (error) {
    console.warn('Failed to refresh user profile from /me:', error);
  }
});

function logout() {
  localStorage.removeItem('spts_token');
  localStorage.removeItem('spts_role');
  localStorage.removeItem('spts_username');
  globalThis.location.href = '/static/login.html';
}

function goToAdmin() {
  globalThis.location.href = '/static/admin.html';
}

async function downloadSessionLog() {
  const token = localStorage.getItem('spts_token');
  if (!token) {
    logout();
    return;
  }

  const btn = document.getElementById('downloadSessionBtn');
  const originalText = btn ? btn.textContent : null;
  if (btn) {
    btn.disabled = true;
    btn.textContent = 'Preparing...';
  }

  try {
    const response = await fetch('/session-log', {
      headers: { Authorization: `Bearer ${token}` },
    });

    if (response.status === 401) {
      logout();
      return;
    }

    if (response.status === 404) {
      alert('No session log yet. Run at least one query first.');
      return;
    }

    if (!response.ok) {
      const detail = await _getErrorDetail(response);
      alert(`Could not download session log: ${detail}`);
      return;
    }

    const blob = await response.blob();
    const username = localStorage.getItem('spts_username') || 'user';
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `session_${username}.json`;
    document.body.appendChild(link);
    link.click();
    link.remove();
    URL.revokeObjectURL(url);
  } catch (error) {
    console.error('Session log download failed:', error);
    alert('Failed to download session log. Please try again.');
  } finally {
    if (btn) {
      btn.disabled = false;
      btn.textContent = originalText || 'Download Session Log';
    }
  }
}

function _prepareRunUi() {
  const btn = document.getElementById("runBtn");
  const rationaleDataBlock = document.getElementById("rationale-data");
  const rationaleLoader = document.getElementById("rationale-loader");
  const rationaleEmpty = document.getElementById("rationale-empty");
  const rationaleBtn = document.getElementById("rationaleBtn");

  btn.disabled = true;
  btn.textContent = "Processing...";
  document.getElementById("resultsArea").style.opacity = "0.5";

  document.getElementById("base-sql").textContent = "Waiting...";
  document.getElementById("spts-sql").textContent = "Waiting...";
  document.getElementById("grounding-container").style.display = "none";
  document.getElementById("base-feedback").style.display = "none";
  document.getElementById("spts-feedback").style.display = "none";
  currentQueryIndex = null;
  _resetFeedbackButtons();

  if (rationaleDataBlock) rationaleDataBlock.style.display = "none";
  if (rationaleEmpty) rationaleEmpty.style.display = "none";
  if (rationaleLoader) rationaleLoader.style.display = "block";
  if (rationaleBtn) rationaleBtn.style.display = "flex";

  return { btn, rationaleLoader };
}

function _restoreRunUi(btn, rationaleLoader) {
  btn.textContent = "Run Experiment";
  const resultsArea = document.getElementById("resultsArea");
  resultsArea.style.opacity = "1";
  resultsArea.style.pointerEvents = "auto";
  if (rationaleLoader) rationaleLoader.style.display = "none";
  _syncRunButtonState();
}

async function _getErrorDetail(response) {
  let detail = `Request failed with status ${response.status}`;
  try {
    const errorBody = await response.json();
    if (errorBody && typeof errorBody.detail === 'string' && errorBody.detail.trim()) {
      detail = errorBody.detail.trim();
    }
  } catch (parseErr) {
    console.warn('Could not parse error response body:', parseErr);
  }
  return detail;
}

function _renderQueryUnavailable(detail) {
  document.getElementById("base-sql").textContent = "Unavailable";
  renderResult("base-result", [[`API Error: ${detail}`]]);
  document.getElementById("spts-sql").textContent = "Unavailable";
  renderResult("spts-result", [[`API Error: ${detail}`]]);
  document.getElementById("grounding-container").style.display = "none";
  showRationaleEmpty();
}

function _renderQuerySuccess(data) {
  document.getElementById("base-sql").textContent = data.baseline_sql;
  renderResult("base-result", data.baseline_result);

  document.getElementById("spts-sql").textContent = data.spts_sql;
  renderResult("spts-result", data.spts_result);

  currentQueryIndex = (data.query_index !== null && data.query_index !== undefined) ? data.query_index : null;
  if (currentQueryIndex !== null) {
    document.getElementById("base-feedback").style.display = "flex";
    document.getElementById("spts-feedback").style.display = "flex";
  }

  const groundingBox = document.getElementById("grounding-container");
  if (data.mappings && data.mappings.length > 0) {
    groundingBox.style.display = "block";
    renderKnowledgeGraph(data.mappings);
  } else {
    groundingBox.style.display = "none";
  }

  if (data.spts_rationale) {
    populateRationale(data.spts_rationale);
  } else {
    showRationaleEmpty();
  }
}

async function runQuery() {
  const queryInput = document.getElementById("query");
  if (!queryInput?.value?.trim()) {
    _syncRunButtonState();
    return;
  }
  const { btn, rationaleLoader } = _prepareRunUi();

  try {
    const token = localStorage.getItem('spts_token');
    if (!token) {
      logout();
      return;
    }

    const response = await fetch("/query", {
      method: "POST",
      headers: { 
        "Content-Type": "application/json",
        "Authorization": `Bearer ${token}`
      },
      body: JSON.stringify({ query: queryInput.value }),
    });

    if (response.status === 401) {
      logout();
      return;
    }

    if (response.status === 403) {
      alert('Your role is not allowed to run queries.');
      return;
    }

    if (!response.ok) {
      const detail = await _getErrorDetail(response);
      _renderQueryUnavailable(detail);
      return;
    }

    const data = await response.json();
    _renderQuerySuccess(data);

  } catch (err) {
    alert("Error: " + err);
    console.error(err);
    showRationaleEmpty();
  } finally {
    _restoreRunUi(btn, rationaleLoader);
  }
}

function renderResult(elementId, resultData) {
  const el = document.getElementById(elementId);

  if (!resultData) {
    el.className = "status status-empty";
    el.textContent = "No Data";
    return;
  }

  if (Array.isArray(resultData) && resultData.length > 0) {
    const firstRowStr = JSON.stringify(resultData[0]).toLowerCase();
    if (
      firstRowStr.includes("error") ||
      firstRowStr.includes("no such column") ||
      firstRowStr.includes("syntax")
    ) {
      el.className = "status status-empty";
      el.style.backgroundColor = "var(--primary-light)";
      el.style.color = "var(--primary)";
      el.style.borderColor = "var(--danger)";
      el.textContent = "Execution Error:\n" + resultData[0];
      return;
    }
  }

  if (Array.isArray(resultData) && resultData.length === 0) {
    el.className = "status status-empty";
    el.textContent = "0 Rows Returned (No Match)";
    return;
  }

  if (
    Array.isArray(resultData) &&
    resultData.length === 1 &&
    resultData[0][0] == 0
  ) {
    el.className = "status status-empty";
    el.textContent = "Count is 0 (No Semantic Match)";
    return;
  }

  el.className = "status status-success";
  const count = resultData.length;

  let sample = "null";
  const validRow = resultData.find((row) => row && row[0] !== null);

  if (validRow) {
    sample = validRow[0];
  }

  if (count === 1 && !Number.isNaN(Number(sample))) {
    el.textContent = `Success: Count is ${sample}`;
  } else {
    el.textContent = `Success: ${count} Row(s) Found\nSample: "${sample}"...`;
  }
}

let network = null;

function renderKnowledgeGraph(mappings) {
  const style = getComputedStyle(document.documentElement);
  
  const colorPrimary = style.getPropertyValue('--primary').trim();
  const colorPrimaryLight = style.getPropertyValue('--primary-light').trim();
  const colorSecondary = style.getPropertyValue('--secondary').trim();
  const colorSecondaryLight = style.getPropertyValue('--success').trim();
  const colorDanger = style.getPropertyValue('--danger').trim();

  const nodes = new vis.DataSet();
  const edges = new vis.DataSet();
  
  let nodeId = 1;
  const addedNodes = new Map(); // Keep track by label to avoid duplicate schema nodes

  mappings.forEach((mapping) => {
    // Node A: Extracted Entity
    const entityId = nodeId++;
    nodes.add({
      id: entityId,
      label: mapping.original,
      shape: "box",
      color: { background: colorPrimaryLight, border: colorDanger },
      font: { color: colorPrimary, face: 'system-ui', size: 14 }
    });

    // Node B: Canonical Database Value
    const valueId = nodeId++;
    nodes.add({
      id: valueId,
      label: mapping.grounded,
      shape: "box",
      color: { background: colorPrimaryLight, border: colorSecondary },
      font: { color: colorPrimary, face: 'system-ui', size: 14 }
    });

    // Node C: Database Schema Location (Deduplicate)
    const schemaLabel = `${mapping.table}.${mapping.column}`;
    let schemaId;
    if (addedNodes.has(schemaLabel)) {
      schemaId = addedNodes.get(schemaLabel);
    } else {
      schemaId = nodeId++;
      nodes.add({
        id: schemaId,
        label: schemaLabel,
        shape: "ellipse",
        color: { background: colorSecondaryLight, border: colorSecondary },
        font: { color: colorPrimary, face: 'system-ui', size: 14 } // ensure text visibility over light blue
      });
      addedNodes.set(schemaLabel, schemaId);
    }

    const matchLabel = `${mapping.type} (${mapping.distance.toFixed(2)})`;
    edges.add({
      from: entityId,
      to: valueId,
      label: matchLabel,
      arrows: "to",
      font: { size: 12, face: 'system-ui', align: "top" },
      color: { color: colorPrimary },
      dashes: true
    });

    edges.add({
      from: valueId,
      to: schemaId,
      label: "Found in Column",
      arrows: "to",
      font: { size: 12, face: 'system-ui', align: "top" },
      color: { color: colorPrimary }
    });
  });

  const container = document.getElementById("kg-network");
  const data = { nodes: nodes, edges: edges };
  const options = {
    layout: {
      hierarchical: {
        direction: "LR", 
        sortMethod: "directed",
        nodeSpacing: 150,
        levelSeparation: 250
      }
    },
    physics: false,
    interaction: {
      dragNodes: false,
      zoomView: true,
      dragView: true
    }
  };

  if (network !== null) {
    network.destroy();
    network = null;
  }
  
  network = new vis.Network(container, data, options);
}

let isRationaleVisible = false;
let currentRationaleData = null;
let currentQueryIndex = null;

function toggleRationale() {
  const panel = document.getElementById("rationale-panel");
  if (!panel) return;

  isRationaleVisible = !isRationaleVisible;
  
  if (isRationaleVisible) {
    panel.style.display = "flex";
    panel.scrollIntoView({ behavior: 'smooth', block: 'start' });
  } else {
    panel.style.display = "none";
  }
}

function showRationaleEmpty() {
  document.getElementById("rationale-loader").style.display = "none";
  document.getElementById("rationale-data").style.display = "none";
  document.getElementById("rationale-empty").style.display = "flex";
}

function populateRationale(rationaleObj) {
  currentRationaleData = rationaleObj;
  
  document.getElementById("rationale-empty").style.display = "none";
  document.getElementById("rationale-loader").style.display = "none";
  document.getElementById("rationale-data").style.display = "block";

  document.getElementById("rat-latency").textContent = `${rationaleObj.latency_ms} ms`;
  document.getElementById("rat-tokens").textContent = rationaleObj.token_usage ? rationaleObj.token_usage.total_tokens : "--";

  document.getElementById("rat-system").textContent = rationaleObj.system_prompt || "No system prompt recorded.";
  
  document.getElementById("rat-context").textContent = rationaleObj.injected_context || "None";
  
  let flagsData = { ...rationaleObj };
  delete flagsData.system_prompt;
  delete flagsData.injected_context;
  document.getElementById("rat-flags").textContent = JSON.stringify(flagsData, null, 2);
}

async function copyRationale() {
  if (!currentRationaleData) {
    alert("No reasoning data available to copy.");
    return;
  }
  
  try {
    const textToCopy = JSON.stringify(currentRationaleData, null, 2);
    await navigator.clipboard.writeText(textToCopy);
    alert("Copied reasoning trace to clipboard!");
  } catch (err) {
    console.error("Failed to copy text: ", err);
    alert("Failed to copy to clipboard.");
  }
}

async function copySptsSql() {
  const sptsSqlEl = document.getElementById('spts-sql');
  if (!sptsSqlEl) return;

  const sqlText = sptsSqlEl.textContent?.trim() || '';
  if (!sqlText || sqlText === 'Waiting for input...' || sqlText === 'Waiting...' || sqlText === 'Unavailable') {
    alert('No generated SQL to copy yet.');
    return;
  }

  try {
    await navigator.clipboard.writeText(sqlText);
    alert('Copied SPTS SQL to clipboard!');
  } catch (err) {
    console.error('Failed to copy SPTS SQL:', err);
    alert('Failed to copy SQL to clipboard.');
  }
}

function _resetFeedbackButtons() {
  for (const prefix of ['base', 'spts']) {
    const pos = document.getElementById(`${prefix}-helpful`);
    const neg = document.getElementById(`${prefix}-unhelpful`);
    if (pos) { pos.className = 'feedback-btn feedback-btn-pos'; pos.disabled = false; }
    if (neg) { neg.className = 'feedback-btn feedback-btn-neg'; neg.disabled = false; }
  }
}

async function submitFeedback(model, rating) {
  if (currentQueryIndex === null) return;
  const token = localStorage.getItem('spts_token');
  if (!token) return;

  const payload = { query_index: currentQueryIndex };
  if (model === 'baseline') payload.baseline_rating = rating;
  else payload.spts_rating = rating;

  try {
    await fetch('/feedback', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
      body: JSON.stringify(payload),
    });
  } catch (e) {
    console.warn('Feedback submission failed:', e);
  }

  const prefix = model === 'baseline' ? 'base' : 'spts';
  const posBtn = document.getElementById(`${prefix}-helpful`);
  const negBtn = document.getElementById(`${prefix}-unhelpful`);
  posBtn.className = 'feedback-btn ' + (rating === 'helpful' ? 'feedback-btn-selected' : 'feedback-btn-pos');
  negBtn.className = 'feedback-btn ' + (rating === 'unhelpful' ? 'feedback-btn-selected' : 'feedback-btn-neg');
  posBtn.disabled = true;
  negBtn.disabled = true;
}
