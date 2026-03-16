document.addEventListener('DOMContentLoaded', () => {
  const token = localStorage.getItem('spts_token');
  if (!token) {
      window.location.href = '/static/login.html';
  }
});

function logout() {
  localStorage.removeItem('spts_token');
  window.location.href = '/static/login.html';
}

async function runQuery() {
  const queryInput = document.getElementById("query");
  const btn = document.getElementById("runBtn");

  btn.disabled = true;
  btn.textContent = "Processing...";
  document.getElementById("resultsArea").style.opacity = "0.5";

  document.getElementById("base-sql").textContent = "Waiting...";
  document.getElementById("spts-sql").textContent = "Waiting...";
  document.getElementById("grounding-container").style.display = "none";

  // Reset Rationale Panel State before fetch
  const rationaleDataBlock = document.getElementById("rationale-data");
  const rationaleLoader = document.getElementById("rationale-loader");
  const rationaleEmpty = document.getElementById("rationale-empty");
  const rationaleBtn = document.getElementById("rationaleBtn");

  if (rationaleDataBlock) rationaleDataBlock.style.display = "none";
  if (rationaleEmpty) rationaleEmpty.style.display = "none";
  if (rationaleLoader) rationaleLoader.style.display = "block";
  if (rationaleBtn) rationaleBtn.style.display = "flex"; // Show the button when loading starts

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

    const data = await response.json();

    document.getElementById("base-sql").textContent = data.baseline_sql;
    renderResult("base-result", data.baseline_result);

    document.getElementById("spts-sql").textContent = data.spts_sql;
    renderResult("spts-result", data.spts_result);

    const groundingBox = document.getElementById("grounding-container");

    if (data.mappings && data.mappings.length > 0) {
      groundingBox.style.display = "block";
      renderKnowledgeGraph(data.mappings);
    } else {
      groundingBox.style.display = "none";
    }

    // Populate Rationale Panel (Using SPTS Rationale)
    if (data.spts_rationale) {
      populateRationale(data.spts_rationale);
    } else {
      showRationaleEmpty();
    }

  } catch (err) {
    alert("Error: " + err);
    console.error(err);
    showRationaleEmpty();
  } finally {
    btn.disabled = false;
    btn.textContent = "Run Experiment";
    const resultsArea = document.getElementById("resultsArea");
    resultsArea.style.opacity = "1";
    resultsArea.style.pointerEvents = "auto";
    if (rationaleLoader) rationaleLoader.style.display = "none";
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
      el.style.backgroundColor = "#fee2e2";
      el.style.color = "#991b1b";
      el.style.borderColor = "#f87171";
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

  if (count === 1 && !isNaN(sample)) {
    el.textContent = `Success: Count is ${sample}`;
  } else {
    el.textContent = `Success: ${count} Row(s) Found\nSample: "${sample}"...`;
  }
}

let network = null;

function renderKnowledgeGraph(mappings) {
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
      color: { background: "#fecdd3", border: "#f43f5e" },
      font: { color: "#881337", face: 'system-ui', size: 14 }
    });

    // Node B: Canonical Database Value
    const valueId = nodeId++;
    nodes.add({
      id: valueId,
      label: mapping.grounded,
      shape: "box",
      color: { background: "#dcfce7", border: "#22c55e" },
      font: { color: "#14532d", face: 'system-ui', size: 14 }
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
        color: { background: "#dbeafe", border: "#3b82f6" },
        font: { color: "#1e3a8a", face: 'system-ui', size: 14 } // ensure text visibility over light blue
      });
      addedNodes.set(schemaLabel, schemaId);
    }

    // Edge A -> B: Distance/Match
    // E.g. "Semantic Match (0.65)"
    const matchLabel = `${mapping.type} (${mapping.distance.toFixed(2)})`;
    edges.add({
      from: entityId,
      to: valueId,
      label: matchLabel,
      arrows: "to",
      font: { size: 12, face: 'system-ui', align: "top" },
      color: { color: "#94a3b8" },
      dashes: true
    });

    // Edge B -> C: Found in Column
    edges.add({
      from: valueId,
      to: schemaId,
      label: "Found in Column",
      arrows: "to",
      font: { size: 12, face: 'system-ui', align: "top" },
      color: { color: "#94a3b8" }
    });
  });

  const container = document.getElementById("kg-network");
  const data = { nodes: nodes, edges: edges };
  const options = {
    layout: {
      hierarchical: {
        direction: "LR", // Left to Right
        sortMethod: "directed",
        nodeSpacing: 150,
        levelSeparation: 250
      }
    },
    physics: false, // Hierarchical layout disables standard physics but keep this clear
    interaction: {
      dragNodes: false,
      zoomView: true,
      dragView: true
    }
  };

  // If a network already exists, destroy it before rendering the new one
  if (network !== null) {
    network.destroy();
    network = null;
  }
  
  network = new vis.Network(container, data, options);
}

// ==========================================
// Rationale Side Panel Logic
// ==========================================

let isRationaleVisible = false;
let currentRationaleData = null;

function toggleRationale() {
  const panel = document.getElementById("rationale-panel");
  if (!panel) return;

  isRationaleVisible = !isRationaleVisible;
  
  if (isRationaleVisible) {
    // Show as flex since it's a card container
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

  // Metrics
  document.getElementById("rat-latency").textContent = `${rationaleObj.latency_ms} ms`;
  document.getElementById("rat-tokens").textContent = rationaleObj.token_usage ? rationaleObj.token_usage.total_tokens : "--";

  // System Prompt
  document.getElementById("rat-system").textContent = rationaleObj.system_prompt || "No system prompt recorded.";
  
  // Injected Context
  document.getElementById("rat-context").textContent = rationaleObj.injected_context || "None";
  
  // Flags & Usage Structure
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
