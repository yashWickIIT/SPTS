async function runQuery() {
  const queryInput = document.getElementById("query");
  const btn = document.getElementById("runBtn");

  btn.disabled = true;
  btn.textContent = "Processing...";
  document.getElementById("resultsArea").style.opacity = "0.5";

  document.getElementById("base-sql").textContent = "Waiting...";
  document.getElementById("spts-sql").textContent = "Waiting...";
  document.getElementById("grounding-container").style.display = "none";

  try {
    const response = await fetch("/query", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ query: queryInput.value }),
    });

    const data = await response.json();

    document.getElementById("base-sql").textContent = data.baseline_sql;
    renderResult("base-result", data.baseline_result);

    document.getElementById("spts-sql").textContent = data.spts_sql;
    renderResult("spts-result", data.spts_result);

    const groundingBox = document.getElementById("grounding-container");
    const groundingText = document.getElementById("grounding-text");

    if (data.mappings && data.mappings.length > 0) {
      groundingBox.style.display = "block";
      groundingText.innerHTML = data.mappings
        .map(
          (m) =>
            `<div><strong>${m.original}</strong> <span class="arrow">➜</span> <strong>${m.grounded}</strong></div>`,
        )
        .join("");
    } else {
      groundingBox.style.display = "none";
    }
  } catch (err) {
    alert("Error: " + err);
    console.error(err);
  } finally {
    btn.disabled = false;
    btn.textContent = "Run Experiment";
    document.getElementById("resultsArea").style.opacity = "1";
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
