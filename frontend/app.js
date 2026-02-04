async function runQuery() {
    const queryInput = document.getElementById("query");
    const btn = document.getElementById("runBtn");
    const resultsArea = document.getElementById("resultsArea");
    
    btn.disabled = true;
    btn.textContent = "Processing...";
    resultsArea.style.opacity = "0.5";
    
    try {
        const response = await fetch("/query", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ query: queryInput.value })
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
            groundingText.innerHTML = data.mappings.map(m => 
                `<div><strong>${m.original}</strong> <span class="arrow">➜</span> <strong>${m.grounded}</strong></div>`
            ).join("");
        } else {
            groundingBox.style.display = "none";
        }

    } catch (err) {
        alert("Error: " + err);
    } finally {
        //Reset
        btn.disabled = false;
        btn.textContent = "Run Experiment";
        resultsArea.style.opacity = "1";
    }
}
