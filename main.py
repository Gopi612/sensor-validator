from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse
import csv
import json
import os
import io
import base64
from typing import List

import matplotlib
matplotlib.use("Agg")   # non-interactive backend — no display required
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

app = FastAPI(title="Sensor Data Validator API")

# Load thresholds once
with open("thresholds.json") as f:
    thresholds = json.load(f)

# Ensure upload folder exists
os.makedirs("uploads", exist_ok=True)

# --------------------------
# Helper: Validate CSV
# --------------------------
def validate_csv(file_path):
    anomalies = []
    stats = {
        "total_rows": 0,
        "total_anomalies": 0,
        "sensor_stats": {}
    }

    with open(file_path, "r") as f:
        reader = csv.DictReader(f)

        for row in reader:
            stats["total_rows"] += 1
            timestamp = row["timestamp"]

            for sensor, value in row.items():
                if sensor == "timestamp":
                    continue

                try:
                    value = float(value)
                except:
                    continue

                # Track stats
                if sensor not in stats["sensor_stats"]:
                    stats["sensor_stats"][sensor] = []

                stats["sensor_stats"][sensor].append(value)

                # Check thresholds
                if sensor not in thresholds:
                    continue

                min_val = thresholds[sensor]["min"]
                max_val = thresholds[sensor]["max"]

                if value < min_val:
                    reason = f"Below {min_val}"
                elif value > max_val:
                    reason = f"Above {max_val}"
                else:
                    reason = None

                if reason:
                    stats["total_anomalies"] += 1
                    anomalies.append({
                        "timestamp": timestamp,
                        "sensor": sensor,
                        "value": value,
                        "reason": reason
                    })

    # Calculate stats
    for sensor, values in stats["sensor_stats"].items():
        stats["sensor_stats"][sensor] = {
            "min": min(values),
            "max": max(values),
            "avg": sum(values) / len(values)
        }

    return anomalies, stats

# --------------------------
# Save anomalies CSV
# --------------------------
def save_anomalies(anomalies):
    output_file = "anomalies.csv"

    with open(output_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["timestamp", "sensor", "value", "reason"])
        writer.writeheader()
        writer.writerows(anomalies)

    return output_file

# --------------------------
# API Endpoint
# --------------------------
@app.post("/validate/")
async def validate(file: UploadFile = File(...)):
    file_path = f"uploads/{file.filename}"

    # Save uploaded file
    with open(file_path, "wb") as f:
        f.write(await file.read())

    anomalies, stats = validate_csv(file_path)
    output_file = save_anomalies(anomalies)

    return {
        "message": "Validation complete",
        "summary": stats,
        "anomalies_file": output_file,
        "anomalies_count": len(anomalies),
        "anomalies": anomalies
    }

# --------------------------
# Root Endpoint — HTML Upload UI
# --------------------------
@app.get("/", response_class=HTMLResponse)
def home():
    return """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>Sensor Data Validator</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: 'Segoe UI', Arial, sans-serif;
      background: #f0f4f8;
      min-height: 100vh;
      display: flex;
      align-items: center;
      justify-content: center;
      padding: 24px;
    }
    .card {
      background: #fff;
      border-radius: 12px;
      box-shadow: 0 4px 24px rgba(0,0,0,0.10);
      padding: 40px 36px;
      max-width: 620px;
      width: 100%;
    }
    h1 { font-size: 1.6rem; color: #1e293b; margin-bottom: 6px; }
    .subtitle { color: #64748b; font-size: 0.95rem; margin-bottom: 28px; }
    .upload-area {
      border: 2px dashed #cbd5e1;
      border-radius: 10px;
      padding: 32px;
      text-align: center;
      cursor: pointer;
      transition: border-color 0.2s, background 0.2s;
      margin-bottom: 20px;
    }
    .upload-area:hover, .upload-area.drag-over {
      border-color: #3b82f6;
      background: #eff6ff;
    }
    .upload-icon { font-size: 2.5rem; margin-bottom: 10px; }
    .upload-area p { color: #64748b; font-size: 0.9rem; margin-top: 6px; }
    .upload-area strong { color: #1e293b; }
    #fileInput { display: none; }
    #fileName {
      font-size: 0.85rem;
      color: #3b82f6;
      margin-top: 8px;
      font-weight: 600;
    }
    button[type=submit] {
      width: 100%;
      padding: 12px;
      background: #3b82f6;
      color: #fff;
      border: none;
      border-radius: 8px;
      font-size: 1rem;
      font-weight: 600;
      cursor: pointer;
      transition: background 0.2s;
    }
    button[type=submit]:hover { background: #2563eb; }
    button[type=submit]:disabled { background: #93c5fd; cursor: not-allowed; }
    #status { margin-top: 18px; font-size: 0.9rem; color: #64748b; text-align: center; }
    #results { margin-top: 24px; display: none; }
    .kpi-grid {
      display: grid;
      grid-template-columns: repeat(3, 1fr);
      gap: 12px;
      margin-bottom: 20px;
    }
    .kpi {
      background: #f8fafc;
      border-radius: 8px;
      padding: 14px;
      text-align: center;
      border-left: 4px solid;
    }
    .kpi.total  { border-color: #3b82f6; }
    .kpi.valid  { border-color: #22c55e; }
    .kpi.anomaly { border-color: #ef4444; }
    .kpi-val { font-size: 1.8rem; font-weight: 800; }
    .kpi.total  .kpi-val { color: #3b82f6; }
    .kpi.valid  .kpi-val { color: #22c55e; }
    .kpi.anomaly .kpi-val { color: #ef4444; }
    .kpi-label { font-size: 0.72rem; color: #94a3b8; text-transform: uppercase; margin-top: 2px; }
    table { width: 100%; border-collapse: collapse; font-size: 0.85rem; margin-top: 12px; }
    th { background: #f1f5f9; padding: 8px 10px; text-align: left;
         font-size: 0.75rem; text-transform: uppercase; color: #64748b; }
    td { padding: 8px 10px; border-bottom: 1px solid #f1f5f9; }
    tr.anomaly-row td { background: #fff5f5; }
    h3 { font-size: 1rem; color: #1e293b; margin: 18px 0 8px; }
    .download-btn {
      display: inline-block;
      margin-top: 16px;
      padding: 9px 20px;
      background: #22c55e;
      color: #fff;
      border-radius: 7px;
      text-decoration: none;
      font-weight: 600;
      font-size: 0.9rem;
    }
    .download-btn:hover { background: #16a34a; }
    .charts-section { margin-top: 24px; }
    .chart-title { font-size: 1rem; font-weight: 700; color: #1e293b; margin-bottom: 10px; }
    .chart-img { width: 100%; border-radius: 10px; border: 1px solid #e2e8f0; margin-bottom: 16px; }
    .chart-grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
    .chart-grid-2 img { width: 100%; border-radius: 8px; border: 1px solid #e2e8f0; }
    .report-link { display: inline-block; margin-top: 8px; padding: 9px 20px;
      background: #3b82f6; color: #fff; border-radius: 7px;
      text-decoration: none; font-weight: 600; font-size: 0.9rem; }
    .report-link:hover { background: #2563eb; }
  </style>
</head>
<body>
<div class="card">
  <h1>📊 Sensor Data Validator</h1>
  <p class="subtitle">Upload a sensor readings CSV to validate against thresholds.</p>

  <form id="uploadForm">
    <div class="upload-area" id="dropZone" onclick="document.getElementById('fileInput').click()">
      <div class="upload-icon">📁</div>
      <strong>Click to choose a CSV file</strong>
      <p>or drag and drop it here</p>
      <div id="fileName"></div>
    </div>
    <input type="file" id="fileInput" name="file" accept=".csv" required />
    <button type="submit" id="submitBtn">Validate</button>
  </form>

  <div id="status"></div>

  <div id="results">
    <div class="kpi-grid">
      <div class="kpi total">
        <div class="kpi-val" id="kpiTotal">—</div>
        <div class="kpi-label">Total Rows</div>
      </div>
      <div class="kpi valid">
        <div class="kpi-val" id="kpiValid">—</div>
        <div class="kpi-label">Valid</div>
      </div>
      <div class="kpi anomaly">
        <div class="kpi-val" id="kpiAnom">—</div>
        <div class="kpi-label">Anomalies</div>
      </div>
    </div>

    <h3>📈 Sensor Statistics</h3>
    <table id="statsTable">
      <thead><tr><th>Sensor</th><th>Min</th><th>Max</th><th>Avg</th></tr></thead>
      <tbody></tbody>
    </table>

    <h3>🚨 Anomalies</h3>
    <table id="anomalyTable">
      <thead><tr><th>Timestamp</th><th>Sensor</th><th>Value</th><th>Reason</th></tr></thead>
      <tbody></tbody>
    </table>

    <a href="/download/anomalies" class="download-btn">⬇ Download anomalies.csv</a>
  </div>

  <div id="chartsSection" class="charts-section" style="display:none">
    <p class="chart-title">📈 Anomalies per Sensor</p>
    <img id="barChart" class="chart-img" src="" alt="Anomalies bar chart"/>
    <p class="chart-title">📋 Sensor Value Distributions</p>
    <div class="chart-grid-2" id="histGrid"></div>
    <br/>
    <a href="/report" class="report-link" target="_blank">📄 Open Full Report</a>
  </div>
</div>

<script>
  const dropZone   = document.getElementById('dropZone');
  const fileInput  = document.getElementById('fileInput');
  const fileNameEl = document.getElementById('fileName');
  const submitBtn  = document.getElementById('submitBtn');
  const status     = document.getElementById('status');
  const results    = document.getElementById('results');

  fileInput.addEventListener('change', () => {
    fileNameEl.textContent = fileInput.files[0]?.name || '';
  });

  dropZone.addEventListener('dragover', e => { e.preventDefault(); dropZone.classList.add('drag-over'); });
  dropZone.addEventListener('dragleave', ()  => dropZone.classList.remove('drag-over'));
  dropZone.addEventListener('drop', e => {
    e.preventDefault();
    dropZone.classList.remove('drag-over');
    fileInput.files = e.dataTransfer.files;
    fileNameEl.textContent = fileInput.files[0]?.name || '';
  });

  document.getElementById('uploadForm').addEventListener('submit', async e => {
    e.preventDefault();
    if (!fileInput.files.length) return;

    submitBtn.disabled = true;
    status.textContent = '⏳ Validating…';
    results.style.display = 'none';

    const fd = new FormData();
    fd.append('file', fileInput.files[0]);

    try {
      const res  = await fetch('/validate/', { method: 'POST', body: fd });
      const data = await res.json();

      if (!res.ok) { status.textContent = '❌ ' + (data.detail || 'Error'); return; }

      // KPIs
      document.getElementById('kpiTotal').textContent = data.summary.total_rows;
      document.getElementById('kpiAnom').textContent  = data.summary.total_anomalies;
      document.getElementById('kpiValid').textContent = data.summary.total_rows - data.summary.total_anomalies;

      // Sensor stats table
      const statsTbody = document.querySelector('#statsTable tbody');
      statsTbody.innerHTML = '';
      for (const [sensor, v] of Object.entries(data.summary.sensor_stats)) {
        statsTbody.innerHTML += `<tr>
          <td><strong>${sensor}</strong></td>
          <td>${v.min}</td><td>${v.max}</td>
          <td>${v.avg.toFixed(2)}</td>
        </tr>`;
      }

      // Anomalies table
      const anomTbody = document.querySelector('#anomalyTable tbody');
      anomTbody.innerHTML = '';
      for (const a of data.anomalies) {
        anomTbody.innerHTML += `<tr class="anomaly-row">
          <td>${a.timestamp}</td><td>${a.sensor}</td>
          <td>${a.value}</td><td>${a.reason}</td>
        </tr>`;
      }

      status.textContent = data.anomalies.length
        ? `✅ Done — ${data.anomalies.length} anomalies found.`
        : '✅ Done — all readings are valid!';
      results.style.display = 'block';

      // Load chart images from the charts API
      const barChart = document.getElementById('barChart');
      barChart.src = '/charts/anomalies?t=' + Date.now();
      barChart.onerror = () => { barChart.style.display = 'none'; };

      const histGrid = document.getElementById('histGrid');
      histGrid.innerHTML = '';
      const sensors = Object.keys(data.summary.sensor_stats);
      for (const sensor of sensors) {
        const img = document.createElement('img');
        img.src = `/charts/sensor/${sensor}?t=` + Date.now();
        img.alt = sensor + ' distribution';
        img.onerror = () => img.remove();
        histGrid.appendChild(img);
      }
      document.getElementById('chartsSection').style.display = 'block';
    } catch (err) {
      status.textContent = '❌ Request failed: ' + err.message;
    } finally {
      submitBtn.disabled = false;
    }
  });
</script>
</body>
</html>
"""

# --------------------------
# Download Anomalies
# --------------------------
@app.get("/download/anomalies")
def download_anomalies():
    if not os.path.exists("anomalies.csv"):
        raise HTTPException(status_code=404, detail="No anomalies file found. Run /validate/ first.")
    return FileResponse(
        path="anomalies.csv",
        media_type="text/csv",
        filename="anomalies.csv"
    )

# --------------------------
# Chart helpers
# --------------------------
def _load_anomalies_counts() -> dict:
    """Return {sensor: count} from the current anomalies.csv."""
    if not os.path.exists("anomalies.csv"):
        return {}
    counts: dict = {}
    with open("anomalies.csv", newline="") as f:
        for row in csv.DictReader(f):
            s = row.get("sensor", "unknown")
            counts[s] = counts.get(s, 0) + 1
    return counts


def _load_sensor_values(sensor: str) -> list:
    """Return all numeric values for a sensor from uploads/ CSVs."""
    values = []
    for fname in os.listdir("uploads"):
        if not fname.endswith(".csv"):
            continue
        with open(os.path.join("uploads", fname), newline="") as f:
            for row in csv.DictReader(f):
                try:
                    values.append(float(row[sensor]))
                except (KeyError, ValueError):
                    pass
    return values


def _fig_to_response(fig) -> StreamingResponse:
    """Render a matplotlib figure to a PNG StreamingResponse."""
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=120, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return StreamingResponse(buf, media_type="image/png")


def _fig_to_base64(fig) -> str:
    """Render a matplotlib figure to a base64-encoded PNG string."""
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=120, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")


# --------------------------
# GET /charts/anomalies  — bar chart: anomalies per sensor
# --------------------------
@app.get("/charts/anomalies", responses={200: {"content": {"image/png": {}}}})
def chart_anomalies():
    counts = _load_anomalies_counts()
    if not counts:
        raise HTTPException(status_code=404, detail="No anomalies data. Run /validate/ first.")

    sensors = list(counts.keys())
    values  = [counts[s] for s in sensors]
    colors  = ["#ef4444", "#f97316", "#eab308", "#3b82f6", "#8b5cf6"]

    fig, ax = plt.subplots(figsize=(8, 4))
    bars = ax.bar(sensors, values, color=colors[:len(sensors)], edgecolor="white", linewidth=0.8)
    ax.bar_label(bars, padding=4, fontsize=11, fontweight="bold")
    ax.set_title("Anomalies per Sensor", fontsize=14, fontweight="bold", pad=14)
    ax.set_xlabel("Sensor", fontsize=11)
    ax.set_ylabel("Anomaly Count", fontsize=11)
    ax.set_ylim(0, max(values) * 1.25)
    ax.spines[["top", "right"]].set_visible(False)
    ax.set_facecolor("#f8fafc")
    fig.patch.set_facecolor("#ffffff")
    plt.tight_layout()
    return _fig_to_response(fig)


# --------------------------
# GET /charts/sensor/{name}  — histogram for one sensor's values
# --------------------------
@app.get("/charts/sensor/{sensor_name}", responses={200: {"content": {"image/png": {}}}})
def chart_sensor(sensor_name: str):
    if sensor_name not in thresholds:
        raise HTTPException(status_code=404, detail=f"Unknown sensor '{sensor_name}'.")

    values = _load_sensor_values(sensor_name)
    if not values:
        raise HTTPException(status_code=404, detail="No data found. Run /validate/ first.")

    lo = thresholds[sensor_name]["min"]
    hi = thresholds[sensor_name]["max"]
    unit = thresholds[sensor_name].get("unit", "")

    fig, ax = plt.subplots(figsize=(8, 4))
    n, bins, patches = ax.hist(values, bins=30, edgecolor="white", linewidth=0.6)

    # Colour bars: red if outside threshold, green if inside
    for patch, left in zip(patches, bins[:-1]):
        patch.set_facecolor("#ef4444" if left < lo or left > hi else "#22c55e")

    ax.axvline(lo, color="#dc2626", linestyle="--", linewidth=1.5, label=f"Min threshold ({lo}{unit})")
    ax.axvline(hi, color="#dc2626", linestyle="--", linewidth=1.5, label=f"Max threshold ({hi}{unit})")
    ax.set_title(f"{sensor_name.capitalize()} — Value Distribution", fontsize=14, fontweight="bold", pad=14)
    ax.set_xlabel(f"Value ({unit})", fontsize=11)
    ax.set_ylabel("Frequency", fontsize=11)
    ax.legend(fontsize=9)
    ax.spines[["top", "right"]].set_visible(False)
    ax.set_facecolor("#f8fafc")
    fig.patch.set_facecolor("#ffffff")
    plt.tight_layout()
    return _fig_to_response(fig)


# --------------------------
# GET /report  — Self-contained HTML report with embedded charts
# --------------------------
def _build_bar_chart_b64(counts: dict) -> str:
    sensors = list(counts.keys())
    values  = [counts[s] for s in sensors]
    colors  = ["#ef4444", "#f97316", "#eab308", "#3b82f6", "#8b5cf6"]
    fig, ax = plt.subplots(figsize=(7, 3.5))
    bars = ax.bar(sensors, values, color=colors[:len(sensors)], edgecolor="white")
    ax.bar_label(bars, padding=4, fontsize=10, fontweight="bold")
    ax.set_title("Anomalies per Sensor", fontsize=13, fontweight="bold", pad=12)
    ax.set_ylim(0, max(values) * 1.25)
    ax.spines[["top", "right"]].set_visible(False)
    ax.set_facecolor("#f8fafc"); fig.patch.set_facecolor("#ffffff")
    plt.tight_layout()
    return _fig_to_base64(fig)


def _build_hist_b64(sensor_name: str, values: list) -> str:
    lo   = thresholds[sensor_name]["min"]
    hi   = thresholds[sensor_name]["max"]
    unit = thresholds[sensor_name].get("unit", "")
    fig, ax = plt.subplots(figsize=(7, 3))
    n, bins, patches = ax.hist(values, bins=25, edgecolor="white", linewidth=0.5)
    for patch, left in zip(patches, bins[:-1]):
        patch.set_facecolor("#ef4444" if left < lo or left > hi else "#22c55e")
    ax.axvline(lo, color="#dc2626", linestyle="--", linewidth=1.4, label=f"Min ({lo}{unit})")
    ax.axvline(hi, color="#dc2626", linestyle="--", linewidth=1.4, label=f"Max ({hi}{unit})")
    ax.set_title(f"{sensor_name.capitalize()} Distribution", fontsize=12, fontweight="bold", pad=10)
    ax.set_xlabel(f"Value ({unit})", fontsize=10); ax.set_ylabel("Frequency", fontsize=10)
    ax.legend(fontsize=8)
    ax.spines[["top", "right"]].set_visible(False)
    ax.set_facecolor("#f8fafc"); fig.patch.set_facecolor("#ffffff")
    plt.tight_layout()
    return _fig_to_base64(fig)


@app.get("/report", response_class=HTMLResponse)
def html_report():
    """Return a self-contained HTML report with embedded charts."""
    if not os.path.exists("anomalies.csv"):
        raise HTTPException(status_code=404, detail="No data. Run /validate/ first.")

    counts = _load_anomalies_counts()
    total_anomalies = sum(counts.values())

    # Build sensor stats rows and histograms
    sensor_rows = ""
    hist_imgs   = ""
    for sensor in thresholds:
        values = _load_sensor_values(sensor)
        if not values:
            continue
        lo   = thresholds[sensor]["min"]
        hi   = thresholds[sensor]["max"]
        unit = thresholds[sensor].get("unit", "")
        avg  = sum(values) / len(values)
        sensor_rows += f"""<tr>
          <td><strong>{sensor.capitalize()}</strong></td>
          <td>{min(values):.2f}{unit}</td>
          <td>{max(values):.2f}{unit}</td>
          <td>{avg:.2f}{unit}</td>
          <td>{lo}{unit} – {hi}{unit}</td>
        </tr>\n"""
        b64 = _build_hist_b64(sensor, values)
        hist_imgs += f'<div class="chart-wrap"><img src="data:image/png;base64,{b64}" alt="{sensor} chart"/></div>\n'

    # Anomaly table rows
    anomaly_rows = ""
    with open("anomalies.csv", newline="") as f:
        for row in csv.DictReader(f):
            anomaly_rows += f"""<tr>
            <td>{row['timestamp']}</td><td>{row['sensor']}</td>
            <td>{row['value']}</td><td>{row['reason']}</td>
          </tr>\n"""

    bar_b64 = _build_bar_chart_b64(counts) if counts else ""
    bar_img = f'<img src="data:image/png;base64,{bar_b64}" alt="anomalies bar chart"/>' if bar_b64 else ""

    total_rows = len(_load_sensor_values(list(thresholds.keys())[0])) if thresholds else 0

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <title>Sensor Validation Report</title>
  <style>
    body{{font-family:'Segoe UI',Arial,sans-serif;background:#f0f4f8;margin:0;padding:24px;}}
    .wrap{{max-width:960px;margin:auto;}}
    h1{{color:#1e293b;margin-bottom:4px;}}
    .meta{{color:#64748b;font-size:.9rem;margin-bottom:28px;}}
    .card{{background:#fff;border-radius:12px;box-shadow:0 2px 12px #0001;padding:28px 32px;margin-bottom:24px;}}
    h2{{font-size:1.1rem;color:#1e293b;margin-bottom:14px;border-bottom:2px solid #f1f5f9;padding-bottom:8px;}}
    .kpi-grid{{display:grid;grid-template-columns:repeat(3,1fr);gap:14px;}}
    .kpi{{background:#f8fafc;border-radius:8px;padding:16px;text-align:center;border-left:4px solid;}}
    .kpi.t{{border-color:#3b82f6;}}.kpi.v{{border-color:#22c55e;}}.kpi.a{{border-color:#ef4444;}}
    .kpi-val{{font-size:2rem;font-weight:800;}}
    .kpi.t .kpi-val{{color:#3b82f6;}}.kpi.v .kpi-val{{color:#22c55e;}}.kpi.a .kpi-val{{color:#ef4444;}}
    .kpi-label{{font-size:.7rem;color:#94a3b8;text-transform:uppercase;margin-top:2px;}}
    table{{width:100%;border-collapse:collapse;font-size:.87rem;}}
    th{{background:#f1f5f9;padding:9px 12px;text-align:left;font-size:.76rem;text-transform:uppercase;color:#64748b;}}
    td{{padding:8px 12px;border-bottom:1px solid #f1f5f9;}}
    .chart-grid{{display:grid;grid-template-columns:1fr 1fr;gap:16px;}}
    .chart-wrap img,.bar-wrap img{{width:100%;border-radius:8px;border:1px solid #f1f5f9;}}
    .dl{{display:inline-block;margin-top:14px;padding:9px 20px;background:#22c55e;
         color:#fff;border-radius:7px;text-decoration:none;font-weight:600;font-size:.9rem;}}
    .dl:hover{{background:#16a34a;}}
  </style>
</head>
<body>
<div class="wrap">
  <h1>📊 Sensor Data Validation Report</h1>
  <p class="meta">Generated by Sensor Data Validator API</p>

  <div class="card">
    <h2>Summary</h2>
    <div class="kpi-grid">
      <div class="kpi t"><div class="kpi-val">{total_rows}</div><div class="kpi-label">Total Rows</div></div>
      <div class="kpi v"><div class="kpi-val">{total_rows - total_anomalies}</div><div class="kpi-label">Valid</div></div>
      <div class="kpi a"><div class="kpi-val">{total_anomalies}</div><div class="kpi-label">Anomalies</div></div>
    </div>
  </div>

  <div class="card">
    <h2>Anomalies per Sensor</h2>
    <div class="bar-wrap">{bar_img}</div>
  </div>

  <div class="card">
    <h2>Sensor Statistics</h2>
    <table>
      <thead><tr><th>Sensor</th><th>Min</th><th>Max</th><th>Avg</th><th>Threshold</th></tr></thead>
      <tbody>{sensor_rows}</tbody>
    </table>
  </div>

  <div class="card">
    <h2>Value Distributions</h2>
    <div class="chart-grid">{hist_imgs}</div>
  </div>

  <div class="card">
    <h2>Anomaly Details</h2>
    <table>
      <thead><tr><th>Timestamp</th><th>Sensor</th><th>Value</th><th>Reason</th></tr></thead>
      <tbody>{anomaly_rows}</tbody>
    </table>
    <a href="/download/anomalies" class="dl">⬇ Download anomalies.csv</a>
  </div>
</div>
</body></html>"""
