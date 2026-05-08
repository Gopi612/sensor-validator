#!/usr/bin/env python3
"""
=======================================================================
  Sensor Data Validator
=======================================================================
  Reads sensor readings from a CSV file and validates each value
  against thresholds defined in a JSON config file.

  Valid rows  → printed in GREEN
  Anomalies   → printed in RED with a clear reason message
  All anomalies are saved to anomalies.csv for further analysis.

  Usage:
      python validator.py --input data.csv --thresholds thresholds.json --output anomalies.csv

  Options:
      --input       PATH   Sensor readings CSV file          [default: data/usecase2_sensor_validator/sensor_readings.csv]
      --thresholds  PATH   Threshold definitions JSON file   [default: thresholds.json]
      --output      PATH   Anomalies output CSV file         [default: anomalies.csv]
      --verbose            Print every row (valid + anomaly)
      --quiet              Suppress row-level output; show summary only
      --version            Show version and exit
=======================================================================
"""

import argparse
import csv
import json
import logging
import os
import sys
from datetime import datetime
from typing import Dict, List, Optional

# Alert engine (email + webhook)
from alerts import AlertConfig, config_from_env, send_alerts

__version__ = "1.0.0"

# -----------------------------------------------------------------------
# Configurable default file paths
# -----------------------------------------------------------------------
DEFAULT_CSV_PATH        = os.path.join("data", "usecase2_sensor_validator", "sensor_readings.csv")
DEFAULT_THRESHOLDS_PATH = "thresholds.json"
DEFAULT_ANOMALIES_PATH  = "anomalies.csv"
DEFAULT_LOG_PATH        = "validator.log"
DEFAULT_REPORT_PATH     = "report.html"

# Module-level logger — configured at runtime by setup_logging()
logger = logging.getLogger("sensor_validator")

# -----------------------------------------------------------------------
# ANSI color codes for terminal output
# These work on Linux, macOS, and Windows 10+ terminals.
# -----------------------------------------------------------------------
GREEN  = "\033[92m"   # valid readings
RED    = "\033[91m"   # anomaly rows
YELLOW = "\033[93m"   # warnings / info highlights
CYAN   = "\033[96m"   # section headers / decorative lines
BOLD   = "\033[1m"
RESET  = "\033[0m"    # reset to default color


# -----------------------------------------------------------------------
# 0. LOGGING SETUP
# -----------------------------------------------------------------------

def setup_logging(log_path: str = DEFAULT_LOG_PATH, level: str = "INFO") -> None:
    """
    Configure the module logger to write to both the terminal (WARNING+)
    and a rotating-friendly plain-text log file (DEBUG+).

    Log file format:
        2024-01-15 08:02:00 | WARNING  | temperature = 95.7°C EXCEEDS ...

    Args:
        log_path: Path to the log file. Created (with directories) if absent.
        level:    Minimum log level for the file handler
                  (DEBUG / INFO / WARNING / ERROR). Terminal always shows WARNING+.
    """
    numeric_level = getattr(logging, level.upper(), logging.INFO)

    # Ensure the log directory exists
    log_dir = os.path.dirname(log_path)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)

    log_format = "%(asctime)s | %(levelname)-8s | %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    # File handler — captures everything at the configured level
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(numeric_level)
    file_handler.setFormatter(logging.Formatter(log_format, datefmt=date_format))

    # Stream handler — only ERROR and above; anomalies go to terminal via print()
    stream_handler = logging.StreamHandler(sys.stderr)
    stream_handler.setLevel(logging.ERROR)
    stream_handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))

    logger.setLevel(numeric_level)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    logger.info("Logging initialised → %s  (level: %s)", log_path, level.upper())


# -----------------------------------------------------------------------
# 1. LOAD THRESHOLDS
# -----------------------------------------------------------------------

def load_thresholds(filepath: str) -> Dict[str, dict]:
    """
    Read and parse the JSON thresholds file into a Python dictionary.

    Expected JSON structure:
        {
          "temperature": {"min": 10.0, "max": 80.0, "unit": "°C"},
          "pressure":    {"min": 950.0, "max": 1050.0, "unit": "hPa"},
          ...
        }

    Args:
        filepath: Path to the JSON thresholds file.

    Returns:
        Dictionary mapping sensor name → {min, max, unit}.

    Exits:
        Prints an error and exits if the file is missing or malformed.
    """
    if not os.path.exists(filepath):
        msg = f"Thresholds file not found: '{filepath}'"
        print(f"{RED}[ERROR] {msg}{RESET}")
        logger.error(msg)
        sys.exit(1)

    try:
        with open(filepath, "r", encoding="utf-8") as f:
            thresholds = json.load(f)
    except json.JSONDecodeError as exc:
        msg = f"Failed to parse thresholds JSON: {exc}"
        print(f"{RED}[ERROR] {msg}{RESET}")
        logger.error(msg)
        sys.exit(1)

    sensors = ', '.join(thresholds.keys())
    print(f"{CYAN}Loaded thresholds for: {sensors}{RESET}")
    logger.info("Loaded thresholds for: %s", sensors)
    return thresholds


# -----------------------------------------------------------------------
# 2. VALUE PARSING (handles missing / invalid values gracefully)
# -----------------------------------------------------------------------

def parse_float(raw: Optional[str], sensor: str) -> Optional[float]:
    """
    Safely convert a raw CSV string to a float.

    Treats empty strings, "null", "N/A", and "nan" as missing values.

    Args:
        raw:    The raw string value from the CSV cell.
        sensor: Sensor name — used only for context in error messages.

    Returns:
        A float if conversion succeeds, or None if the value is
        missing / unparseable.
    """
    if raw is None:
        return None

    cleaned = raw.strip().lower()

    # Treat these strings as "no data"
    if cleaned in ("", "null", "n/a", "nan", "none", "-"):
        return None

    try:
        return float(cleaned)
    except ValueError:
        # Value exists but cannot be converted to a number
        return None


# -----------------------------------------------------------------------
# 3. ANOMALY DETECTION
# -----------------------------------------------------------------------

def check_row(
    row: dict,
    thresholds: Dict[str, dict],
    row_number: int,
) -> List[dict]:
    """
    Compare every sensor column in *row* against its threshold limits.

    For each sensor defined in thresholds:
      - If the CSV value is missing / invalid  → anomaly (reason: missing)
      - If value < min                          → anomaly (below minimum)
      - If value > max                          → anomaly (exceeds maximum)

    Args:
        row:        One parsed CSV row (dict of column → raw string).
        thresholds: Threshold definitions loaded from JSON.
        row_number: Current row number (for display only).

    Returns:
        List of anomaly dicts; empty list means the row is fully valid.
        Each anomaly dict has keys: timestamp, sensor, value, reason.
    """
    anomalies = []
    timestamp = row.get("timestamp", "unknown")

    for sensor, limits in thresholds.items():
        raw_value = row.get(sensor)          # Raw string from CSV (may be None if column is absent)
        value     = parse_float(raw_value, sensor)
        unit      = limits.get("unit", "")
        min_val   = limits.get("min")
        max_val   = limits.get("max")

        # ── Case 1: Missing or unparseable value ──────────────────────
        if value is None:
            reason = f"Missing or invalid value for '{sensor}'"
            anomalies.append({
                "timestamp": timestamp,
                "sensor":    sensor,
                "value":     raw_value if raw_value is not None else "MISSING",
                "reason":    reason,
            })
            logger.warning("[%s] %s", timestamp, reason)
            continue   # No further checks needed for this sensor

        # ── Case 2: Value below minimum ───────────────────────────────
        if min_val is not None and value < min_val:
            reason = (
                f"{sensor} = {value}{unit} is BELOW minimum "
                f"threshold of {min_val}{unit}"
            )
            anomalies.append({"timestamp": timestamp, "sensor": sensor,
                              "value": value, "reason": reason})
            logger.warning("[%s] %s", timestamp, reason)

        # ── Case 3: Value above maximum ───────────────────────────────
        elif max_val is not None and value > max_val:
            reason = (
                f"{sensor} = {value}{unit} EXCEEDS maximum "
                f"threshold of {max_val}{unit}"
            )
            anomalies.append({"timestamp": timestamp, "sensor": sensor,
                              "value": value, "reason": reason})
            logger.warning("[%s] %s", timestamp, reason)

    return anomalies


# -----------------------------------------------------------------------
# 4. TERMINAL OUTPUT FORMATTING
# -----------------------------------------------------------------------

def print_row_result(
    row: dict,
    anomalies: List[dict],
    row_number: int,
    verbose: bool = False,
    quiet: bool = False,
) -> None:
    """
    Print a single row's validation result to the terminal.

    Green  → row is fully valid
    Red    → at least one anomaly was found

    Modes:
        default : print anomalies only
        verbose : print every row (valid + anomaly)
        quiet   : suppress all row-level output

    Args:
        row:        CSV row dictionary.
        anomalies:  Anomalies found in this row (empty = valid).
        row_number: Row index for the display prefix.
        verbose:    Print valid rows too.
        quiet:      Suppress all row output.
    """
    if quiet:
        return

    timestamp = row.get("timestamp", "unknown")
    sensor_id = row.get("sensor_id", "unknown")
    prefix    = f"[Row {row_number:04d}]"

    if not anomalies:
        # Print valid rows only in verbose mode
        if verbose:
            print(f"{GREEN}{prefix} ✔  VALID   | {timestamp} | {sensor_id}{RESET}")
    else:
        # Anomalies are always printed (unless quiet)
        reasons = " | ".join(a["reason"] for a in anomalies)
        print(f"{RED}{prefix} ✘  ANOMALY | {timestamp} | {sensor_id} | {reasons}{RESET}")


# -----------------------------------------------------------------------
# 5. SAVE ANOMALIES TO CSV
# -----------------------------------------------------------------------

def save_anomalies(anomalies: List[dict], output_path: str) -> None:
    """
    Write all detected anomalies to a CSV file.

    Output columns: timestamp, sensor, value, reason

    Args:
        anomalies:   List of anomaly dictionaries.
        output_path: File path for the output CSV.
    """
    fieldnames = ["timestamp", "sensor", "value", "reason"]

    # Create the output directory if it doesn't exist yet
    output_dir = os.path.dirname(output_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(anomalies)

    print(f"\n{CYAN}Anomalies saved → {output_path}{RESET}")
    logger.info("Anomalies saved → %s  (%d records)", output_path, len(anomalies))


# -----------------------------------------------------------------------
# 6b. HTML REPORT GENERATOR
# -----------------------------------------------------------------------

def generate_html_report(
    report_path: str,
    csv_path: str,
    thresholds: Dict[str, dict],
    total_rows: int,
    anomaly_rows: int,
    all_anomalies: List[dict],
    sensor_stats: Dict[str, dict],
    generated_at: str,
) -> None:
    """
    Write a self-contained HTML report to *report_path*.

    The report includes:
      - Run metadata banner
      - KPI cards (total rows / valid / anomalies)
      - Bar chart: anomalies per sensor  (Chart.js via CDN)
      - Doughnut chart: valid vs anomaly rows
      - Per-sensor statistics table (min / max / avg / threshold range)
      - Full anomalies detail table with row-level colouring

    The file is entirely self-contained — one HTML file, no local assets.

    Args:
        report_path:    Output path for the HTML file.
        csv_path:       Source CSV path (shown in metadata).
        thresholds:     Threshold definitions (for range column).
        total_rows:     Total rows processed.
        anomaly_rows:   Rows containing at least one anomaly.
        all_anomalies:  Flat list of every anomaly event dict.
        sensor_stats:   Per-sensor {min, max, sum, count} accumulators.
        generated_at:   Human-readable timestamp string.
    """
    valid_rows   = total_rows - anomaly_rows
    anomaly_pct  = (anomaly_rows / total_rows * 100) if total_rows else 0

    # ── Build sensor-level anomaly counts ─────────────────────────────
    sensor_counts: Dict[str, int] = {}
    for a in all_anomalies:
        sensor_counts[a["sensor"]] = sensor_counts.get(a["sensor"], 0) + 1

    # ── Chart.js data ─────────────────────────────────────────────────
    bar_labels  = json.dumps([s.capitalize() for s in sensor_counts])
    bar_values  = json.dumps(list(sensor_counts.values()))
    bar_colors  = json.dumps([
        "#e74c3c", "#e67e22", "#f39c12", "#9b59b6", "#3498db",
        "#1abc9c", "#2ecc71", "#e91e63", "#ff5722", "#607d8b",
    ][:len(sensor_counts)])

    # ── Anomaly rows HTML ─────────────────────────────────────────────
    anomaly_rows_html = ""
    for a in all_anomalies:
        severity = "critical" if "EXCEEDS" in a["reason"] else "warning"
        badge    = (
            '<span class="badge badge-critical">HIGH</span>'
            if severity == "critical"
            else '<span class="badge badge-warning">LOW</span>'
        )
        anomaly_rows_html += (
            f"<tr>"
            f"<td>{a['timestamp']}</td>"
            f"<td><strong>{a['sensor'].capitalize()}</strong></td>"
            f"<td class='val-cell'>{a['value']}</td>"
            f"<td>{badge}</td>"
            f"<td class='reason-cell'>{a['reason']}</td>"
            f"</tr>\n"
        )

    # ── Stats rows HTML ───────────────────────────────────────────────
    stats_rows_html = ""
    for sensor, stats in sensor_stats.items():
        limits   = thresholds.get(sensor, {})
        unit     = limits.get("unit", "")
        mn, mx   = limits.get("min", "—"), limits.get("max", "—")
        if stats["count"] > 0:
            avg = stats["sum"] / stats["count"]
            stats_rows_html += (
                f"<tr>"
                f"<td><strong>{sensor.capitalize()}</strong></td>"
                f"<td>{stats['min']:.1f} {unit}</td>"
                f"<td>{stats['max']:.1f} {unit}</td>"
                f"<td>{avg:.1f} {unit}</td>"
                f"<td>{mn} {unit} – {mx} {unit}</td>"
                f"<td>{stats['count']}</td>"
                f"</tr>\n"
            )
        else:
            stats_rows_html += (
                f"<tr>"
                f"<td><strong>{sensor.capitalize()}</strong></td>"
                f"<td colspan='4' class='na'>No data</td>"
                f"<td>0</td>"
                f"</tr>\n"
            )

    # ── Full HTML template ────────────────────────────────────────────
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Sensor Data Validator — Report</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
  <style>
    *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{
      font-family: 'Segoe UI', system-ui, sans-serif;
      background: #0f1117;
      color: #e2e8f0;
      padding: 2rem;
    }}
    h1 {{ font-size: 1.8rem; font-weight: 700; }}
    h2 {{ font-size: 1.1rem; font-weight: 600; color: #94a3b8; text-transform: uppercase;
          letter-spacing: .08em; margin: 2rem 0 .8rem; }}
    .header {{
      display: flex; justify-content: space-between; align-items: flex-start;
      padding-bottom: 1.2rem; border-bottom: 1px solid #1e293b; margin-bottom: 1.5rem;
    }}
    .meta {{ font-size: .78rem; color: #64748b; margin-top: .4rem; line-height: 1.7; }}
    .meta span {{ color: #94a3b8; }}
    /* KPI cards */
    .kpi-grid {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 1rem; margin-bottom: 2rem; }}
    .kpi {{
      background: #1e293b; border-radius: .75rem; padding: 1.25rem 1.5rem;
      border-left: 4px solid;
    }}
    .kpi.total  {{ border-color: #3b82f6; }}
    .kpi.valid  {{ border-color: #22c55e; }}
    .kpi.anom   {{ border-color: #ef4444; }}
    .kpi-val {{ font-size: 2.4rem; font-weight: 800; line-height: 1; margin-bottom: .3rem; }}
    .kpi.total .kpi-val {{ color: #60a5fa; }}
    .kpi.valid .kpi-val {{ color: #4ade80; }}
    .kpi.anom  .kpi-val {{ color: #f87171; }}
    .kpi-label {{ font-size: .8rem; color: #94a3b8; text-transform: uppercase; letter-spacing: .06em; }}
    .kpi-sub   {{ font-size: .75rem; color: #64748b; margin-top: .25rem; }}
    /* Charts */
    .charts-grid {{ display: grid; grid-template-columns: 2fr 1fr; gap: 1.5rem; margin-bottom: 2rem; }}
    .chart-card {{
      background: #1e293b; border-radius: .75rem; padding: 1.25rem;
    }}
    .chart-title {{ font-size: .85rem; font-weight: 600; color: #94a3b8;
                    text-transform: uppercase; letter-spacing: .06em; margin-bottom: 1rem; }}
    /* Tables */
    .tbl-wrap {{ overflow-x: auto; margin-bottom: 2rem; }}
    table {{ width: 100%; border-collapse: collapse; font-size: .85rem; }}
    thead th {{
      background: #1e293b; color: #94a3b8; font-weight: 600;
      text-transform: uppercase; letter-spacing: .05em;
      padding: .65rem 1rem; text-align: left; white-space: nowrap;
    }}
    tbody tr {{ border-bottom: 1px solid #1e293b; }}
    tbody tr:hover {{ background: #1e293b55; }}
    tbody td {{ padding: .6rem 1rem; vertical-align: middle; }}
    .val-cell    {{ font-family: monospace; font-weight: 600; color: #f87171; }}
    .reason-cell {{ color: #cbd5e1; font-size: .82rem; }}
    .na          {{ color: #475569; font-style: italic; }}
    /* Badges */
    .badge {{ display: inline-block; padding: .15rem .55rem; border-radius: 9999px;
              font-size: .72rem; font-weight: 700; text-transform: uppercase; }}
    .badge-critical {{ background: #7f1d1d; color: #fca5a5; }}
    .badge-warning  {{ background: #78350f; color: #fcd34d; }}
    /* Footer */
    footer {{ margin-top: 3rem; padding-top: 1rem; border-top: 1px solid #1e293b;
              font-size: .75rem; color: #475569; text-align: center; }}
  </style>
</head>
<body>

  <div class="header">
    <div>
      <h1>&#128200; Sensor Data Validator</h1>
      <div class="meta">
        Generated  <span>{generated_at}</span> &nbsp;|&nbsp;
        Source CSV <span>{csv_path}</span>
      </div>
    </div>
    <div class="meta" style="text-align:right">
      v{__version__} &nbsp;|&nbsp; Python stdlib + Chart.js
    </div>
  </div>

  <!-- KPI CARDS -->
  <h2>Run Overview</h2>
  <div class="kpi-grid">
    <div class="kpi total">
      <div class="kpi-val">{total_rows:,}</div>
      <div class="kpi-label">Total Rows Processed</div>
    </div>
    <div class="kpi valid">
      <div class="kpi-val">{valid_rows:,}</div>
      <div class="kpi-label">Valid Rows</div>
      <div class="kpi-sub">{100 - anomaly_pct:.1f}% pass rate</div>
    </div>
    <div class="kpi anom">
      <div class="kpi-val">{len(all_anomalies):,}</div>
      <div class="kpi-label">Anomaly Events</div>
      <div class="kpi-sub">{anomaly_pct:.1f}% of rows affected</div>
    </div>
  </div>

  <!-- CHARTS -->
  <h2>Charts</h2>
  <div class="charts-grid">
    <div class="chart-card">
      <div class="chart-title">Anomalies per Sensor</div>
      <canvas id="barChart" height="120"></canvas>
    </div>
    <div class="chart-card">
      <div class="chart-title">Valid vs Anomaly Rows</div>
      <canvas id="doughnutChart" height="120"></canvas>
    </div>
  </div>

  <!-- STATS TABLE -->
  <h2>Per-Sensor Statistics</h2>
  <div class="tbl-wrap">
    <table>
      <thead>
        <tr>
          <th>Sensor</th><th>Min</th><th>Max</th><th>Avg</th>
          <th>Threshold Range</th><th>Readings</th>
        </tr>
      </thead>
      <tbody>
        {stats_rows_html}
      </tbody>
    </table>
  </div>

  <!-- ANOMALIES TABLE -->
  <h2>Anomaly Details ({len(all_anomalies)} events)</h2>
  <div class="tbl-wrap">
    <table>
      <thead>
        <tr>
          <th>Timestamp</th><th>Sensor</th><th>Value</th>
          <th>Severity</th><th>Reason</th>
        </tr>
      </thead>
      <tbody>
        {anomaly_rows_html}
      </tbody>
    </table>
  </div>

  <footer>
    Sensor Data Validator v{__version__} &mdash; generated {generated_at}
  </footer>

  <script>
    // ── Bar chart: anomalies per sensor ──────────────────────────────
    new Chart(document.getElementById('barChart'), {{
      type: 'bar',
      data: {{
        labels: {bar_labels},
        datasets: [{{
          label: 'Anomaly Events',
          data: {bar_values},
          backgroundColor: {bar_colors},
          borderRadius: 6,
          borderSkipped: false,
        }}]
      }},
      options: {{
        responsive: true,
        plugins: {{
          legend: {{ display: false }},
          tooltip: {{ callbacks: {{ label: ctx => ` ${{ctx.parsed.y}} events` }} }}
        }},
        scales: {{
          x: {{ ticks: {{ color: '#94a3b8' }}, grid: {{ color: '#1e293b' }} }},
          y: {{
            ticks: {{ color: '#94a3b8', stepSize: 1 }},
            grid: {{ color: '#1e293b' }},
            beginAtZero: true
          }}
        }}
      }}
    }});

    // ── Doughnut chart: valid vs anomaly rows ─────────────────────────
    new Chart(document.getElementById('doughnutChart'), {{
      type: 'doughnut',
      data: {{
        labels: ['Valid', 'Anomaly'],
        datasets: [{{
          data: [{valid_rows}, {anomaly_rows}],
          backgroundColor: ['#22c55e', '#ef4444'],
          borderWidth: 0,
          hoverOffset: 8,
        }}]
      }},
      options: {{
        responsive: true,
        cutout: '65%',
        plugins: {{
          legend: {{
            position: 'bottom',
            labels: {{ color: '#94a3b8', padding: 16, font: {{ size: 12 }} }}
          }},
          tooltip: {{ callbacks: {{ label: ctx => ` ${{ctx.parsed}} rows` }} }}
        }}
      }}
    }});
  </script>
</body>
</html>"""

    # Write the report file
    report_dir = os.path.dirname(report_path)
    if report_dir:
        os.makedirs(report_dir, exist_ok=True)

    with open(report_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"{CYAN}HTML report saved → {report_path}{RESET}")
    logger.info("HTML report saved → %s", report_path)


# -----------------------------------------------------------------------
# 6b-alt. SIMPLE HTML REPORT  (lightweight alternative to 6b above)
# -----------------------------------------------------------------------

def generate_simple_html_report(
    anomalies: List[dict],
    stats: Dict[str, dict],
    output_file: str = "report.html",
) -> None:
    """
    Write a lightweight HTML validation report to *output_file*.

    This is a simpler alternative to generate_html_report() — no external
    JS dependencies, no dark theme, just a clean table-based layout.

    Args:
        anomalies:   List of anomaly dicts, each with keys:
                     timestamp, sensor, value, reason.
        stats:       Dict with keys:
                       total_rows      (int)
                       total_anomalies (int)
                       sensor_stats    (dict of sensor → {min, max, avg})
        output_file: Destination path for the HTML file.
    """
    html = f"""
    <html>
    <head>
        <title>Sensor Validation Report</title>
        <style>
            body {{ font-family: Arial; margin: 20px; }}
            h1 {{ color: #333; }}
            .summary {{ margin-bottom: 20px; }}
            table {{ border-collapse: collapse; width: 100%; margin-top: 20px; }}
            th, td {{ border: 1px solid #ddd; padding: 8px; text-align: center; }}
            th {{ background-color: #f2f2f2; }}
            .anomaly {{ background-color: #ffcccc; }}
        </style>
    </head>
    <body>

    <h1>📊 Sensor Data Validation Report</h1>

    <div class="summary">
        <h2>Summary</h2>
        <p><b>Total Rows:</b> {stats['total_rows']}</p>
        <p><b>Total Anomalies:</b> {stats['total_anomalies']}</p>
    </div>

    <h2>📈 Sensor Statistics</h2>
    <table>
        <tr><th>Sensor</th><th>Min</th><th>Max</th><th>Average</th></tr>
    """

    # Sensor stats
    for sensor, values in stats["sensor_stats"].items():
        html += f"""
        <tr>
            <td>{sensor}</td>
            <td>{values['min']}</td>
            <td>{values['max']}</td>
            <td>{round(values['avg'], 2)}</td>
        </tr>
        """

    html += """
    </table>

    <h2>🚨 Anomalies</h2>
    <table>
        <tr><th>Timestamp</th><th>Sensor</th><th>Value</th><th>Reason</th></tr>
    """

    # Anomaly rows
    for a in anomalies:
        html += f"""
        <tr class="anomaly">
            <td>{a['timestamp']}</td>
            <td>{a['sensor']}</td>
            <td>{a['value']}</td>
            <td>{a['reason']}</td>
        </tr>
        """

    html += """
    </table>

    </body>
    </html>
    """

    os.makedirs(os.path.dirname(os.path.abspath(output_file)), exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"📄 HTML report generated: {output_file}")


# -----------------------------------------------------------------------
# 6. PRINT SUMMARY
# -----------------------------------------------------------------------

def print_summary(
    total_rows: int,
    anomaly_rows: int,
    total_events: int,
    anomalies: List[dict],
    sensor_stats: Dict[str, dict],
) -> None:
    """
    Print a formatted summary table after processing all rows.

    Args:
        total_rows:    Total number of data rows read from the CSV.
        anomaly_rows:  Number of rows that had at least one anomaly.
        total_events:  Total individual anomaly events detected.
        anomalies:     Full anomaly list (used to show per-sensor counts).
        sensor_stats:  Per-sensor {min, max, sum, count} accumulators.
    """
    valid_rows = total_rows - anomaly_rows
    divider    = CYAN + "─" * 65 + RESET

    # Build per-sensor anomaly counts
    sensor_counts: Dict[str, int] = {}
    for a in anomalies:
        sensor_counts[a["sensor"]] = sensor_counts.get(a["sensor"], 0) + 1

    print(f"\n{divider}")
    print(f"{BOLD}  VALIDATION SUMMARY{RESET}")
    print(divider)

    # ── Primary stats (exact format requested) ────────────────────────
    print(f"  {BOLD}Total rows processed  :{RESET} {BOLD}{total_rows}{RESET}")
    print(f"  {GREEN}Valid rows            : {valid_rows}{RESET}")
    print(f"  {RED}Total anomalies       : {total_events}{RESET}")

    # ── Per-sensor breakdown ──────────────────────────────────────────
    if sensor_counts:
        print(f"\n  {YELLOW}Anomaly breakdown by sensor:{RESET}")
        for sensor, count in sorted(sensor_counts.items(), key=lambda x: -x[1]):
            # Capitalise sensor name for readability: "temperature" → "Temperature"
            label = f"{sensor.capitalize()} anomalies"
            print(f"    {RED}• {label:<25} : {count}{RESET}")

    # ── Per-sensor statistics ─────────────────────────────────────────
    print(f"\n  {CYAN}Per-sensor statistics:{RESET}")
    print(f"  {'Sensor':<14} {'Min':>8}  {'Max':>8}  {'Avg':>8}  {'Readings':>9}")
    print(f"  {'-'*14}  {'-'*7}  {'-'*7}  {'-'*7}  {'-'*8}")
    for sensor, stats in sensor_stats.items():
        if stats["count"] == 0:
            print(f"  {sensor.capitalize():<14}  {'N/A':>7}  {'N/A':>7}  {'N/A':>7}  {'0':>8}")
        else:
            avg = stats["sum"] / stats["count"]
            print(
                f"  {BOLD}{sensor.capitalize():<14}{RESET}"
                f"  {stats['min']:>7.1f}"
                f"  {stats['max']:>7.1f}"
                f"  {avg:>7.1f}"
                f"  {stats['count']:>8}"
            )
        logger.info(
            "STATS | %-14s min=%.1f  max=%.1f  avg=%.1f  readings=%d",
            sensor,
            stats["min"] if stats["count"] else 0,
            stats["max"] if stats["count"] else 0,
            (stats["sum"] / stats["count"]) if stats["count"] else 0,
            stats["count"],
        )

    # ── Pass / Fail verdict ───────────────────────────────────────────
    print()
    if total_events == 0:
        print(f"  {GREEN}{BOLD}✔  RESULT : ALL CLEAR — no anomalies detected{RESET}")
    else:
        print(f"  {RED}{BOLD}✘  RESULT : {total_events} anomaly event(s) across "
              f"{anomaly_rows} row(s) — see anomalies.csv{RESET}")

    print(divider + "\n")

    # Also write the summary to the log
    logger.info(
        "SUMMARY | rows=%d | valid=%d | total_anomalies=%d | %s",
        total_rows, valid_rows, total_events,
        ", ".join(f"{s}={c}" for s, c in sorted(sensor_counts.items())),
    )


# -----------------------------------------------------------------------
# 7. MAIN VALIDATION ORCHESTRATOR
# -----------------------------------------------------------------------

def validate_sensor_data(
    csv_path: str = DEFAULT_CSV_PATH,
    thresholds_path: str = DEFAULT_THRESHOLDS_PATH,
    anomalies_path: str = DEFAULT_ANOMALIES_PATH,
    report_path: str = DEFAULT_REPORT_PATH,
    verbose: bool = False,
    quiet: bool = False,
) -> int:
    """
    Orchestrates the full validation pipeline:

      1. Load thresholds from JSON.
      2. Stream-read the CSV row by row (memory-efficient).
      3. Check each row against all thresholds.
      4. Print results in color to the terminal.
      5. Collect anomalies and save them to anomalies.csv.
      6. Print a summary report.

    Args:
        csv_path:        Path to the sensor readings CSV file.
        thresholds_path: Path to the thresholds JSON file.
        anomalies_path:  Output path for the anomalies CSV.
        verbose:         Print every row (valid + anomaly).
        quiet:           Suppress row-level output; show summary only.

    Returns:
        Exit code — 0 if no anomalies, 1 if anomalies were found.
    """
    # ── Step 1: Load thresholds ────────────────────────────────────────
    thresholds = load_thresholds(thresholds_path)

    # Validate that the CSV file exists before we start processing
    if not os.path.exists(csv_path):
        msg = f"CSV file not found: '{csv_path}'"
        print(f"{RED}[ERROR] {msg}{RESET}")
        logger.error(msg)
        sys.exit(1)

    # ── Header banner ──────────────────────────────────────────────────
    divider = CYAN + "─" * 65 + RESET
    print(f"\n{divider}")
    print(
        f"{BOLD}{CYAN}  Sensor Data Validator  "
        f"| {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}{RESET}"
    )
    print(f"{CYAN}  CSV        : {csv_path}{RESET}")
    print(f"{CYAN}  Thresholds : {thresholds_path}{RESET}")
    print(f"{divider}\n")
    logger.info("Run started  | csv=%s | thresholds=%s | output=%s",
                csv_path, thresholds_path, anomalies_path)

    # ── Stats dict (populated after processing) ─────────────────────
    stats: Dict[str, object] = {
        "total_rows":      0,
        "total_anomalies": 0,
        "sensor_stats":    {},
        "anomalies":       [],
    }

    total_rows    = 0      # Total data rows processed
    anomaly_rows  = 0      # Rows containing at least one anomaly
    all_anomalies = []     # Flat list of every anomaly event

    # Per-sensor accumulators for min / max / avg statistics.
    # Structure: { sensor_name: {"min": float, "max": float, "sum": float, "count": int} }
    sensor_stats: Dict[str, dict] = {
        s: {"min": float("inf"), "max": float("-inf"), "sum": 0.0, "count": 0}
        for s in thresholds
    }

    # ── Step 2: Stream-read the CSV (one row at a time) ────────────────
    # csv.DictReader yields each row as an ordered dict without loading
    # the entire file into memory — safe for very large files.
    with open(csv_path, "r", newline="", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file)

        for row in reader:
            total_rows += 1
            stats["total_rows"] += 1

            # ── Step 2a: Update per-sensor statistics ──────────────────
            for sensor in thresholds:
                value = parse_float(row.get(sensor), sensor)
                if value is not None:
                    acc = sensor_stats[sensor]
                    if value < acc["min"]:
                        acc["min"] = value
                    if value > acc["max"]:
                        acc["max"] = value
                    acc["sum"]   += value
                    acc["count"] += 1

                    # Initialize sensor stats
                    if sensor not in stats["sensor_stats"]:
                        stats["sensor_stats"][sensor] = {
                            "values": []
                        }

                    stats["sensor_stats"][sensor]["values"].append(value)

            # ── Step 3: Check this row against all thresholds ──────────
            row_anomalies = check_row(row, thresholds, total_rows)

            # ── Step 4: Print color-coded result ───────────────────────
            print_row_result(row, row_anomalies, total_rows, verbose=verbose, quiet=quiet)

            # Accumulate anomaly data
            if row_anomalies:
                anomaly_rows  += 1
                all_anomalies.extend(row_anomalies)
                for anomaly in row_anomalies:
                    stats["total_anomalies"] += 1
                    stats.setdefault("anomalies", []).append({
                        "timestamp": anomaly["timestamp"],
                        "sensor":    anomaly["sensor"],
                        "value":     anomaly["value"],
                        "reason":    anomaly["reason"],
                    })

    # ── Populate stats dict ──────────────────────────────────────────
    stats["total_rows"]      = total_rows
    stats["total_anomalies"] = len(all_anomalies)

    for sensor, data in stats["sensor_stats"].items():
        values = data["values"]
        stats["sensor_stats"][sensor] = {
            "min": min(values),
            "max": max(values),
            "avg": sum(values) / len(values),
        }

    # ── Step 5: Save anomalies to CSV ─────────────────────────────────
    if all_anomalies:
        save_anomalies(all_anomalies, anomalies_path)
    else:
        print(f"\n{GREEN}  No anomalies detected — all readings are within range!{RESET}")

    # ── Step 6: Print summary ─────────────────────────────────────────
    print_summary(total_rows, anomaly_rows, len(all_anomalies), all_anomalies, sensor_stats)

    # ── Step 7: Generate HTML report ─────────────────────────────────
    generate_html_report(
        report_path   = report_path,
        csv_path      = csv_path,
        thresholds    = thresholds,
        total_rows    = total_rows,
        anomaly_rows  = anomaly_rows,
        all_anomalies = all_anomalies,
        sensor_stats  = sensor_stats,
        generated_at  = datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )

    # ── Step 7b: Generate simple HTML report ─────────────────────────
    generate_simple_html_report(stats["anomalies"], stats, output_file=report_path.replace(".html", "_simple.html"))

    logger.info(
        "Run complete | rows=%d | anomaly_rows=%d | total_events=%d | exit=%d",
        total_rows, anomaly_rows, len(all_anomalies),
        1 if all_anomalies else 0,
    )
    # Return exit code: 0 = clean, 1 = anomalies detected
    return 1 if all_anomalies else 0


# -----------------------------------------------------------------------
# 8. COMMAND-LINE ENTRY POINT
# -----------------------------------------------------------------------

class _ColoredHelpFormatter(argparse.RawDescriptionHelpFormatter):
    """
    Custom formatter that injects ANSI colors into the help text so the
    --help output looks polished in any color-capable terminal.
    """
    def _format_usage(self, usage, actions, groups, prefix):
        return super()._format_usage(usage, actions, groups,
                                     f"{CYAN}{BOLD}Usage{RESET}: ")

    def start_section(self, heading):
        super().start_section(f"{YELLOW}{BOLD}{heading}{RESET}")


def _build_parser() -> argparse.ArgumentParser:
    """Construct and return the argument parser."""
    parser = argparse.ArgumentParser(
        prog="validator.py",
        formatter_class=_ColoredHelpFormatter,
        description=(
            f"{CYAN}{BOLD}Sensor Data Validator v{__version__}{RESET}\n"
            "Validate CSV sensor readings against JSON threshold rules.\n"
            "Anomalies are highlighted in red and saved to an output CSV."
        ),
        epilog=(
            f"{YELLOW}Examples:{RESET}\n"
            "  python validator.py\n"
            "  python validator.py --input data.csv --thresholds thresholds.json --output anomalies.csv\n"
            "  python validator.py --input data.csv --verbose\n"
            "  python validator.py --input data.csv --quiet\n"
        ),
        add_help=True,
    )

    # ── Input / Output arguments ─────────────────────────────────────
    io_group = parser.add_argument_group("Input / Output")
    io_group.add_argument(
        "--input",
        default=DEFAULT_CSV_PATH,
        metavar="PATH",
        help=f"Sensor readings CSV file  (default: {DEFAULT_CSV_PATH})",
    )
    io_group.add_argument(
        "--thresholds",
        default=DEFAULT_THRESHOLDS_PATH,
        metavar="PATH",
        help=f"Threshold definitions JSON (default: {DEFAULT_THRESHOLDS_PATH})",
    )
    io_group.add_argument(
        "--output",
        default=None,
        metavar="PATH",
        help="Anomalies output CSV file  (default: anomalies_YYYYMMDD_HHMMSS.csv)",
    )
    io_group.add_argument(
        "--report",
        default=None,
        metavar="PATH",
        help="HTML report output file    (default: report_YYYYMMDD_HHMMSS.html)",
    )

    # ── Output verbosity ─────────────────────────────────────────────
    verbosity = parser.add_mutually_exclusive_group()
    verbosity.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Print every row (valid and anomaly)",
    )
    verbosity.add_argument(
        "--quiet", "-q",
        action="store_true",
        help="Suppress row-level output; show summary only",
    )

    # ── Logging ──────────────────────────────────────────────────
    log_group = parser.add_argument_group("Logging")
    log_group.add_argument(
        "--log",
        default=DEFAULT_LOG_PATH,
        metavar="PATH",
        help=f"Path to the log file  (default: {DEFAULT_LOG_PATH})",
    )
    log_group.add_argument(
        "--log-level",
        default="INFO",
        metavar="LEVEL",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Log verbosity for the log file (default: INFO)",
    )

    # ── Meta ─────────────────────────────────────────────────────────
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )

    # ── Alerts ──────────────────────────────────────────────────
    alert_group = parser.add_argument_group("Alerts")
    alert_group.add_argument(
        "--alert-threshold",
        type=int,
        default=1,
        metavar="N",
        help="Send alerts when anomaly count ≥ N  (default: 1, -1 = disabled)",
    )
    alert_group.add_argument(
        "--alert-email",
        metavar="ADDRESS",
        nargs="+",
        default=[],
        help="One or more recipient email addresses",
    )
    alert_group.add_argument(
        "--smtp-host",   metavar="HOST",  default=os.environ.get("SMTP_HOST"),
        help="SMTP server hostname  (or set SMTP_HOST env var)",
    )
    alert_group.add_argument(
        "--smtp-port",   metavar="PORT",  type=int, default=587,
        help="SMTP port (default: 587 STARTTLS)",
    )
    alert_group.add_argument(
        "--smtp-user",   metavar="USER",  default=os.environ.get("SMTP_USER"),
        help="SMTP login user  (or set SMTP_USER env var)",
    )
    alert_group.add_argument(
        "--smtp-pass",   metavar="PASS",  default=os.environ.get("SMTP_PASS"),
        help="SMTP password    (or set SMTP_PASS env var)",
    )
    alert_group.add_argument(
        "--webhook-url",  metavar="URL", default=os.environ.get("ALERT_WEBHOOK_URL"),
        help="Slack / Teams / Discord / generic webhook URL  (or set ALERT_WEBHOOK_URL)",
    )
    alert_group.add_argument(
        "--webhook-type", metavar="TYPE", default="slack",
        choices=["slack", "teams", "discord", "generic"],
        help="Webhook payload format (default: slack)",
    )

    return parser


def _validate_args(args: argparse.Namespace) -> None:
    """
    Pre-flight checks: abort early with a clear message if required
    input files are missing, before any processing begins.
    """
    errors = []
    if not os.path.exists(args.input):
        errors.append(f"  --input       '{args.input}' not found")
    if not os.path.exists(args.thresholds):
        errors.append(f"  --thresholds  '{args.thresholds}' not found")
    if errors:
        print(f"\n{RED}{BOLD}[ERROR] The following required files were not found:{RESET}")
        for e in errors:
            print(f"{RED}{e}{RESET}")
        print(f"\n{YELLOW}Tip: run with --help to see all options.{RESET}\n")
        sys.exit(2)


def main() -> None:
    """Parse arguments, validate inputs, and run the validator."""
    # Enable ANSI colors on Windows before any colored output
    if sys.platform == "win32":
        os.system("")   # Activates VT100 escape codes on Windows Console

    parser = _build_parser()
    args   = parser.parse_args()

    # Pre-flight file existence checks
    _validate_args(args)

    # Generate timestamped output filenames when not explicitly provided.
    # This ensures each run creates new files instead of overwriting.
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    anomalies_path = args.output if args.output else f"anomalies_{ts}.csv"
    report_path    = args.report if args.report else f"report_{ts}.html"

    # Initialise logging before any processing
    setup_logging(log_path=args.log, level=args.log_level)

    exit_code = validate_sensor_data(
        csv_path        = args.input,
        thresholds_path = args.thresholds,
        anomalies_path  = anomalies_path,
        report_path     = report_path,
        verbose         = args.verbose,
        quiet           = args.quiet,
    )

    # ── Fire alerts if threshold exceeded ────────────────────────────────
    # Build AlertConfig from CLI args, falling back to env vars for
    # any credential not explicitly provided on the command line.
    env_cfg = config_from_env()   # reads SMTP_*/ALERT_* env vars
    alert_cfg = AlertConfig(
        threshold    = args.alert_threshold,
        email_to     = args.alert_email or env_cfg.email_to,
        smtp_host    = args.smtp_host    or env_cfg.smtp_host,
        smtp_port    = args.smtp_port,
        smtp_user    = args.smtp_user    or env_cfg.smtp_user,
        smtp_pass    = args.smtp_pass    or env_cfg.smtp_pass,
        webhook_url  = args.webhook_url  or env_cfg.webhook_url,
        webhook_type = args.webhook_type or env_cfg.webhook_type,
    )

    # Read anomaly stats from the CSV we just generated
    import csv as _csv
    anomaly_count  = 0
    sensor_counts: Dict[str, int] = {}
    if os.path.exists(anomalies_path):
        with open(anomalies_path, newline="", encoding="utf-8") as f:
            for row in _csv.DictReader(f):
                anomaly_count += 1
                s = row.get("sensor", "unknown")
                sensor_counts[s] = sensor_counts.get(s, 0) + 1

    send_alerts(
        cfg           = alert_cfg,
        total_rows    = 0,          # summary totals not re-read here; threshold check is on anomaly count
        anomaly_count = anomaly_count,
        sensor_counts = sensor_counts,
        csv_path      = args.input,
        report_url    = None,       # local file path — no public URL in CLI mode
        generated_at  = datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
