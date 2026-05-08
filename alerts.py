#!/usr/bin/env python3
"""
=======================================================================
  alerts.py  —  Alert Engine for Sensor Data Validator
=======================================================================
  Sends notifications when anomaly count exceeds a configured threshold.

  Supported channels:
    1. Email  — via SMTP (Gmail, Outlook, any SMTP server)
    2. Slack  — via Incoming Webhook URL
    3. Generic Webhook — HTTP POST with JSON payload (Teams, Discord,
                         PagerDuty, custom endpoints, etc.)

  Uses only Python stdlib:
    smtplib, email, urllib.request — no third-party packages needed.

  Usage (standalone):
      python alerts.py --test-email   --to you@example.com
      python alerts.py --test-webhook --webhook-url https://hooks.slack.com/...

  Typical integration:
      from alerts import AlertConfig, send_alerts

      cfg = AlertConfig(
          threshold    = 5,
          email_to     = ["ops@example.com"],
          smtp_host    = "smtp.gmail.com",
          smtp_port    = 587,
          smtp_user    = "sender@gmail.com",
          smtp_pass    = "app-password",
          webhook_url  = "https://hooks.slack.com/services/...",
      )
      send_alerts(cfg, total_rows=1000, anomaly_count=45,
                  sensor_counts={"temperature": 20}, csv_path="data.csv",
                  report_url="http://localhost:8000/report/20260507_180000")
=======================================================================
"""

import argparse
import json
import os
import smtplib
import ssl
import sys
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Dict, List, Optional

# -----------------------------------------------------------------------
# Configuration dataclass
# -----------------------------------------------------------------------

@dataclass
class AlertConfig:
    """
    All settings needed to fire alerts.

    Only `threshold` is mandatory. Omit email / webhook fields to skip
    those channels.

    Attributes:
        threshold:    Minimum anomaly count that triggers an alert.
                      Set to 0 to always alert, -1 to never alert.
        email_to:     List of recipient email addresses.
        smtp_host:    SMTP server hostname  (e.g. smtp.gmail.com).
        smtp_port:    SMTP port — 587 (STARTTLS) or 465 (SSL).
        smtp_user:    SMTP login username.
        smtp_pass:    SMTP login password / app-password.
        smtp_from:    Sender address (defaults to smtp_user).
        smtp_tls:     Use STARTTLS when True (default). Set False for
                      servers that use implicit SSL on port 465.
        webhook_url:  Slack Incoming Webhook or any HTTP POST endpoint.
        webhook_type: "slack" | "teams" | "discord" | "generic"
                      Controls the JSON payload shape.
        timeout:      HTTP / SMTP connection timeout in seconds.
    """
    threshold:    int              = 1
    email_to:     List[str]        = field(default_factory=list)
    smtp_host:    Optional[str]    = None
    smtp_port:    int              = 587
    smtp_user:    Optional[str]    = None
    smtp_pass:    Optional[str]    = None
    smtp_from:    Optional[str]    = None
    smtp_tls:     bool             = True
    webhook_url:  Optional[str]    = None
    webhook_type: str              = "slack"
    timeout:      int              = 10


# -----------------------------------------------------------------------
# Message builder
# -----------------------------------------------------------------------

def _build_message(
    total_rows: int,
    anomaly_count: int,
    sensor_counts: Dict[str, int],
    csv_path: str,
    report_url: Optional[str] = None,
    generated_at: Optional[str] = None,
) -> dict:
    """
    Build a structured alert message dict used by all channels.

    Returns:
        dict with keys: subject, text, html, slack_blocks, teams_card,
                        discord_embed, generic_payload
    """
    ts          = generated_at or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    pass_rate   = round((total_rows - anomaly_count) / total_rows * 100, 1) if total_rows else 0
    sensor_lines = "\n".join(
        f"  • {sensor.capitalize():<18}: {count} event(s)"
        for sensor, count in sorted(sensor_counts.items(), key=lambda x: -x[1])
    )
    report_line = f"\nReport URL  : {report_url}" if report_url else ""

    subject = (
        f"🚨 Sensor Alert — {anomaly_count} anomalies detected "
        f"({pass_rate}% pass rate)"
    )

    text = f"""\
Sensor Data Validator — ANOMALY ALERT
======================================
Timestamp   : {ts}
Source CSV  : {csv_path}

Summary
-------
Total rows  : {total_rows:,}
Anomalies   : {anomaly_count:,}
Pass rate   : {pass_rate}%

Breakdown by sensor
-------------------
{sensor_lines}{report_line}

This alert was generated automatically by Sensor Data Validator.
"""

    html = f"""\
<!DOCTYPE html><html><head>
<style>
  body {{ font-family: 'Segoe UI', system-ui, sans-serif; background:#f8fafc; color:#1e293b; padding:2rem; }}
  .card {{ background:#fff; border-radius:.75rem; padding:1.5rem 2rem;
           box-shadow:0 2px 12px #0001; max-width:560px; margin:auto; }}
  h2   {{ color:#ef4444; margin-bottom:.25rem; }}
  .meta {{ color:#64748b; font-size:.85rem; margin-bottom:1.5rem; }}
  table {{ width:100%; border-collapse:collapse; margin:1rem 0; }}
  th {{ background:#f1f5f9; text-align:left; padding:.5rem .75rem;
        font-size:.78rem; text-transform:uppercase; letter-spacing:.05em; color:#64748b; }}
  td {{ padding:.5rem .75rem; border-bottom:1px solid #f1f5f9; }}
  .kpi-grid {{ display:grid; grid-template-columns:repeat(3,1fr); gap:.75rem; margin:1rem 0; }}
  .kpi {{ background:#f8fafc; border-radius:.5rem; padding:.75rem; border-left:3px solid; text-align:center; }}
  .kpi.total {{ border-color:#3b82f6; }}
  .kpi.valid {{ border-color:#22c55e; }}
  .kpi.anom  {{ border-color:#ef4444; }}
  .kpi-val   {{ font-size:1.6rem; font-weight:800; }}
  .kpi.total .kpi-val {{ color:#3b82f6; }}
  .kpi.valid .kpi-val {{ color:#22c55e; }}
  .kpi.anom  .kpi-val {{ color:#ef4444; }}
  .kpi-label {{ font-size:.7rem; color:#94a3b8; text-transform:uppercase; }}
  .btn {{ display:inline-block; padding:.65rem 1.25rem; background:#3b82f6; color:#fff !important;
          border-radius:.5rem; text-decoration:none; font-weight:700; font-size:.9rem; margin-top:1rem; }}
  footer {{ margin-top:1.5rem; font-size:.75rem; color:#94a3b8; text-align:center; }}
</style></head><body>
<div class="card">
  <h2>🚨 Sensor Anomaly Alert</h2>
  <p class="meta">Generated {ts} &nbsp;|&nbsp; Source: <code>{csv_path}</code></p>

  <div class="kpi-grid">
    <div class="kpi total"><div class="kpi-val">{total_rows:,}</div>
      <div class="kpi-label">Total Rows</div></div>
    <div class="kpi valid"><div class="kpi-val">{total_rows - anomaly_count:,}</div>
      <div class="kpi-label">Valid</div></div>
    <div class="kpi anom"><div class="kpi-val">{anomaly_count:,}</div>
      <div class="kpi-label">Anomalies</div></div>
  </div>

  <table>
    <tr><th>Sensor</th><th>Anomaly Events</th></tr>
    {''.join(f'<tr><td><strong>{s.capitalize()}</strong></td><td>{c}</td></tr>'
             for s, c in sorted(sensor_counts.items(), key=lambda x: -x[1]))}
  </table>

  {f'<a class="btn" href="{report_url}">View Full Report →</a>' if report_url else ''}

  <footer>Sent by Sensor Data Validator — automatic alert</footer>
</div></body></html>
"""

    # Slack blocks payload
    sensor_text = "\n".join(
        f"• *{s.capitalize()}*: {c} event(s)"
        for s, c in sorted(sensor_counts.items(), key=lambda x: -x[1])
    )
    slack_blocks = {
        "blocks": [
            {"type": "header", "text": {"type": "plain_text",
             "text": f"🚨 Sensor Alert — {anomaly_count} anomalies detected"}},
            {"type": "section", "fields": [
                {"type": "mrkdwn", "text": f"*Total rows:*\n{total_rows:,}"},
                {"type": "mrkdwn", "text": f"*Anomaly events:*\n{anomaly_count:,}"},
                {"type": "mrkdwn", "text": f"*Pass rate:*\n{pass_rate}%"},
                {"type": "mrkdwn", "text": f"*Timestamp:*\n{ts}"},
            ]},
            {"type": "section", "text": {"type": "mrkdwn",
             "text": f"*Breakdown by sensor:*\n{sensor_text}"}},
            *(
                [{"type": "actions", "elements": [{"type": "button",
                   "text": {"type": "plain_text", "text": "View Report"},
                   "url": report_url, "style": "primary"}]}]
                if report_url else []
            ),
            {"type": "divider"},
        ]
    }

    # Microsoft Teams Adaptive Card
    teams_card = {
        "type": "message",
        "attachments": [{
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "type": "AdaptiveCard", "version": "1.4",
                "body": [
                    {"type": "TextBlock", "size": "Large", "weight": "Bolder",
                     "text": f"🚨 Sensor Alert — {anomaly_count} anomalies", "color": "attention"},
                    {"type": "FactSet", "facts": [
                        {"title": "Total rows",     "value": str(total_rows)},
                        {"title": "Anomalies",      "value": str(anomaly_count)},
                        {"title": "Pass rate",      "value": f"{pass_rate}%"},
                        {"title": "Timestamp",      "value": ts},
                        {"title": "Source CSV",     "value": csv_path},
                    ]},
                    {"type": "TextBlock", "weight": "Bolder", "text": "Breakdown by sensor"},
                    {"type": "TextBlock", "text": sensor_text, "wrap": True},
                ],
                "actions": ([{"type": "Action.OpenUrl", "title": "View Report",
                               "url": report_url}] if report_url else []),
            }
        }]
    }

    # Discord embed
    discord_embed = {
        "embeds": [{
            "title":       f"🚨 Sensor Alert — {anomaly_count} anomalies detected",
            "color":       0xEF4444,
            "description": f"**Pass rate:** {pass_rate}%\n**Source:** `{csv_path}`",
            "fields":      [{"name": s.capitalize(), "value": f"{c} event(s)", "inline": True}
                            for s, c in sorted(sensor_counts.items(), key=lambda x: -x[1])],
            "footer":      {"text": f"Sensor Data Validator • {ts}"},
            **({"url": report_url} if report_url else {}),
        }]
    }

    # Generic JSON payload (for custom webhooks, PagerDuty, etc.)
    generic_payload = {
        "event":          "sensor_anomaly_alert",
        "timestamp":      ts,
        "csv_path":       csv_path,
        "total_rows":     total_rows,
        "anomaly_count":  anomaly_count,
        "pass_rate_pct":  pass_rate,
        "sensor_counts":  sensor_counts,
        "report_url":     report_url,
    }

    return {
        "subject":         subject,
        "text":            text,
        "html":            html,
        "slack_blocks":    slack_blocks,
        "teams_card":      teams_card,
        "discord_embed":   discord_embed,
        "generic_payload": generic_payload,
    }


# -----------------------------------------------------------------------
# Email sender
# -----------------------------------------------------------------------

def send_email_alert(
    cfg: AlertConfig,
    msg: dict,
) -> bool:
    """
    Send an HTML + plain-text email via SMTP.

    Supports:
      - Gmail  (smtp.gmail.com:587 STARTTLS, use App Password)
      - Outlook (smtp.office365.com:587 STARTTLS)
      - Any standard SMTP server

    Args:
        cfg: AlertConfig with SMTP settings populated.
        msg: Message dict from _build_message().

    Returns:
        True on success, False on failure (error is printed, not raised).
    """
    if not cfg.email_to or not cfg.smtp_host or not cfg.smtp_user or not cfg.smtp_pass:
        print("[alerts] Email skipped — incomplete SMTP config "
              "(need smtp_host, smtp_user, smtp_pass, email_to).")
        return False

    sender = cfg.smtp_from or cfg.smtp_user

    # Build MIME message with both plain-text and HTML parts
    mime = MIMEMultipart("alternative")
    mime["Subject"] = msg["subject"]
    mime["From"]    = sender
    mime["To"]      = ", ".join(cfg.email_to)
    mime.attach(MIMEText(msg["text"], "plain", "utf-8"))
    mime.attach(MIMEText(msg["html"], "html",  "utf-8"))

    try:
        context = ssl.create_default_context()
        if cfg.smtp_tls:
            # STARTTLS (port 587) — most common
            with smtplib.SMTP(cfg.smtp_host, cfg.smtp_port,
                              timeout=cfg.timeout) as server:
                server.ehlo()
                server.starttls(context=context)
                server.login(cfg.smtp_user, cfg.smtp_pass)
                server.sendmail(sender, cfg.email_to, mime.as_bytes())
        else:
            # Implicit SSL (port 465)
            with smtplib.SMTP_SSL(cfg.smtp_host, cfg.smtp_port,
                                  context=context,
                                  timeout=cfg.timeout) as server:
                server.login(cfg.smtp_user, cfg.smtp_pass)
                server.sendmail(sender, cfg.email_to, mime.as_bytes())

        print(f"[alerts] ✔  Email sent → {', '.join(cfg.email_to)}")
        return True

    except smtplib.SMTPAuthenticationError:
        print("[alerts] ✘  Email failed — authentication error. "
              "Check smtp_user / smtp_pass (Gmail requires an App Password).")
    except smtplib.SMTPException as exc:
        print(f"[alerts] ✘  Email SMTP error: {exc}")
    except OSError as exc:
        print(f"[alerts] ✘  Email connection error: {exc}")

    return False


# -----------------------------------------------------------------------
# Webhook sender  (Slack / Teams / Discord / generic)
# -----------------------------------------------------------------------

def send_webhook_alert(
    cfg: AlertConfig,
    msg: dict,
) -> bool:
    """
    POST a JSON payload to a webhook URL.

    Automatically selects the correct payload shape based on cfg.webhook_type:
      "slack"   → Slack Block Kit  (Incoming Webhook)
      "teams"   → MS Teams Adaptive Card
      "discord" → Discord embed
      "generic" → flat JSON dict

    Args:
        cfg: AlertConfig with webhook_url (and optionally webhook_type).
        msg: Message dict from _build_message().

    Returns:
        True on success (HTTP 2xx), False otherwise.
    """
    if not cfg.webhook_url:
        print("[alerts] Webhook skipped — no webhook_url configured.")
        return False

    payload_map = {
        "slack":   msg["slack_blocks"],
        "teams":   msg["teams_card"],
        "discord": msg["discord_embed"],
        "generic": msg["generic_payload"],
    }
    payload = payload_map.get(cfg.webhook_type, msg["generic_payload"])

    try:
        data    = json.dumps(payload).encode("utf-8")
        request = urllib.request.Request(
            cfg.webhook_url,
            data    = data,
            headers = {
                "Content-Type":   "application/json",
                "Content-Length": str(len(data)),
                "User-Agent":     "SensorDataValidator/1.0",
            },
            method  = "POST",
        )
        with urllib.request.urlopen(request, timeout=cfg.timeout) as resp:
            status = resp.getcode()

        if 200 <= status < 300:
            print(f"[alerts] ✔  Webhook ({cfg.webhook_type}) sent → HTTP {status}")
            return True
        else:
            print(f"[alerts] ✘  Webhook returned HTTP {status}")
            return False

    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")[:200]
        print(f"[alerts] ✘  Webhook HTTP error {exc.code}: {body}")
    except urllib.error.URLError as exc:
        print(f"[alerts] ✘  Webhook connection error: {exc.reason}")
    except Exception as exc:  # noqa: BLE001
        print(f"[alerts] ✘  Webhook unexpected error: {exc}")

    return False


# -----------------------------------------------------------------------
# Main public interface
# -----------------------------------------------------------------------

def send_alerts(
    cfg: AlertConfig,
    total_rows: int,
    anomaly_count: int,
    sensor_counts: Dict[str, int],
    csv_path: str,
    report_url: Optional[str] = None,
    generated_at: Optional[str] = None,
) -> dict:
    """
    Fire all configured alert channels if anomaly_count >= cfg.threshold.

    Args:
        cfg:           AlertConfig instance.
        total_rows:    Total rows processed.
        anomaly_count: Total anomaly events detected.
        sensor_counts: Per-sensor anomaly counts dict.
        csv_path:      Source CSV path (for message context).
        report_url:    Optional URL to the HTML report.
        generated_at:  Optional timestamp string.

    Returns:
        dict with keys "triggered", "email_sent", "webhook_sent"
    """
    result = {"triggered": False, "email_sent": False, "webhook_sent": False}

    # Check threshold
    if cfg.threshold < 0:
        print("[alerts] Alerts disabled (threshold=-1).")
        return result

    if anomaly_count < cfg.threshold:
        print(f"[alerts] No alert — anomaly count ({anomaly_count}) "
              f"is below threshold ({cfg.threshold}).")
        return result

    result["triggered"] = True
    print(f"\n[alerts] 🚨 Threshold exceeded ({anomaly_count} >= {cfg.threshold}) "
          f"— firing alerts...")

    msg = _build_message(
        total_rows    = total_rows,
        anomaly_count = anomaly_count,
        sensor_counts = sensor_counts,
        csv_path      = csv_path,
        report_url    = report_url,
        generated_at  = generated_at,
    )

    # Fire all configured channels
    if cfg.email_to and cfg.smtp_host:
        result["email_sent"] = send_email_alert(cfg, msg)

    if cfg.webhook_url:
        result["webhook_sent"] = send_webhook_alert(cfg, msg)

    return result


# -----------------------------------------------------------------------
# AlertConfig factory from environment variables
# -----------------------------------------------------------------------

def config_from_env() -> AlertConfig:
    """
    Build an AlertConfig by reading environment variables.

    This is the recommended approach for production — keeps credentials
    out of command-line history and source code.

    Environment variables:
        ALERT_THRESHOLD       int   (default: 1)
        ALERT_EMAIL_TO        comma-separated addresses
        SMTP_HOST
        SMTP_PORT             int   (default: 587)
        SMTP_USER
        SMTP_PASS
        SMTP_FROM
        SMTP_TLS              "true" | "false"  (default: true)
        ALERT_WEBHOOK_URL
        ALERT_WEBHOOK_TYPE    slack | teams | discord | generic
    """
    return AlertConfig(
        threshold    = int(os.environ.get("ALERT_THRESHOLD", "1")),
        email_to     = [e.strip() for e in
                        os.environ.get("ALERT_EMAIL_TO", "").split(",")
                        if e.strip()],
        smtp_host    = os.environ.get("SMTP_HOST"),
        smtp_port    = int(os.environ.get("SMTP_PORT", "587")),
        smtp_user    = os.environ.get("SMTP_USER"),
        smtp_pass    = os.environ.get("SMTP_PASS"),
        smtp_from    = os.environ.get("SMTP_FROM"),
        smtp_tls     = os.environ.get("SMTP_TLS", "true").lower() == "true",
        webhook_url  = os.environ.get("ALERT_WEBHOOK_URL"),
        webhook_type = os.environ.get("ALERT_WEBHOOK_TYPE", "slack"),
    )


# -----------------------------------------------------------------------
# CLI test mode — fire a test alert without running the full validator
# -----------------------------------------------------------------------

def _cli_test(args: argparse.Namespace) -> None:
    """Send a test alert with dummy data to verify configuration."""
    cfg = AlertConfig(
        threshold    = 0,   # Always trigger in test mode
        email_to     = [args.to] if args.to else [],
        smtp_host    = args.smtp_host,
        smtp_port    = args.smtp_port,
        smtp_user    = args.smtp_user,
        smtp_pass    = args.smtp_pass,
        smtp_tls     = not args.ssl,
        webhook_url  = args.webhook_url,
        webhook_type = args.webhook_type,
    )

    print("\n[alerts] Sending test alert with dummy data...")
    result = send_alerts(
        cfg           = cfg,
        total_rows    = 1000,
        anomaly_count = 45,
        sensor_counts = {"temperature": 20, "pressure": 8, "humidity": 7,
                         "voltage": 6, "vibration": 4},
        csv_path      = "test_data.csv",
        report_url    = args.report_url,
        generated_at  = datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )

    print("\n[alerts] Test result:", result)
    sys.exit(0 if any([result["email_sent"], result["webhook_sent"]]) else 1)


def main() -> None:
    """Standalone CLI for testing alert configuration."""
    parser = argparse.ArgumentParser(
        prog="alerts.py",
        description="Test alert configuration for Sensor Data Validator.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--to",           metavar="EMAIL",  help="Recipient email address")
    parser.add_argument("--smtp-host",    metavar="HOST",   default=os.environ.get("SMTP_HOST"),
                        help="SMTP server host (or set SMTP_HOST env var)")
    parser.add_argument("--smtp-port",    metavar="PORT",   type=int, default=587)
    parser.add_argument("--smtp-user",    metavar="USER",   default=os.environ.get("SMTP_USER"))
    parser.add_argument("--smtp-pass",    metavar="PASS",   default=os.environ.get("SMTP_PASS"))
    parser.add_argument("--ssl",          action="store_true",
                        help="Use implicit SSL (port 465) instead of STARTTLS")
    parser.add_argument("--webhook-url",  metavar="URL",    default=os.environ.get("ALERT_WEBHOOK_URL"),
                        help="Webhook URL (Slack, Teams, Discord, or generic)")
    parser.add_argument("--webhook-type", metavar="TYPE",   default="slack",
                        choices=["slack", "teams", "discord", "generic"])
    parser.add_argument("--report-url",   metavar="URL",    default=None,
                        help="Optional report URL to include in the alert")

    args = parser.parse_args()

    if not args.to and not args.webhook_url:
        parser.error("Provide at least --to (email) or --webhook-url to test.")

    _cli_test(args)


if __name__ == "__main__":
    main()
