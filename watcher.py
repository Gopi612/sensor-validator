#!/usr/bin/env python3
"""
=======================================================================
  watcher.py  —  Folder Watch Mode for Sensor Data Validator
=======================================================================
  Drop any CSV into the watch folder and it is automatically validated.
  Results (anomalies CSV + HTML report) land in the output folder.

  Uses only Python stdlib — no watchdog or inotify dependencies.
  Polling interval is configurable (default: 3 seconds).

  Usage:
      python watcher.py
      python watcher.py --watch ./inbox --output ./results --interval 5

  Stop with:   Ctrl+C
=======================================================================
"""

import argparse
import os
import shutil
import sys
import time
from datetime import datetime

# Re-use all validation logic from the main module
from sensor_validator import (
    DEFAULT_THRESHOLDS_PATH,
    __version__,
    setup_logging,
    validate_sensor_data,
)

# -----------------------------------------------------------------------
# Defaults
# -----------------------------------------------------------------------
DEFAULT_WATCH_DIR    = "inbox"           # Drop CSV files here
DEFAULT_OUTPUT_DIR   = "results"         # Anomaly CSV + HTML land here
DEFAULT_INTERVAL     = 3                 # Poll every N seconds
DEFAULT_ARCHIVE_DIR  = "processed"       # Processed CSVs are moved here

# -----------------------------------------------------------------------
# ANSI colours (reused from sensor_validator palette)
# -----------------------------------------------------------------------
GREEN  = "\033[92m"
CYAN   = "\033[96m"
YELLOW = "\033[93m"
RED    = "\033[91m"
BOLD   = "\033[1m"
RESET  = "\033[0m"


# -----------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------

def _ts() -> str:
    """Return a compact timestamp string for console output."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _banner(watch_dir: str, output_dir: str, interval: int,
            thresholds: str, archive_dir: str) -> None:
    """Print the startup banner."""
    divider = CYAN + "─" * 65 + RESET
    print(f"\n{divider}")
    print(f"{BOLD}{CYAN}  Sensor Validator — Folder Watch Mode  v{__version__}{RESET}")
    print(divider)
    print(f"{CYAN}  Watching   : {os.path.abspath(watch_dir)}{RESET}")
    print(f"{CYAN}  Output     : {os.path.abspath(output_dir)}{RESET}")
    print(f"{CYAN}  Archive    : {os.path.abspath(archive_dir)}{RESET}")
    print(f"{CYAN}  Thresholds : {os.path.abspath(thresholds)}{RESET}")
    print(f"{CYAN}  Poll every : {interval}s{RESET}")
    print(divider)
    print(f"{YELLOW}  Drop a CSV into the watch folder to trigger validation.{RESET}")
    print(f"{YELLOW}  Press Ctrl+C to stop.{RESET}\n")


def _ensure_dirs(*dirs: str) -> None:
    """Create directories if they don't already exist."""
    for d in dirs:
        os.makedirs(d, exist_ok=True)


# -----------------------------------------------------------------------
# Process a single CSV file
# -----------------------------------------------------------------------

def process_file(
    csv_path: str,
    thresholds_path: str,
    output_dir: str,
    archive_dir: str,
) -> None:
    """
    Validate *csv_path* and write results to *output_dir*.

    Outputs produced for each file  ``<stem>.<ext>``:
      - ``<output_dir>/<stem>_anomalies.csv``
      - ``<output_dir>/<stem>_report.html``

    After processing the source CSV is moved to *archive_dir* so it
    won't be picked up again on the next poll cycle.

    Args:
        csv_path:        Absolute path to the incoming CSV file.
        thresholds_path: Path to the thresholds JSON file.
        output_dir:      Directory for anomalies CSV + HTML report.
        archive_dir:     Directory to move the processed CSV into.
    """
    stem = os.path.splitext(os.path.basename(csv_path))[0]
    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    anomalies_path = os.path.join(output_dir, f"{stem}_anomalies_{ts}.csv")
    report_path    = os.path.join(output_dir, f"{stem}_report_{ts}.html")

    print(f"\n{CYAN}[{_ts()}] New file detected: {os.path.basename(csv_path)}{RESET}")

    try:
        exit_code = validate_sensor_data(
            csv_path        = csv_path,
            thresholds_path = thresholds_path,
            anomalies_path  = anomalies_path,
            report_path     = report_path,
            verbose         = False,
            quiet           = True,   # Summary only — keeps watch output readable
        )

        verdict = (
            f"{RED}✘  Anomalies found — check {anomalies_path}{RESET}"
            if exit_code == 1
            else f"{GREEN}✔  All clear — no anomalies{RESET}"
        )
        print(f"  {verdict}")
        print(f"  {CYAN}Report  → {report_path}{RESET}")

    except Exception as exc:  # noqa: BLE001 — surface any unexpected error
        print(f"  {RED}[ERROR] Failed to process '{csv_path}': {exc}{RESET}")
        return

    # ── Archive the processed file ────────────────────────────────────
    archive_path = os.path.join(archive_dir, os.path.basename(csv_path))
    # If a file with the same name already exists in the archive,
    # prefix with a timestamp to avoid clobbering.
    if os.path.exists(archive_path):
        ts_tag       = datetime.now().strftime("%Y%m%d_%H%M%S")
        archive_path = os.path.join(
            archive_dir, f"{stem}_{ts_tag}.csv"
        )
    shutil.move(csv_path, archive_path)
    print(f"  {YELLOW}Archived → {archive_path}{RESET}")


# -----------------------------------------------------------------------
# Watch loop
# -----------------------------------------------------------------------

def watch(
    watch_dir: str,
    thresholds_path: str,
    output_dir: str,
    archive_dir: str,
    interval: int,
) -> None:
    """
    Poll *watch_dir* every *interval* seconds for new ``.csv`` files.

    Any ``.csv`` found is immediately processed and then moved to
    *archive_dir*, so it is not processed twice.

    Args:
        watch_dir:       Directory to monitor for incoming CSV files.
        thresholds_path: Path to the thresholds JSON.
        output_dir:      Where to write anomaly CSV and HTML report.
        archive_dir:     Where to move processed CSV files.
        interval:        Poll frequency in seconds.
    """
    _ensure_dirs(watch_dir, output_dir, archive_dir)
    _banner(watch_dir, output_dir, interval, thresholds_path, archive_dir)

    print(f"{CYAN}[{_ts()}] Watcher started — waiting for files...{RESET}")

    try:
        while True:
            # Find all CSV files currently in the watch folder
            try:
                entries = [
                    os.path.join(watch_dir, f)
                    for f in os.listdir(watch_dir)
                    if f.lower().endswith(".csv")
                ]
            except PermissionError as exc:
                print(f"{RED}[{_ts()}] Cannot read watch dir: {exc}{RESET}")
                entries = []

            if entries:
                for csv_path in sorted(entries):
                    # Small delay — ensure the file is fully written
                    # before we open it (guards against partial writes).
                    time.sleep(0.5)
                    if os.path.exists(csv_path):   # may have vanished
                        process_file(
                            csv_path        = csv_path,
                            thresholds_path = thresholds_path,
                            output_dir      = output_dir,
                            archive_dir     = archive_dir,
                        )
            else:
                # Print a heartbeat every ~30 s so the user knows
                # the watcher is still alive.
                now = time.time()
                if not hasattr(watch, "_last_heartbeat") or \
                        now - watch._last_heartbeat >= 30:
                    print(f"{YELLOW}[{_ts()}] Watching... (no new files){RESET}")
                    watch._last_heartbeat = now

            time.sleep(interval)

    except KeyboardInterrupt:
        print(f"\n{YELLOW}[{_ts()}] Watcher stopped by user (Ctrl+C).{RESET}\n")
        sys.exit(0)


# -----------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------

def main() -> None:
    """Parse arguments and start the folder watcher."""
    if sys.platform == "win32":
        os.system("")   # Enable ANSI colour codes on Windows

    parser = argparse.ArgumentParser(
        prog="watcher.py",
        description=(
            f"{CYAN}{BOLD}Sensor Validator — Folder Watch Mode  v{__version__}{RESET}\n"
            "Drop a CSV into the watch folder to trigger automatic validation.\n"
            "Results (anomalies CSV + HTML report) are written to the output folder."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            f"{YELLOW}Examples:{RESET}\n"
            "  python watcher.py\n"
            "  python watcher.py --watch ./inbox --output ./results\n"
            "  python watcher.py --watch ./inbox --interval 10 --thresholds custom.json\n"
        ),
    )

    parser.add_argument(
        "--watch", "-w",
        default=DEFAULT_WATCH_DIR,
        metavar="DIR",
        help=f"Folder to monitor for incoming CSV files (default: {DEFAULT_WATCH_DIR})",
    )
    parser.add_argument(
        "--output", "-o",
        default=DEFAULT_OUTPUT_DIR,
        metavar="DIR",
        help=f"Folder for anomaly CSV + HTML report output (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--archive", "-a",
        default=DEFAULT_ARCHIVE_DIR,
        metavar="DIR",
        help=f"Folder for processed (archived) CSV files (default: {DEFAULT_ARCHIVE_DIR})",
    )
    parser.add_argument(
        "--thresholds", "-t",
        default=DEFAULT_THRESHOLDS_PATH,
        metavar="PATH",
        help=f"Threshold definitions JSON file (default: {DEFAULT_THRESHOLDS_PATH})",
    )
    parser.add_argument(
        "--interval", "-i",
        default=DEFAULT_INTERVAL,
        type=int,
        metavar="SECS",
        help=f"Poll interval in seconds (default: {DEFAULT_INTERVAL})",
    )
    parser.add_argument(
        "--log",
        default="watcher.log",
        metavar="PATH",
        help="Log file path (default: watcher.log)",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )

    args = parser.parse_args()

    # Validate thresholds file exists before starting the loop
    if not os.path.exists(args.thresholds):
        print(f"\n{RED}[ERROR] Thresholds file not found: '{args.thresholds}'{RESET}")
        print(f"{YELLOW}Tip: run with --help to see all options.{RESET}\n")
        sys.exit(2)

    setup_logging(log_path=args.log, level="INFO")

    watch(
        watch_dir       = args.watch,
        thresholds_path = args.thresholds,
        output_dir      = args.output,
        archive_dir     = args.archive,
        interval        = args.interval,
    )


if __name__ == "__main__":
    main()
