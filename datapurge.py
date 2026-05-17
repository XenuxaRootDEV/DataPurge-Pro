#!/usr/bin/env python3
"""
DataPurge Pro — Structured JSON & CSV Validator
High-performance streaming validator with Elite Rich TUI.
"""

from __future__ import annotations

import csv
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Tuple

# ── Rich imports ──────────────────────────────────────────────────────────────
from rich import box
from rich.columns import Columns
from rich.console import Console
from rich.live import Live
from rich.markup import escape
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TransferSpeedColumn,
)
from rich.rule import Rule
from rich.table import Table
from rich.text import Text
from rich.theme import Theme

# ─────────────────────────────────────────────────────────────────────────────
# Theme & console
# ─────────────────────────────────────────────────────────────────────────────

THEME = Theme(
    {
        "banner":      "bold bright_cyan",
        "ok":          "bold bright_green",
        "warn":        "bold yellow",
        "err":         "bold bright_red",
        "info":        "dim cyan",
        "muted":       "dim white",
        "heading":     "bold underline white",
        "stat.label":  "cyan",
        "stat.value":  "bold white",
        "error.row":   "bright_red",
        "error.type":  "yellow",
        "error.msg":   "white",
    }
)

console = Console(theme=THEME, highlight=False)


# ─────────────────────────────────────────────────────────────────────────────
# Data structures
# ─────────────────────────────────────────────────────────────────────────────

class ErrorRecord:
    """Represents a single validation error."""

    __slots__ = ("line", "error_type", "message", "raw_snippet")

    def __init__(
        self,
        line: int,
        error_type: str,
        message: str,
        raw_snippet: str = "",
    ) -> None:
        self.line = line
        self.error_type = error_type
        self.message = message
        self.raw_snippet = raw_snippet[:120]  # cap snippet length

    def __repr__(self) -> str:  # pragma: no cover
        return f"ErrorRecord(line={self.line}, type={self.error_type!r})"


class ValidationResult:
    """Aggregated result of a full file scan."""

    def __init__(self) -> None:
        self.total_rows: int = 0
        self.clean_rows: int = 0
        self.corrupt_rows: int = 0
        self.errors: List[ErrorRecord] = []
        self.error_type_counts: Dict[str, int] = {}
        self.duration_seconds: float = 0.0
        self.file_path: str = ""
        self.file_type: str = ""
        self.file_size_bytes: int = 0
        self.encoding_used: str = "utf-8"

    def add_error(self, record: ErrorRecord) -> None:
        self.errors.append(record)
        self.error_type_counts[record.error_type] = (
            self.error_type_counts.get(record.error_type, 0) + 1
        )
        self.corrupt_rows += 1

    @property
    def rows_per_second(self) -> float:
        if self.duration_seconds == 0:
            return 0.0
        return self.total_rows / self.duration_seconds

    @property
    def corruption_rate(self) -> float:
        if self.total_rows == 0:
            return 0.0
        return (self.corrupt_rows / self.total_rows) * 100


# ─────────────────────────────────────────────────────────────────────────────
# Encoding helper
# ─────────────────────────────────────────────────────────────────────────────

def detect_encoding(file_path: str) -> str:
    """Try UTF-8 first, fall back to latin-1."""
    try:
        with open(file_path, "r", encoding="utf-8") as fh:
            fh.read(4096)
        return "utf-8"
    except UnicodeDecodeError:
        return "latin-1"


# ─────────────────────────────────────────────────────────────────────────────
# Streaming generators
# ─────────────────────────────────────────────────────────────────────────────

def stream_csv_rows(
    file_path: str,
    encoding: str,
) -> Generator[Tuple[int, Optional[List[str]], Optional[str]], None, None]:
    """
    Yield (line_number, fields_or_None, raw_line_or_None).
    On parse error yields (line_number, None, raw_line).
    """
    with open(file_path, "r", encoding=encoding, errors="replace", newline="") as fh:
        reader = csv.reader(fh)
        for line_num, row in enumerate(reader, start=1):
            raw = ",".join(row)
            yield line_num, row, raw


def stream_json_lines(
    file_path: str,
    encoding: str,
) -> Generator[Tuple[int, Optional[Any], str], None, None]:
    """
    Yield (line_number, parsed_object_or_None, raw_line).
    Handles JSONL (one JSON object per line).
    """
    with open(file_path, "r", encoding=encoding, errors="replace") as fh:
        for line_num, raw_line in enumerate(fh, start=1):
            stripped = raw_line.rstrip("\n\r")
            if not stripped:
                continue
            try:
                obj = json.loads(stripped)
                yield line_num, obj, stripped
            except json.JSONDecodeError:
                yield line_num, None, stripped


# ─────────────────────────────────────────────────────────────────────────────
# CSV validation engine
# ─────────────────────────────────────────────────────────────────────────────

_MISSING_VALUES = {"", "null", "none", "na", "n/a", "nan", "#n/a"}


def _is_missing(value: str) -> bool:
    return value.strip().lower() in _MISSING_VALUES


def validate_csv(
    file_path: str,
    encoding: str,
    progress: Progress,
    task_id: Any,
    file_size: int,
) -> ValidationResult:
    result = ValidationResult()
    result.file_path = file_path
    result.file_type = "CSV"
    result.file_size_bytes = file_size
    result.encoding_used = encoding

    expected_columns: Optional[int] = None
    header: List[str] = []
    bytes_read = 0
    start_time = time.perf_counter()

    for line_num, fields, raw in stream_csv_rows(file_path, encoding):
        if fields is None:
            continue  # should not happen with csv.reader but guard anyway

        bytes_read += len(raw.encode(encoding, errors="replace")) + 1
        progress.update(task_id, completed=min(bytes_read, file_size))

        if line_num == 1:
            header = fields
            expected_columns = len(fields)
            result.total_rows += 1
            result.clean_rows += 1
            continue

        result.total_rows += 1
        row_has_error = False

        # ── Column count check ───────────────────────────────────────────────
        if len(fields) != expected_columns:
            result.add_error(
                ErrorRecord(
                    line=line_num,
                    error_type="COLUMN_MISMATCH",
                    message=(
                        f"Expected {expected_columns} columns, "
                        f"found {len(fields)}"
                    ),
                    raw_snippet=raw,
                )
            )
            row_has_error = True

        # ── Missing value check ──────────────────────────────────────────────
        missing_cols: List[str] = []
        for idx, val in enumerate(fields):
            col_name = header[idx] if idx < len(header) else f"col_{idx}"
            if _is_missing(val):
                missing_cols.append(col_name)

        if missing_cols and not row_has_error:
            result.add_error(
                ErrorRecord(
                    line=line_num,
                    error_type="MISSING_VALUE",
                    message=f"Null/empty in columns: {', '.join(missing_cols)}",
                    raw_snippet=raw,
                )
            )
            row_has_error = True
        elif missing_cols and row_has_error:
            # Append additional sub-error without double-counting corrupt row
            result.errors.append(
                ErrorRecord(
                    line=line_num,
                    error_type="MISSING_VALUE",
                    message=f"Null/empty in columns: {', '.join(missing_cols)}",
                    raw_snippet=raw,
                )
            )
            result.error_type_counts["MISSING_VALUE"] = (
                result.error_type_counts.get("MISSING_VALUE", 0) + 1
            )

        if not row_has_error:
            result.clean_rows += 1

    result.duration_seconds = time.perf_counter() - start_time
    return result


# ─────────────────────────────────────────────────────────────────────────────
# JSON validation engine
# ─────────────────────────────────────────────────────────────────────────────

def _check_json_missing(obj: Any, line_num: int) -> Optional[ErrorRecord]:
    """Check for null / empty-string values in a JSON object (top-level keys)."""
    if not isinstance(obj, dict):
        return None
    missing_keys: List[str] = []
    for k, v in obj.items():
        if v is None:
            missing_keys.append(str(k))
        elif isinstance(v, str) and _is_missing(v):
            missing_keys.append(str(k))
    if missing_keys:
        return ErrorRecord(
            line=line_num,
            error_type="MISSING_VALUE",
            message=f"Null/empty keys: {', '.join(missing_keys)}",
            raw_snippet="",
        )
    return None


def _infer_schema(sample_obj: Dict[str, Any]) -> Dict[str, type]:
    """Build a simple type map from the first valid object."""
    schema: Dict[str, type] = {}
    for k, v in sample_obj.items():
        if v is not None:
            schema[k] = type(v)
    return schema


def _check_type_mismatches(
    obj: Dict[str, Any],
    schema: Dict[str, type],
    line_num: int,
    raw: str,
) -> Optional[ErrorRecord]:
    mismatches: List[str] = []
    for k, expected_type in schema.items():
        if k not in obj:
            continue
        v = obj[k]
        if v is None:
            continue
        # Allow int where float expected and vice-versa
        if expected_type in (int, float) and isinstance(v, (int, float)):
            continue
        if not isinstance(v, expected_type):
            mismatches.append(
                f"{k}: expected {expected_type.__name__}, got {type(v).__name__}"
            )
    if mismatches:
        return ErrorRecord(
            line=line_num,
            error_type="TYPE_MISMATCH",
            message="; ".join(mismatches),
            raw_snippet=raw[:120],
        )
    return None


def validate_json(
    file_path: str,
    encoding: str,
    progress: Progress,
    task_id: Any,
    file_size: int,
) -> ValidationResult:
    result = ValidationResult()
    result.file_path = file_path
    result.file_type = "JSON/JSONL"
    result.file_size_bytes = file_size
    result.encoding_used = encoding

    schema: Optional[Dict[str, type]] = None
    bytes_read = 0
    start_time = time.perf_counter()

    for line_num, obj, raw in stream_json_lines(file_path, encoding):
        bytes_read += len(raw.encode(encoding, errors="replace")) + 1
        progress.update(task_id, completed=min(bytes_read, file_size))

        result.total_rows += 1
        row_has_error = False

        # ── Syntax error ─────────────────────────────────────────────────────
        if obj is None:
            result.add_error(
                ErrorRecord(
                    line=line_num,
                    error_type="SYNTAX_ERROR",
                    message="Invalid JSON syntax",
                    raw_snippet=raw,
                )
            )
            row_has_error = True
            continue

        # ── Schema inference from first valid object ──────────────────────────
        if schema is None and isinstance(obj, dict):
            schema = _infer_schema(obj)

        # ── Missing value check ───────────────────────────────────────────────
        missing_err = _check_json_missing(obj, line_num)
        if missing_err:
            result.add_error(missing_err)
            row_has_error = True

        # ── Type mismatch check ───────────────────────────────────────────────
        if schema and isinstance(obj, dict):
            type_err = _check_type_mismatches(obj, schema, line_num, raw)
            if type_err:
                if row_has_error:
                    result.errors.append(type_err)
                    result.error_type_counts["TYPE_MISMATCH"] = (
                        result.error_type_counts.get("TYPE_MISMATCH", 0) + 1
                    )
                else:
                    result.add_error(type_err)
                    row_has_error = True

        if not row_has_error:
            result.clean_rows += 1

    result.duration_seconds = time.perf_counter() - start_time
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Report generation
# ─────────────────────────────────────────────────────────────────────────────

def _human_size(num_bytes: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(num_bytes) < 1024.0:
            return f"{num_bytes:.1f} {unit}"
        num_bytes /= 1024.0  # type: ignore[assignment]
    return f"{num_bytes:.1f} PB"


def write_markdown_report(result: ValidationResult, output_path: str) -> None:
    """Write a detailed Markdown report to disk."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines: List[str] = []

    lines.append("# DataPurge Pro — Validation Report")
    lines.append("")
    lines.append(f"> Generated: {now}")
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## Summary")
    lines.append("")
    lines.append(f"| Field              | Value |")
    lines.append(f"|--------------------|-------|")
    lines.append(f"| File               | `{result.file_path}` |")
    lines.append(f"| File Type          | {result.file_type} |")
    lines.append(f"| File Size          | {_human_size(result.file_size_bytes)} |")
    lines.append(f"| Encoding           | {result.encoding_used} |")
    lines.append(f"| Total Rows Scanned | {result.total_rows:,} |")
    lines.append(f"| Clean Rows         | {result.clean_rows:,} |")
    lines.append(f"| Corrupt Rows       | {result.corrupt_rows:,} |")
    lines.append(f"| Corruption Rate    | {result.corruption_rate:.2f}% |")
    lines.append(f"| Processing Time    | {result.duration_seconds:.2f}s |")
    lines.append(f"| Throughput         | {result.rows_per_second:,.0f} rows/s |")
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## Error Type Breakdown")
    lines.append("")

    if result.error_type_counts:
        lines.append("| Error Type      | Count |")
        lines.append("|-----------------|-------|")
        for etype, count in sorted(
            result.error_type_counts.items(), key=lambda x: -x[1]
        ):
            lines.append(f"| `{etype}` | {count:,} |")
    else:
        lines.append("_No errors detected. File is clean!_ ✅")

    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## Detailed Error Log")
    lines.append("")

    if result.errors:
        lines.append("| Line # | Error Type | Message | Snippet |")
        lines.append("|--------|------------|---------|---------|")
        for err in result.errors[:10_000]:  # cap at 10k rows in report
            snippet = err.raw_snippet.replace("|", "\\|").replace("\n", " ")
            msg = err.message.replace("|", "\\|")
            lines.append(
                f"| {err.line:,} | `{err.error_type}` | {msg} | `{snippet}` |"
            )
        if len(result.errors) > 10_000:
            lines.append("")
            lines.append(
                f"> ⚠️  Only the first 10,000 errors are shown. "
                f"Total: {len(result.errors):,}"
            )
    else:
        lines.append("_No errors detected._")

    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("*DataPurge Pro — High-Performance Streaming Validator*")
    lines.append("")

    with open(output_path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines))


# ─────────────────────────────────────────────────────────────────────────────
# TUI rendering
# ─────────────────────────────────────────────────────────────────────────────

def render_banner() -> None:
    banner_text = Text()
    banner_text.append("  ██████╗  █████╗ ████████╗ █████╗ \n", style="bright_cyan bold")
    banner_text.append(" ██╔══██╗██╔══██╗╚══██╔══╝██╔══██╗\n", style="cyan bold")
    banner_text.append(" ██║  ██║███████║   ██║   ███████║\n", style="bright_blue bold")
    banner_text.append(" ██║  ██║██╔══██║   ██║   ██╔══██║\n", style="blue bold")
    banner_text.append(" ██████╔╝██║  ██║   ██║   ██║  ██║\n", style="bright_magenta bold")
    banner_text.append(" ╚═════╝ ╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝\n", style="magenta bold")

    subtitle = Text(" PURGE PRO  ·  Streaming JSON & CSV Validator  ·  v1.0.0")
    subtitle.stylize("bold bright_white on #1a1a2e")

    console.print()
    console.print(Panel(banner_text, subtitle=subtitle, border_style="bright_cyan", padding=(0, 2)))
    console.print()


def render_file_info(file_path: str, file_size: int, encoding: str, file_type: str) -> None:
    table = Table(box=box.SIMPLE_HEAVY, show_header=False, padding=(0, 2))
    table.add_column("Label", style="stat.label", no_wrap=True)
    table.add_column("Value", style="stat.value")

    table.add_row("📁  File", escape(file_path))
    table.add_row("📏  Size", _human_size(file_size))
    table.add_row("🔤  Encoding", encoding)
    table.add_row("📄  Format", file_type.upper())

    console.print(
        Panel(table, title="[heading]File Information[/]", border_style="cyan", padding=(0, 1))
    )
    console.print()


def render_summary_table(result: ValidationResult) -> None:
    console.print(Rule("[heading]Scan Results[/]", style="bright_cyan"))
    console.print()

    # ── Top stats row ─────────────────────────────────────────────────────────
    def stat_panel(label: str, value: str, style: str, icon: str) -> Panel:
        body = Text()
        body.append(f"{icon}  ", style="bold")
        body.append(value, style=f"bold {style}")
        return Panel(body, title=f"[muted]{label}[/]", border_style=style, padding=(0, 1))

    pct_clean = (
        f"{(result.clean_rows / result.total_rows * 100):.1f}%"
        if result.total_rows
        else "—"
    )
    pct_corrupt = f"{result.corruption_rate:.2f}%"

    console.print(
        Columns(
            [
                stat_panel("Total Rows", f"{result.total_rows:,}", "white", "📊"),
                stat_panel("Clean Rows", f"{result.clean_rows:,} ({pct_clean})", "bright_green", "✅"),
                stat_panel("Corrupt Rows", f"{result.corrupt_rows:,} ({pct_corrupt})", "bright_red", "❌"),
                stat_panel("Throughput", f"{result.rows_per_second:,.0f} rows/s", "bright_cyan", "⚡"),
            ],
            equal=True,
            expand=True,
        )
    )
    console.print()

    # ── Error breakdown ───────────────────────────────────────────────────────
    if result.error_type_counts:
        err_table = Table(
            title="Error Type Breakdown",
            box=box.ROUNDED,
            border_style="yellow",
            header_style="bold yellow",
            show_lines=False,
            padding=(0, 2),
        )
        err_table.add_column("Error Type", style="error.type", no_wrap=True)
        err_table.add_column("Count", style="error.msg", justify="right")
        err_table.add_column("Share", style="muted", justify="right")

        total_errors = sum(result.error_type_counts.values())
        for etype, count in sorted(
            result.error_type_counts.items(), key=lambda x: -x[1]
        ):
            share = f"{count / total_errors * 100:.1f}%" if total_errors else "—"
            err_table.add_row(etype, f"{count:,}", share)

        console.print(err_table)
        console.print()

    # ── Sample errors ─────────────────────────────────────────────────────────
    if result.errors:
        sample = result.errors[:25]
        sample_table = Table(
            title=f"Sample Errors (showing {len(sample)} of {len(result.errors):,})",
            box=box.MINIMAL_DOUBLE_HEAD,
            border_style="red",
            header_style="bold red",
            show_lines=True,
            padding=(0, 1),
        )
        sample_table.add_column("Line #", style="error.row", justify="right", no_wrap=True)
        sample_table.add_column("Type", style="error.type", no_wrap=True)
        sample_table.add_column("Message", style="error.msg", max_width=60)
        sample_table.add_column("Snippet", style="muted", max_width=40)

        for err in sample:
            sample_table.add_row(
                f"{err.line:,}",
                err.error_type,
                escape(err.message),
                escape(err.raw_snippet[:40]),
            )

        console.print(sample_table)
        console.print()

    # ── Timing ────────────────────────────────────────────────────────────────
    console.print(
        f"[muted]⏱  Completed in [bold white]{result.duration_seconds:.2f}s[/] "
        f"· File size: [bold white]{_human_size(result.file_size_bytes)}[/][/]"
    )
    console.print()


# ─────────────────────────────────────────────────────────────────────────────
# Orchestrator
# ─────────────────────────────────────────────────────────────────────────────

def detect_file_type(file_path: str) -> str:
    ext = Path(file_path).suffix.lower()
    if ext in (".csv", ".tsv"):
        return "csv"
    if ext in (".json", ".jsonl", ".ndjson"):
        return "json"
    # Sniff first 512 bytes
    try:
        with open(file_path, "rb") as fh:
            head = fh.read(512).lstrip()
        if head.startswith(b"{") or head.startswith(b"["):
            return "json"
    except OSError:
        pass
    return "csv"  # default


def run(file_path: str) -> None:
    # ── Existence & permission checks ─────────────────────────────────────────
    path = Path(file_path)

    if not path.exists():
        console.print(f"[err]✘  File not found:[/] {escape(str(path))}")
        sys.exit(1)

    if not path.is_file():
        console.print(f"[err]✘  Path is not a regular file:[/] {escape(str(path))}")
        sys.exit(1)

    try:
        file_size = path.stat().st_size
    except PermissionError:
        console.print(f"[err]✘  Permission denied:[/] {escape(str(path))}")
        sys.exit(1)

    if file_size == 0:
        console.print("[warn]⚠  File is empty. Nothing to validate.[/]")
        sys.exit(0)

    render_banner()

    file_type = detect_file_type(file_path)
    encoding = detect_encoding(file_path)

    render_file_info(str(path.resolve()), file_size, encoding, file_type)

    # ── Progress bar ──────────────────────────────────────────────────────────
    progress = Progress(
        SpinnerColumn(spinner_name="dots2", style="bright_cyan"),
        TextColumn("[bright_cyan]{task.description}"),
        BarColumn(bar_width=40, style="cyan", complete_style="bright_green", finished_style="bright_green"),
        TaskProgressColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        TransferSpeedColumn(),
        console=console,
        transient=False,
    )

    with progress:
        task_id = progress.add_task(
            "Scanning…",
            total=file_size,
        )

        if file_type == "csv":
            result = validate_csv(file_path, encoding, progress, task_id, file_size)
        else:
            result = validate_json(file_path, encoding, progress, task_id, file_size)

        progress.update(task_id, completed=file_size, description="[ok]Done ✔[/]")

    console.print()

    # ── Display results ───────────────────────────────────────────────────────
    render_summary_table(result)

    # ── Write Markdown report ─────────────────────────────────────────────────
    report_path = str(path.parent / f"{path.stem}_report.md")
    try:
        write_markdown_report(result, report_path)
        console.print(
            f"[ok]📝  Report saved →[/] [bold white]{escape(report_path)}[/]"
        )
    except PermissionError:
        console.print(
            f"[warn]⚠  Could not write report (permission denied): {escape(report_path)}[/]"
        )

    console.print()

    # ── Final verdict ─────────────────────────────────────────────────────────
    if result.corrupt_rows == 0:
        console.print(
            Panel(
                "[ok]🎉  File is [bold]CLEAN[/bold] — zero errors detected![/]",
                border_style="bright_green",
                padding=(0, 2),
            )
        )
    else:
        rate_color = "bright_red" if result.corruption_rate > 10 else "yellow"
        console.print(
            Panel(
                f"[{rate_color}]⚠  [{rate_color} bold]{result.corrupt_rows:,}[/] corrupt rows "
                f"({result.corruption_rate:.2f}%) detected. "
                f"See the report for details.[/]",
                border_style=rate_color,
                padding=(0, 2),
            )
        )

    console.print()


# ─────────────────────────────────────────────────────────────────────────────
# CLI entry-point
# ─────────────────────────────────────────────────────────────────────────────

def _usage() -> None:
    console.print(
        Panel(
            "[bold white]Usage:[/]  [bright_cyan]datapurge_pro.py[/] "
            "[yellow]<path/to/file.csv|.json|.jsonl>[/]\n\n"
            "[muted]Supports:[/]  CSV · TSV · JSON · JSONL · NDJSON\n"
            "[muted]Output:[/]   <filename>_report.md  (same directory as input)",
            title="[heading]DataPurge Pro[/]",
            border_style="cyan",
        )
    )


if __name__ == "__main__":
    try:
        if len(sys.argv) < 2:
            render_banner()
            _usage()
            sys.exit(0)

        target_file = sys.argv[1]
        run(target_file)

    except KeyboardInterrupt:
        console.print()
        console.print("[warn]⚡  Interrupted by user — scan aborted.[/]")
        console.print()
        sys.exit(130)
    except Exception as exc:  # pylint: disable=broad-except
        console.print_exception(show_locals=False)
        console.print(f"[err]✘  Unexpected error: {escape(str(exc))}[/]")
        sys.exit(1)