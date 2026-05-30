"""Local Windows printing helpers for inventory labels.

This is intentionally scoped to localhost direct printing. Production runs on
Linux and should never try to talk to a workstation printer.
"""
from __future__ import annotations

import json
import os
import subprocess
import tempfile
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_LABEL_PRINTER_NAME = "JADENS C10"
DEFAULT_PRINTER_NAME = DEFAULT_LABEL_PRINTER_NAME
SCRIPT_PATH = REPO_ROOT / "scripts" / "print_windows_labels.ps1"


def list_windows_label_printers() -> list[str]:
    """Return installed local Windows printer names, sorted as reported by Windows."""
    if os.name != "nt":
        return []
    proc = subprocess.run(
        [
            "powershell.exe",
            "-NoProfile",
            "-Command",
            "Get-Printer | Select-Object -ExpandProperty Name",
        ],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    if proc.returncode != 0:
        return []
    return [line.strip() for line in proc.stdout.splitlines() if line.strip()]


def choose_label_printer(requested: str | None, printers: list[str]) -> str:
    requested_name = (requested or "").strip()
    if requested_name and requested_name in printers:
        return requested_name
    for printer in printers:
        if printer.lower() == DEFAULT_LABEL_PRINTER_NAME.lower():
            return printer
    if printers:
        return printers[0]
    return requested_name or DEFAULT_LABEL_PRINTER_NAME


def build_windows_label_print_payload(
    labels: list[dict[str, Any]],
    *,
    printer_name: str = DEFAULT_LABEL_PRINTER_NAME,
    logo_path: Path | None = None,
    barcode_image_paths: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Return the JSON-safe payload consumed by the PowerShell print script."""
    logo = logo_path or (REPO_ROOT / "app" / "static" / "degen-logo-label.png")
    barcode_paths = barcode_image_paths or {}
    return {
        "printer_name": printer_name,
        "paper_width_hundredths": 300,
        "paper_height_hundredths": 100,
        "logo_path": str(logo),
        "labels": [
            {
                "price_text": str(label.get("price_text") or ""),
                "price_class": str(label.get("price_class") or ""),
                "barcode_value": str(label.get("barcode_value") or ""),
                "barcode_image_path": barcode_paths.get(str(label.get("barcode_value") or ""), ""),
                "employee_lines": [
                    {
                        "field": str(line.get("field") or ""),
                        "label": str(line.get("label") or ""),
                        "value": str(line.get("value") or ""),
                    }
                    for line in label.get("employee_lines", [])
                ],
            }
            for label in labels
        ],
    }


build_jadens_print_payload = build_windows_label_print_payload


def _write_barcode_images(labels: list[dict[str, Any]], output_dir: Path) -> dict[str, str]:
    try:
        import barcode
        from barcode.writer import ImageWriter
    except ImportError as exc:  # pragma: no cover - dependency is present in app env
        raise RuntimeError("python-barcode image writer is not available") from exc

    Code128 = barcode.get_barcode_class("code128")
    paths: dict[str, str] = {}
    for label in labels:
        value = str(label.get("barcode_value") or "").strip()
        if not value or value in paths:
            continue
        target_base = output_dir / value.replace("/", "_")
        code = Code128(value, writer=ImageWriter())
        written = code.save(
            str(target_base),
            options={
                "module_width": 0.22,
                "module_height": 8.5,
                "quiet_zone": 1.2,
                "font_size": 0,
                "text_distance": 1,
                "write_text": False,
            },
        )
        paths[value] = str(Path(written))
    return paths


def print_windows_wrap_3x1_labels(
    labels: list[dict[str, Any]],
    *,
    printer_name: str = DEFAULT_LABEL_PRINTER_NAME,
) -> int:
    """Print 3x1 wraparound labels directly to a local Windows printer queue."""
    if os.name != "nt":
        raise RuntimeError("direct label printing is only available on Windows localhost")
    if not labels:
        return 0
    if not SCRIPT_PATH.exists():
        raise RuntimeError(f"direct label print script not found: {SCRIPT_PATH}")

    selected_printer = choose_label_printer(printer_name, list_windows_label_printers())

    with tempfile.TemporaryDirectory(prefix="degen_label_print_") as temp:
        temp_path = Path(temp)
        barcode_paths = _write_barcode_images(labels, temp_path)
        payload = build_windows_label_print_payload(
            labels,
            printer_name=selected_printer,
            barcode_image_paths=barcode_paths,
        )
        payload_path = temp_path / "labels.json"
        payload_path.write_text(json.dumps(payload), encoding="utf-8")

        proc = subprocess.run(
            [
                "powershell.exe",
                "-NoProfile",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                str(SCRIPT_PATH),
                "-PayloadPath",
                str(payload_path),
            ],
            capture_output=True,
            text=True,
            timeout=90,
            check=False,
        )
        if proc.returncode != 0:
            details = (proc.stderr or proc.stdout or "").strip()
            raise RuntimeError(details or f"direct label print failed with exit code {proc.returncode}")
        return len(labels)


print_jadens_wrap_3x1_labels = print_windows_wrap_3x1_labels
