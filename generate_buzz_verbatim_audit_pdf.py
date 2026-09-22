"""Create an exhaustive, redacted-free verbatim input/output audit PDF.

The canonical Buzz Layer PDF is appended as original PDF pages.  The front
matter also contains its extracted text, exact run artifacts, and every raw
platform receipt written by the bounded run.  No credentials are added; raw
receipts are copied as recorded by Agent-Reach.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import textwrap
from datetime import datetime, timezone
from pathlib import Path

from pypdf import PdfReader, PdfWriter
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas


PAGE_W, PAGE_H = A4
LEFT = 36
TOP = PAGE_H - 42
BOTTOM = 38
FONT = "Courier"
FONT_SIZE = 6.2
LEADING = 7.2
MAX_CHARS = 132


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_text(pdf: Path) -> str:
    pdftotext = shutil.which("pdftotext")
    if pdftotext:
        result = subprocess.run(
            [pdftotext, "-layout", os.fspath(pdf), "-"],
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        return result.stdout
    return "\n\n".join((page.extract_text() or "") for page in PdfReader(os.fspath(pdf)).pages)


def json_bytes(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace")


class AuditCanvas:
    def __init__(self, path: Path, title: str) -> None:
        self.canvas = canvas.Canvas(os.fspath(path), pagesize=A4, pageCompression=1)
        self.title = title
        self.page = 0
        self.y = TOP
        self._new_page()

    def _new_page(self) -> None:
        if self.page:
            self.canvas.setFont("Helvetica", 6)
            self.canvas.drawRightString(PAGE_W - LEFT, 20, f"{self.page}")
            self.canvas.showPage()
        self.page += 1
        self.canvas.setFont("Helvetica-Bold", 7)
        self.canvas.drawString(LEFT, PAGE_H - 24, self.title[:120])
        self.canvas.setFont(FONT, FONT_SIZE)
        self.y = TOP

    def line(self, value: str = "") -> None:
        wrapped = textwrap.wrap(
            value.expandtabs(2),
            width=MAX_CHARS,
            replace_whitespace=False,
            drop_whitespace=False,
            break_long_words=True,
            break_on_hyphens=False,
        ) or [""]
        for part in wrapped:
            if self.y < BOTTOM:
                self._new_page()
            self.canvas.drawString(LEFT, self.y, part[:MAX_CHARS])
            self.y -= LEADING

    def heading(self, value: str) -> None:
        if self.y < BOTTOM + 4 * LEADING:
            self._new_page()
        self.canvas.setFont("Helvetica-Bold", 9)
        self.canvas.drawString(LEFT, self.y, value[:120])
        self.y -= 2 * LEADING
        self.canvas.setFont(FONT, FONT_SIZE)

    def finish(self) -> None:
        self.canvas.setFont("Helvetica", 6)
        self.canvas.drawRightString(PAGE_W - LEFT, 20, f"{self.page}")
        self.canvas.save()


def add_file(audit: AuditCanvas, label: str, path: Path) -> None:
    audit.heading(label)
    if not path.exists():
        audit.line(f"[MISSING FILE] {path}")
        return
    audit.line(f"PATH: {path}")
    audit.line(f"BYTES: {path.stat().st_size}")
    audit.line(f"SHA256: {sha256(path)}")
    audit.line("----- BEGIN VERBATIM FILE -----")
    for line in json_bytes(path).splitlines():
        audit.line(line)
    audit.line("----- END VERBATIM FILE -----")


def create_audit(root: Path, site_root: Path, canonical_pdf: Path, output_pdf: Path, site_commit: str, production_version: str) -> dict[str, object]:
    latest_path = root / "output" / "latest_buzz_run.json"
    latest = json.loads(latest_path.read_text(encoding="utf-8"))
    run_id = latest["run_id"]
    raw_dir = root / "output" / "runs" / run_id / "raw"

    staging = output_pdf.with_suffix(".front.pdf")
    audit = AuditCanvas(staging, "OnTheRice canonical Buzz Layer — verbatim input/output audit")
    audit.heading("AUDIT SCOPE")
    for line in [
        "This PDF is an evidence-preserving audit of the canonical Buzz Layer run.",
        f"Generated UTC: {datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')}",
        f"Canonical input: {canonical_pdf}",
        f"Canonical input SHA256: {sha256(canonical_pdf)}",
        f"Canonical input pages: {len(PdfReader(os.fspath(canonical_pdf)).pages)}",
        f"Run ID: {run_id}",
        f"Site evidence commit: {site_commit or 'not supplied'}",
        f"Production version: {production_version or 'not supplied'}",
        "Raw receipts are reproduced from their stored JSON envelopes. Raw files were not mutated by finalization.",
        "The appended original canonical PDF pages are included unchanged after this generated audit section.",
    ]:
        audit.line(line)

    audit.heading("VERBATIM CANONICAL INPUT — EXTRACTED TEXT")
    audit.line("----- BEGIN VERBATIM CANONICAL TEXT -----")
    for line in canonical_text(canonical_pdf).splitlines():
        audit.line(line)
    audit.line("----- END VERBATIM CANONICAL TEXT -----")

    audit.heading("VERBATIM EXECUTION COMMAND")
    audit.line(".\\.venv\\Scripts\\python.exe -m agent_reach.buzz --site https://new.ontherice.org --output output --channel-timeout 12 --global-timeout 180")

    output_files = [
        "latest_buzz_run.json",
        "edition_anchor_registry.json",
        "buzz_query_packs.json",
        "platform_coverage.json",
        "raw_social_observations.json",
        "normalized_social_observations.json",
        "rejected_social_observations.json",
        "buzzpacks.json",
        "source_ledger.json",
        "verification_ledger.json",
        "buzz_mapping.json",
        "finalization_receipt.json",
        "agent_audit_receipts.json",
        "postdeploy_route_audit.json",
    ]
    for name in output_files:
        add_file(audit, f"VERBATIM OUTPUT ARTIFACT — {name}", root / "output" / name)

    receipts = sorted(raw_dir.rglob("*.json")) if raw_dir.exists() else []
    audit.heading(f"VERBATIM RAW PLATFORM RECEIPTS — {len(receipts)} FILES")
    for receipt in receipts:
        add_file(audit, f"RAW RECEIPT — {receipt.relative_to(raw_dir)}", receipt)
    audit.heading("END OF GENERATED AUDIT SECTION")
    audit.line("The following pages are the unchanged original canonical input PDF.")
    audit.finish()

    writer = PdfWriter()
    for page in PdfReader(os.fspath(staging)).pages:
        writer.add_page(page)
    for page in PdfReader(os.fspath(canonical_pdf)).pages:
        writer.add_page(page)
    with output_pdf.open("wb") as handle:
        writer.write(handle)
    staging.unlink(missing_ok=True)
    return {
        "output": str(output_pdf),
        "run_id": run_id,
        "canonical_pdf_sha256": sha256(canonical_pdf),
        "raw_receipt_count": len(receipts),
        "output_artifact_count": len(output_files),
        "site_commit": site_commit,
        "production_version": production_version,
        "pdf_pages": len(PdfReader(os.fspath(output_pdf)).pages),
        "pdf_sha256": sha256(output_pdf),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--site-root", type=Path, default=None)
    parser.add_argument("--canonical-pdf", type=Path, default=None)
    parser.add_argument("--output", type=Path, default=None)
    parser.add_argument("--site-commit", default="")
    parser.add_argument("--production-version", default="")
    args = parser.parse_args()
    root = args.root.resolve()
    canonical_pdf = (args.canonical_pdf or (root / "output" / "Buzz Layer.pdf")).resolve()
    output_pdf = (args.output or (root / "output" / "Buzz_Layer_Verbatim_Input_Output_Audit.pdf")).resolve()
    output_pdf.parent.mkdir(parents=True, exist_ok=True)
    result = create_audit(root, (args.site_root or root / "production-bound-rc2").resolve(), canonical_pdf, output_pdf, args.site_commit, args.production_version)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
