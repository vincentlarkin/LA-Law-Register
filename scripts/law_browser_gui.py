r"""
Browser-style desktop GUI for navigating the local Louisiana law library.

Features:
- Browse by category, bundle, and document without typing a search first.
- Show a document summary pane with a case-law-focused "What Was Learned" view.
- Open source URLs and local opinion PDFs when available.

Run:
  python scripts\law_browser_gui.py
  python scripts\law_browser_gui.py --db out/index.sqlite
"""

from __future__ import annotations

import argparse
import html
import json
import re
import sqlite3
import sys
from pathlib import Path

from PySide6.QtCore import Qt, QUrl
from PySide6.QtGui import QColor, QDesktopServices, QFont, QTextOption
from PySide6.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QFileDialog,
    QGroupBox,
    QHeaderView,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QMainWindow,
    QMessageBox,
    QPlainTextEdit,
    QPushButton,
    QSplitter,
    QStatusBar,
    QTabWidget,
    QTableWidget,
    QTableWidgetItem,
    QTextBrowser,
    QVBoxLayout,
    QWidget,
)


_INVALID_PATH_CHARS = re.compile(r'[<>:"/\\|?*\x00-\x1F]')
_WS = re.compile(r"\s+")
_BODY_HEADER_RE = re.compile(
    r"^(facts and procedural history|discussion|conclusion|decree|analysis|law and discussion)\b",
    re.IGNORECASE,
)
_META_LINE_RE = re.compile(
    r"^(Court|Release date|Release|Author|Parish|Disposition|Release notes|Official release page|Official PDF):\s*(.+)$",
    re.IGNORECASE,
)
_BILL_JSON_FIELD_RE_TEMPLATE = r'"{key}"\s*:\s*"((?:\\.|[^"\\])*)"'


SECTION_ALL = "All Sources"
SECTION_BILLS = "Bills"
SECTION_CODES = "Louisiana Codes"
SECTION_CONSTITUTION = "Constitution"
SECTION_CASES = "Court Decisions"
SECTION_RULES = "Legislative Rules"
SECTION_OTHER = "Other"

BILL_STATUS_OPTIONS = [
    ("All statuses", ""),
    ("Passed into Law", "Passed into Law"),
    ("Vetoed", "Vetoed"),
    ("Still in Process", "Still in Process"),
    ("Failed / Other", "Failed or Other Final Disposition"),
]

DOC_SORT_OPTIONS = [
    ("Citation", "citation"),
    ("Status", "status"),
    ("Title", "title"),
]


def safe_name(value: str, *, max_len: int = 140) -> str:
    value = _WS.sub(" ", value.strip())
    value = _INVALID_PATH_CHARS.sub("_", value)
    value = value.rstrip(". ")
    if len(value) > max_len:
        value = value[: max_len - 1].rstrip()
    return value or "_"


def _connect(db_path: str) -> sqlite3.Connection:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    return con


def _resolve_default_db_path() -> str:
    candidates: list[Path] = []
    cwd = Path.cwd()
    candidates.append(cwd / "out" / "index.sqlite")

    if getattr(sys, "frozen", False):
        exe_dir = Path(sys.executable).resolve().parent
        candidates.append(exe_dir / "out" / "index.sqlite")
        candidates.append(exe_dir.parent / "out" / "index.sqlite")
    else:
        script_dir = Path(__file__).resolve().parent
        repo_root = script_dir.parent
        candidates.append(repo_root / "out" / "index.sqlite")

    for path in candidates:
        if path.exists():
            return str(path)
    return str(candidates[0])


def _normalize_ws(value: str) -> str:
    return _WS.sub(" ", (value or "").replace("\xa0", " ")).strip()


def _is_case_law(category: str) -> bool:
    lowered = (category or "").lower()
    return "court" in lowered or "case" in lowered or "decision" in lowered


def _is_bill(category: str) -> bool:
    return "bill" in (category or "").lower()


def _category_section(category: str) -> str:
    lowered = (category or "").lower()
    if "bill" in lowered:
        return SECTION_BILLS
    if _is_case_law(category):
        return SECTION_CASES
    if "constitution" in lowered:
        return SECTION_CONSTITUTION
    if "rule" in lowered:
        return SECTION_RULES
    if "code" in lowered or "statute" in lowered:
        return SECTION_CODES
    return SECTION_OTHER


def _section_sort_key(section: str) -> tuple[int, str]:
    order = {
        SECTION_CODES: 0,
        SECTION_CONSTITUTION: 1,
        SECTION_RULES: 2,
        SECTION_BILLS: 3,
        SECTION_CASES: 4,
        SECTION_OTHER: 5,
    }
    return (order.get(section, 99), section)


def _bill_status_color(category: str, status_group: str = "", status_label: str = "", bundle: str = "") -> QColor | None:
    if "bill" not in (category or "").lower():
        return None
    lowered = " ".join([status_group or "", status_label or "", bundle or ""]).lower()
    if "law" in lowered or "passed into law" in lowered:
        return QColor("#d8f3df")
    if "vetoed" in lowered:
        return QColor("#f8d7da")
    if "still in process" in lowered or "pending" in lowered:
        return QColor("#fffdf8")
    if "failed" in lowered or "final disposition" in lowered:
        return QColor("#eceff1")
    return None


def _bill_status_sort_key(status_label: str) -> tuple[int, str]:
    normalized = _normalize_ws(status_label).lower()
    order = {
        "passed into law": 0,
        "vetoed": 1,
        "still in process": 2,
        "failed or other final disposition": 3,
    }
    return (order.get(normalized, 99), normalized)


def _citation_sort_key(citation: str, title: str) -> tuple[object, ...]:
    raw = (citation or "").strip().upper()
    if not raw:
        return (2, (1, (title or "").strip().upper()))

    parts: list[tuple[int, object]] = []
    for tok in re.findall(r"[A-Z]+|\d+", raw):
        if tok.isdigit():
            parts.append((0, int(tok)))
        else:
            parts.append((1, tok))
    return (0, tuple(parts), (title or "").strip().upper())


def _bundle_sort_key(category: str, bundle: str) -> tuple[object, ...]:
    if _is_bill(category):
        match = re.search(r"\b(19|20)\d{2}\b", bundle or "")
        year = int(match.group(0)) if match else 0
        chamber_rank = 0 if "House Bills" in bundle else 1 if "Senate Bills" in bundle else 2
        return (-year, chamber_rank, bundle)
    return (0, bundle)


def _extract_json_string_field(raw: str, key: str) -> str:
    match = re.search(_BILL_JSON_FIELD_RE_TEMPLATE.format(key=re.escape(key)), raw)
    if not match:
        return ""
    try:
        return json.loads(f'"{match.group(1)}"')
    except Exception:
        return match.group(1).replace(r"\/", "/")


def _normalize_paragraphs(text: str) -> list[str]:
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    return [_normalize_ws(part) for part in text.split("\n\n") if _normalize_ws(part)]


def _sentence_excerpt(paragraph: str, *, max_sentences: int = 2, max_chars: int = 520) -> str:
    normalized = _normalize_ws(paragraph)
    if not normalized:
        return ""
    sentences = re.split(r"(?<=[.!?])\s+(?=[A-Z0-9\"'])", normalized)
    excerpt = " ".join(sentences[:max_sentences]).strip()
    if len(excerpt) > max_chars:
        excerpt = excerpt[: max_chars - 1].rstrip() + "..."
    return excerpt


def _chunk_display_paragraph(paragraph: str, *, max_chars: int = 900, max_sentences: int = 5) -> list[str]:
    raw = paragraph.strip()
    if not raw:
        return []
    if "\n" in raw:
        return [raw]

    normalized = _normalize_ws(raw)
    if not normalized:
        return []
    if len(normalized) <= max_chars:
        return [normalized]
    if _is_header_like_paragraph(normalized):
        return [normalized]

    sentences = _split_sentences(normalized)
    if len(sentences) <= 2:
        return [normalized]

    chunks: list[str] = []
    current: list[str] = []
    current_len = 0
    for sentence in sentences:
        sentence = sentence.strip()
        if not sentence:
            continue
        projected_len = current_len + len(sentence) + (1 if current else 0)
        if current and (projected_len > max_chars or len(current) >= max_sentences):
            chunks.append(" ".join(current).strip())
            current = [sentence]
            current_len = len(sentence)
            continue
        current.append(sentence)
        current_len = projected_len

    if current:
        chunks.append(" ".join(current).strip())

    return chunks or [normalized]


def _format_full_text_for_display(text: str) -> str:
    normalized = (text or "").replace("\r\n", "\n").replace("\r", "\n")
    normalized = re.sub(r"\n{3,}", "\n\n", normalized)
    blocks = [block.strip() for block in normalized.split("\n\n") if block.strip()]
    formatted_blocks: list[str] = []
    for block in blocks:
        formatted_blocks.extend(_chunk_display_paragraph(block))
    return "\n\n".join(formatted_blocks).strip()


def _formatted_heading_level(paragraph: str) -> int:
    normalized = _normalize_ws(paragraph)
    if not normalized:
        return 0
    if _BODY_HEADER_RE.match(normalized):
        return 2
    if normalized == "SUPREME COURT OF LOUISIANA":
        return 2
    if re.match(r"^No\.\s", normalized, re.IGNORECASE):
        return 3
    if normalized.isupper() and len(normalized) <= 140 and len(normalized.split()) <= 14:
        return 3
    return 0


def _link_or_text_html(value: str) -> str:
    escaped = html.escape(value)
    if re.match(r"^https?://\S+$", value):
        return f"<a href='{escaped}'>{escaped}</a>"
    return escaped


def _paragraph_to_html(paragraph: str) -> str:
    return html.escape(paragraph).replace("\n", "<br />\n")


def _bill_text_parts(text: str) -> tuple[str, str, str, str]:
    marker = "===== OFFICIAL ACT TEXT ====="
    if marker not in (text or ""):
        return text or "", "", "", ""

    history, official = text.split(marker, 1)
    official = official.strip()
    source_label = ""
    source_url = ""
    body_lines: list[str] = []
    lines = official.splitlines()
    idx = 0
    if idx < len(lines) and lines[idx].strip().casefold().startswith("source:"):
        source_label = lines[idx].split(":", 1)[1].strip()
        idx += 1
    if idx < len(lines) and re.match(r"^https?://", lines[idx].strip()):
        source_url = lines[idx].strip()
        idx += 1
    body_lines = lines[idx:]
    return history.strip(), "\n".join(body_lines).strip(), source_label, source_url


def _clean_bill_text_lines(text: str) -> list[str]:
    cleaned: list[str] = []
    for raw in (text or "").replace("\r\n", "\n").replace("\r", "\n").splitlines():
        line = _WS.sub(" ", raw.replace("\xa0", " ")).strip()
        if not line:
            if cleaned and cleaned[-1]:
                cleaned.append("")
            continue
        line = (
            line.replace("\u00c2\u00a7", "\u00a7")
            .replace("\u00c2", "")
            .replace("\u00e2\u20ac\u00a2", "\u2022")
            .replace("\u00e2\u20ac\u201c", "-")
            .replace("\u00e2\u20ac\u201d", "-")
        )
        if line == "`" or line.isdigit():
            continue
        if re.match(r"^Page\s+\d+\s+of\s+\d+\b", line, re.IGNORECASE):
            continue
        if re.match(r"^Table of Contents\b", line, re.IGNORECASE):
            continue
        if line.casefold().startswith("coding:"):
            continue
        if "struck through" in line.casefold() or "underscored" in line.casefold():
            continue
        if line.casefold() in {"are additions.", "are deletions.", "words underscored", "words underscored are additions."}:
            continue
        if line in {
            "SPEAKER OF THE HOUSE OF REPRESENTATIVES",
            "PRESIDENT OF THE SENATE",
            "GOVERNOR OF THE STATE OF LOUISIANA",
            "APPROVED:",
        }:
            continue
        if re.match(r"^[HS]B\s+NO\.\s+\d+\b", line, re.IGNORECASE):
            continue
        cleaned.append(line)
    while cleaned and not cleaned[-1]:
        cleaned.pop()
    return cleaned


def _skip_bill_front_matter(lines: list[str]) -> list[str]:
    has_toc = any(line.casefold() == "table of contents" or _is_dot_leader_line(line) for line in lines[:80])
    if not has_toc:
        return lines

    for idx, line in enumerate(lines):
        if idx < 20:
            continue
        if re.match(r"^ACT\s+No\.\s+\d+", line, re.IGNORECASE):
            start = idx
            if idx > 0 and lines[idx - 1].casefold() == "enrolled":
                start = idx - 1
            return lines[start:]
    for idx, line in enumerate(lines):
        if idx >= 20 and line.casefold() == "an act":
            start = idx
            while start > 0 and start > idx - 8:
                previous = lines[start - 1]
                if re.match(r"^(ENROLLED|ACT\s+No\.|HOUSE BILL NO\.|SENATE BILL NO\.|BY\s+)", previous, re.IGNORECASE):
                    start -= 1
                    continue
                break
            return lines[start:]
    return lines


def _is_dot_leader_line(line: str) -> bool:
    return ". . ." in line or bool(re.search(r"\.{5,}", line))


def _bill_line_starts_paragraph(line: str) -> bool:
    if not line:
        return True
    patterns = [
        r"^ENROLLED$",
        r"^ACT\s+No\.\s+\d+",
        r"^(19|20)\d{2}\s+",
        r"^(HOUSE|SENATE)\s+BILL\s+NO\.",
        r"^BY\s+",
        r"^AN\s+ACT$",
        r"^Be it enacted\b",
        r"^Section\s+\d+\.",
        r"^\u00a7",
        r"^[A-Z]\.",
        r"^\([A-Za-z0-9]+\)",
        r"^\*\s+\*\s+\*$",
    ]
    return any(re.match(pattern, line, re.IGNORECASE) for pattern in patterns)


def _readable_bill_text(text: str) -> str:
    lines = _skip_bill_front_matter(_clean_bill_text_lines(text))
    filtered_lines: list[str] = []
    seen_enrolled = False
    seen_bill_number = False
    for line in lines:
        if line.casefold() == "enrolled":
            if seen_enrolled:
                continue
            seen_enrolled = True
        if re.match(r"^(HOUSE|SENATE)\s+BILL\s+NO\.", line, re.IGNORECASE):
            if seen_bill_number:
                continue
            seen_bill_number = True
        filtered_lines.append(line)
    lines = filtered_lines

    paragraphs: list[str] = []
    current = ""
    for line in lines:
        if not line:
            if current:
                paragraphs.append(current.strip())
                current = ""
            continue
        if _is_dot_leader_line(line):
            if current:
                paragraphs.append(current.strip())
                current = ""
            paragraphs.append(line)
            continue
        if current and (
            re.match(r"^BY\s+", current, re.IGNORECASE)
            or current.casefold() == "an act"
        ):
            paragraphs.append(current.strip())
            current = line
            continue
        if _bill_line_starts_paragraph(line) or not current:
            if current:
                paragraphs.append(current.strip())
            current = line
            continue
        current = f"{current} {line}".strip()
    if current:
        paragraphs.append(current.strip())
    return "\n\n".join(paragraphs)


def _bill_preview_text(text: str, *, max_paragraphs: int = 90) -> str:
    readable = _readable_bill_text(text)
    if not readable:
        return ""
    paragraphs = [part for part in readable.split("\n\n") if _normalize_ws(part)]
    if len(paragraphs) <= max_paragraphs:
        return readable
    return "\n\n".join(paragraphs[:max_paragraphs])


def _format_bill_text_for_display(text: str) -> str:
    history, official_text, source_label, source_url = _bill_text_parts(text)
    if not official_text:
        return _format_full_text_for_display(text)

    parts = ["Official Act Text"]
    if source_label:
        parts.append(f"Source: {source_label}")
    if source_url:
        parts.append(source_url)
    parts.append("")
    parts.append(_readable_bill_text(official_text))
    if history:
        parts.extend(["", "Bill History", _format_full_text_for_display(history)])
    return "\n".join(part for part in parts if part is not None).strip()


def _bill_text_html_blocks(text: str, *, class_name: str = "bill-text") -> str:
    readable = _bill_preview_text(text) if class_name == "bill-summary-text" else _readable_bill_text(text)
    if not readable:
        return ""
    blocks: list[str] = []
    for paragraph in readable.split("\n\n"):
        normalized = _normalize_ws(paragraph)
        if not normalized:
            continue
        escaped = html.escape(normalized)
        if _is_dot_leader_line(normalized):
            normalized = re.sub(r"\s*\. ?\. ?\.\s*", " . . . ", normalized)
            escaped = html.escape(normalized)
            blocks.append(f"<p class='{class_name} bill-toc-line'>{escaped}</p>")
        elif _bill_line_starts_paragraph(normalized) and (
            normalized.isupper()
            or normalized.startswith("\u00a7")
            or re.match(r"^Section\s+\d+\.", normalized, re.IGNORECASE)
            or re.match(r"^ACT\s+No\.", normalized, re.IGNORECASE)
        ):
            blocks.append(f"<p class='{class_name} bill-heading'>{escaped}</p>")
        else:
            blocks.append(f"<p class='{class_name}'>{escaped}</p>")
    return "\n".join(blocks)


def _build_bill_formatted_text_html(detail: dict[str, str]) -> str:
    raw_text = detail.get("text", "")
    history, official_text, source_label, source_url = _bill_text_parts(raw_text)
    display_text = official_text or raw_text
    if not display_text:
        return "<p>No bill text available.</p>"
    title = html.escape(detail.get("title", "").strip() or detail.get("citation", "").strip() or "Bill")
    citation = html.escape(detail.get("citation", "").strip())
    status = html.escape(detail.get("bill_status_label", "").strip() or detail.get("status_label", "").strip())
    current_status = html.escape(detail.get("current_status", "").strip())
    meta_parts = []
    if citation:
        meta_parts.append(f"<b>{citation}</b>")
    if status:
        meta_parts.append(f"Status: {status}")
    if current_status:
        meta_parts.append(f"Current: {current_status}")
    if source_label:
        meta_parts.append(f"Document: {html.escape(source_label)}")
    meta_html = " | ".join(meta_parts)
    official_html = _bill_text_html_blocks(display_text)
    history_html = ""
    if official_text and history:
        history_html = f"""
        <div class="history-heading">Bill History</div>
        <pre>{html.escape(_format_full_text_for_display(history))}</pre>
        """
    return f"""
    <style>
      body {{
        font-family: "Segoe UI";
        font-size: 13px;
        color: #1f1b16;
        background: #fffdf8;
        margin: 12px;
      }}
      .doc-title {{
        margin: 0 0 8px 0;
        font-size: 18px;
        font-weight: 700;
        line-height: 1.28;
      }}
      .meta {{
        margin: 0 0 16px 0;
        color: #5d4e3a;
        font-size: 12px;
      }}
      a {{
        color: #8b3d1b;
        text-decoration: none;
      }}
      a:hover {{
        text-decoration: underline;
      }}
      .source {{
        margin: -6px 0 18px 0;
        color: #6a5a44;
        font-size: 12px;
      }}
      .bill-text {{
        margin: 0 auto 12px 0;
        max-width: 980px;
        font-family: Georgia, "Times New Roman", serif;
        font-size: 15px;
        line-height: 1.62;
      }}
      .bill-heading {{
        margin: 19px 0 9px 0;
        font-family: "Segoe UI";
        font-weight: 700;
        font-size: 14px;
        color: #4d3d2c;
      }}
      .bill-toc-line {{
        font-family: Consolas, "Courier New", monospace;
        font-size: 12px;
        line-height: 1.35;
        white-space: pre-wrap;
        color: #463b2d;
      }}
      .history-heading {{
        margin: 24px 0 10px 0;
        padding-top: 12px;
        border-top: 1px solid #e2d5c2;
        font-weight: 700;
        color: #6f2f15;
      }}
      pre {{
        white-space: pre-wrap;
        font-family: Consolas, "Courier New", monospace;
        font-size: 12px;
        line-height: 1.45;
        margin: 0;
      }}
    </style>
    <div class="doc-title">{title}</div>
    <div class="meta">{meta_html}</div>
    {f"<div class='source'><a href='{html.escape(source_url)}'>{html.escape(source_url)}</a></div>" if source_url else ""}
    {official_html}
    {history_html}
    """


def _rich_text_blocks(text: str, *, citation: str, title: str) -> list[str]:
    raw_blocks = [block.strip() for block in text.split("\n\n") if block.strip()]
    blocks: list[str] = []
    for block in raw_blocks:
        lines = [line.strip() for line in block.split("\n") if line.strip()]
        if len(lines) <= 1:
            blocks.append(block)
            continue

        structured_lines = 0
        for line in lines:
            normalized = _normalize_ws(line)
            if not normalized:
                continue
            if normalized == citation or normalized == title:
                structured_lines += 1
                continue
            if _META_LINE_RE.match(normalized) or re.match(r"^https?://\S+$", normalized):
                structured_lines += 1
                continue
            if _formatted_heading_level(normalized):
                structured_lines += 1

        if structured_lines >= max(2, len(lines) // 2):
            blocks.extend(lines)
            continue
        blocks.append(block)
    return blocks


def _build_formatted_text_html(detail: dict[str, str]) -> str:
    if _is_bill(detail.get("category", "")):
        return _build_bill_formatted_text_html(detail)

    text = _format_full_text_for_display(detail.get("text", ""))
    if not text:
        return "<p>No document selected.</p>"

    citation = _normalize_ws(detail.get("citation", ""))
    title = _normalize_ws(detail.get("title", ""))
    paragraphs = _rich_text_blocks(text, citation=citation, title=title)
    blocks: list[str] = [
        """
        <style>
          body {
            font-family: "Segoe UI";
            font-size: 13px;
            line-height: 1.55;
            color: #1f1b16;
            background: #fffdf8;
            margin: 12px;
          }
          a {
            color: #8b3d1b;
            text-decoration: none;
          }
          a:hover {
            text-decoration: underline;
          }
          p {
            margin: 0 0 14px 0;
          }
          .doc-citation {
            margin: 0 0 6px 0;
            color: #8b3d1b;
            font-size: 12px;
            font-weight: 700;
            letter-spacing: 0.04em;
            text-transform: uppercase;
          }
          .doc-title {
            margin: 0 0 14px 0;
            font-size: 21px;
            font-weight: 700;
            line-height: 1.25;
          }
          .meta {
            margin: 0 0 8px 0;
            color: #5d4e3a;
          }
          .section-2 {
            margin: 22px 0 10px 0;
            font-size: 16px;
            font-weight: 700;
            color: #6f2f15;
            border-top: 1px solid #e2d5c2;
            padding-top: 12px;
          }
          .section-3 {
            margin: 18px 0 8px 0;
            font-size: 14px;
            font-weight: 700;
            color: #4d3d2c;
          }
        </style>
        """
    ]

    for paragraph in paragraphs:
        normalized = _normalize_ws(paragraph)
        if not normalized:
            continue
        if citation and normalized == citation:
            blocks.append(f"<p class='doc-citation'>{html.escape(normalized)}</p>")
            continue
        if title and normalized == title:
            blocks.append(f"<div class='doc-title'>{html.escape(normalized)}</div>")
            continue

        meta_match = _META_LINE_RE.match(normalized)
        if meta_match:
            label = meta_match.group(1)
            value = meta_match.group(2).strip()
            blocks.append(f"<p class='meta'><b>{html.escape(label)}:</b> {_link_or_text_html(value)}</p>")
            continue

        if re.match(r"^https?://\S+$", normalized):
            blocks.append(f"<p><a href='{html.escape(normalized)}'>{html.escape(normalized)}</a></p>")
            continue

        heading_level = _formatted_heading_level(normalized)
        if heading_level:
            blocks.append(f"<div class='section-{heading_level}'>{_paragraph_to_html(normalized)}</div>")
            continue

        blocks.append(f"<p>{_paragraph_to_html(paragraph)}</p>")

    return "".join(blocks)


def _strip_case_preface(text: str) -> str:
    marker = "SUPREME COURT OF LOUISIANA"
    idx = text.find(marker)
    if idx >= 0:
        return text[idx:]
    return text


def _is_header_like_paragraph(paragraph: str) -> bool:
    normalized = _normalize_ws(paragraph)
    lowered = normalized.lower()
    if not normalized:
        return True
    if lowered.startswith("supreme court of louisiana"):
        return True
    if lowered.startswith("court: ") or lowered.startswith("release date: ") or lowered.startswith("release: "):
        return True
    if lowered.startswith("source: ") or lowered.startswith("source case page: ") or lowered.startswith("source pdf: "):
        return True
    if lowered.startswith("official release page:") or lowered.startswith("official pdf:"):
        return True
    if lowered.startswith("author: ") or lowered.startswith("parish: ") or lowered.startswith("disposition: "):
        return True
    if re.match(r"^no\.\s", normalized, re.IGNORECASE):
        return True
    if normalized.isupper() and len(normalized) < 180:
        return True
    if _BODY_HEADER_RE.match(normalized):
        return True
    if re.match(r"^on (writ|appeal|supervisory writ|certiorari)\b", lowered):
        return True
    if re.match(r"^[A-Z][A-Za-z .,'-]+,\s*(chief justice|justice|j\.)", normalized):
        return True
    return False


def _case_body_paragraphs(text: str) -> list[str]:
    body_text = _strip_case_preface(text)
    out: list[str] = []
    for paragraph in _normalize_paragraphs(body_text):
        if len(paragraph) < 45:
            continue
        if _is_header_like_paragraph(paragraph):
            continue
        out.append(paragraph)
    return out


def _collapse_case_body_text(text: str) -> str:
    body_text = _strip_case_preface(text)
    body_text = body_text.replace("\r\n", "\n").replace("\r", "\n")
    body_text = re.sub(r"\n\s*\d+\s*\n", "\n", body_text)
    body_text = re.sub(r"\n+", "\n", body_text)
    return _normalize_ws(body_text)


def _split_sentences(text: str) -> list[str]:
    normalized = _normalize_ws(text)
    if not normalized:
        return []
    return [part.strip() for part in re.split(r"(?<=[.!?])\s+(?=[A-Z0-9\"'])", normalized) if part.strip()]


def _find_sentence_excerpt(
    sentences: list[str],
    *,
    patterns: list[str],
    reverse: bool = False,
    max_sentences: int = 2,
) -> str:
    iterable = range(len(sentences) - 1, -1, -1) if reverse else range(len(sentences))
    for idx in iterable:
        sentence = sentences[idx]
        for pattern in patterns:
            if re.search(pattern, sentence, re.IGNORECASE):
                end = min(len(sentences), idx + max_sentences)
                return " ".join(sentences[idx:end]).strip()
    return ""


def _find_case_issue(paragraphs: list[str]) -> str:
    patterns = [
        r"\bthe threshold issue\b",
        r"\bthis case addresses\b",
        r"\bthis case\b",
        r"\bthe issue\b",
        r"\bat issue\b",
        r"\bthis appeal\b",
        r"\bthis matter\b",
        r"\bwe are called upon\b",
        r"\bwhether\b",
    ]
    for pattern in patterns:
        for paragraph in paragraphs[:12]:
            if re.search(pattern, paragraph, re.IGNORECASE):
                return paragraph
    return paragraphs[0] if paragraphs else ""


def _find_case_holding(paragraphs: list[str]) -> str:
    patterns = [
        r"\bfor the foregoing reasons\b",
        r"\baccordingly\b",
        r"\bwe hold\b",
        r"\bwe conclude\b",
        r"\bwe find\b",
        r"\btherefore\b",
    ]
    for pattern in patterns:
        for paragraph in reversed(paragraphs):
            if re.search(pattern, paragraph, re.IGNORECASE):
                return paragraph
    return paragraphs[min(1, len(paragraphs) - 1)] if paragraphs else ""


def _candidate_generic_summary_paragraphs(text: str) -> list[str]:
    out: list[str] = []
    for paragraph in _normalize_paragraphs(text):
        if len(paragraph) < 40:
            continue
        if _is_header_like_paragraph(paragraph):
            continue
        out.append(paragraph)
        if len(out) >= 3:
            break
    return out


def _build_case_summary_html(detail: dict[str, str]) -> str:
    title = html.escape(detail.get("title", "").strip() or detail.get("citation", "").strip() or "Case")
    disposition = html.escape(_normalize_ws(detail.get("disposition", "")))
    author = html.escape(_normalize_ws(detail.get("author", "")))
    release_date = html.escape(_normalize_ws(detail.get("release_date", "")))
    parish = html.escape(_normalize_ws(detail.get("parish", "")))
    notes = html.escape(_normalize_ws(detail.get("notes", "")))

    case_text = detail.get("text", "")
    paragraphs = _case_body_paragraphs(case_text)
    sentences = _split_sentences(_collapse_case_body_text(case_text))
    issue_text = _find_sentence_excerpt(
        sentences,
        patterns=[
            r"\bthe threshold issue\b",
            r"\bthis case addresses\b",
            r"\bthis case\b",
            r"\bat issue\b",
            r"\bthe issue\b",
            r"\bthis appeal\b",
            r"\bthis matter\b",
            r"\bwe are called upon\b",
        ],
        max_sentences=2,
    ) or _sentence_excerpt(_find_case_issue(paragraphs))
    holding_text = _find_sentence_excerpt(
        sentences,
        patterns=[
            r"\bfor the foregoing reasons\b",
            r"\baccordingly\b",
            r"\bwe hold\b",
            r"\bwe conclude\b",
            r"\bwe find\b",
            r"\btherefore\b",
        ],
        reverse=True,
        max_sentences=2,
    ) or _sentence_excerpt(_find_case_holding(paragraphs), max_sentences=3)
    issue = html.escape(issue_text)
    holding = html.escape(holding_text)

    sections: list[str] = [f"<h2>{title}</h2>", "<h3>What Was Learned</h3>"]
    if disposition:
        sections.append(f"<p><b>Bottom line:</b> {disposition}</p>")
    if issue:
        sections.append(f"<p><b>Issue:</b> {issue}</p>")
    if holding and holding != issue:
        sections.append(f"<p><b>Holding:</b> {holding}</p>")

    meta_bits = [bit for bit in [release_date, author, parish] if bit]
    if meta_bits:
        sections.append(f"<p><b>Context:</b> {' | '.join(meta_bits)}</p>")
    if notes:
        sections.append(f"<p><b>Separate writings / release notes:</b> {notes}</p>")

    if not paragraphs:
        sections.append("<p>No case-body summary could be extracted from the stored text.</p>")
    return "\n".join(sections)


def _build_generic_summary_html(detail: dict[str, str]) -> str:
    title = html.escape(detail.get("title", "").strip() or detail.get("citation", "").strip() or "Document")
    citation = html.escape(detail.get("citation", "").strip())
    paragraphs = _candidate_generic_summary_paragraphs(detail.get("text", ""))
    parts = [f"<h2>{title}</h2>"]
    if citation and citation != title:
        parts.append(f"<p><b>Citation:</b> {citation}</p>")
    parts.append("<h3>Summary</h3>")
    if paragraphs:
        parts.append(f"<p>{html.escape(_sentence_excerpt(paragraphs[0], max_sentences=3))}</p>")
        if len(paragraphs) > 1:
            parts.append(f"<p>{html.escape(_sentence_excerpt(paragraphs[1], max_sentences=2))}</p>")
    else:
        parts.append("<p>No short summary could be extracted from the stored text.</p>")
    return "\n".join(parts)


def _build_bill_summary_html(detail: dict[str, str]) -> str:
    title = html.escape(detail.get("title", "").strip() or detail.get("citation", "").strip() or "Bill")
    citation = html.escape(detail.get("citation", "").strip())
    author = html.escape(detail.get("author", "").strip())
    current_status = html.escape(detail.get("current_status", "").strip())
    final_disposition = html.escape(detail.get("final_disposition", "").strip())
    status_label = html.escape(detail.get("bill_status_label", "").strip())
    history, official_text, source_label, source_url = _bill_text_parts(detail.get("text", ""))
    readable_text = _bill_text_html_blocks(official_text or history or detail.get("text", ""), class_name="bill-summary-text")

    parts = [
        """
        <style>
          body {
            color: #1f1b16;
            background: #fffdf8;
          }
          h2 {
            margin: 0 0 10px 0;
            font-size: 18px;
            line-height: 1.28;
          }
          h3 {
            margin: 20px 0 10px 0;
            color: #6f2f15;
            font-size: 15px;
          }
          .bill-meta {
            margin: 0 0 5px 0;
            font-size: 12px;
          }
          .bill-source {
            margin: 0 0 18px 0;
            color: #6a5a44;
            font-size: 12px;
          }
          .bill-summary-text {
            margin: 0 auto 12px 0;
            max-width: 980px;
            font-family: Georgia, "Times New Roman", serif;
            font-size: 15px;
            line-height: 1.62;
          }
          .bill-heading {
            margin: 19px 0 9px 0;
            font-family: "Segoe UI";
            font-weight: 700;
            font-size: 14px;
            color: #4d3d2c;
          }
          .bill-toc-line {
            font-family: Consolas, "Courier New", monospace;
            font-size: 12px;
            line-height: 1.35;
            white-space: pre-wrap;
            color: #463b2d;
          }
          a {
            color: #8b3d1b;
            text-decoration: none;
          }
        </style>
        """,
        f"<h2>{title}</h2>",
    ]
    if citation:
        parts.append(f"<p class='bill-meta'><b>Bill:</b> {citation}</p>")
    if status_label:
        parts.append(f"<p class='bill-meta'><b>Outcome:</b> {status_label}</p>")
    if final_disposition:
        parts.append(f"<p class='bill-meta'><b>Final disposition:</b> {final_disposition}</p>")
    if current_status and current_status != final_disposition:
        parts.append(f"<p class='bill-meta'><b>Current status:</b> {current_status}</p>")
    if author:
        parts.append(f"<p class='bill-meta'><b>Author:</b> {author}</p>")
    if official_text:
        parts.append("<h3>Official Act Text</h3>")
        if source_url:
            label = html.escape(source_label or source_url)
            parts.append(
                f"<p class='bill-source'>Source: <a href='{html.escape(source_url)}'>{label}</a></p>"
            )
        parts.append(readable_text or "<p>No readable act text could be extracted.</p>")
    elif readable_text:
        parts.append("<h3>Bill Text / History</h3>")
        parts.append(readable_text)
    else:
        parts.append("<p>No bill text available.</p>")
    return "\n".join(parts)


def _build_metadata_html(detail: dict[str, str]) -> str:
    rows: list[str] = []
    ordered_keys = [
        ("citation", "Citation"),
        ("title", "Title"),
        ("category", "Category"),
        ("bundle", "Bundle"),
        ("court", "Court"),
        ("release_date", "Release Date"),
        ("release_code", "Release"),
        ("author", "Author"),
        ("session_id", "Session"),
        ("chamber", "Chamber"),
        ("current_status", "Current Status"),
        ("final_disposition", "Final Disposition"),
        ("bill_status_label", "Bill Outcome"),
        ("parish", "Parish"),
        ("disposition", "Disposition"),
        ("notes", "Notes"),
        ("url", "Source URL"),
        ("pdf_label", "Official Document"),
        ("pdf_url", "Source PDF"),
        ("local_file", "Local File"),
        ("doc_id", "Doc ID"),
    ]
    for key, label in ordered_keys:
        value = _normalize_ws(detail.get(key, ""))
        if not value:
            continue
        rows.append(
            f"<tr><th align='left' valign='top' style='padding:4px 12px 4px 0;'>{html.escape(label)}</th>"
            f"<td style='padding:4px 0;'>{html.escape(value)}</td></tr>"
        )
    if not rows:
        return "<p>No metadata available.</p>"
    return "<table>" + "".join(rows) + "</table>"


def _build_summary_html(detail: dict[str, str]) -> str:
    if _is_case_law(detail.get("category", "")):
        return _build_case_summary_html(detail)
    if _is_bill(detail.get("category", "")):
        return _build_bill_summary_html(detail)
    return _build_generic_summary_html(detail)


class InfoDialog(QDialog):
    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setWindowTitle("Info")
        self.resize(720, 520)

        layout = QVBoxLayout(self)
        browser = QTextBrowser(self)
        browser.setOpenExternalLinks(True)
        browser.setHtml(
            """
            <h2 style="margin-bottom:8px;">LA Law Browser</h2>
            <p style="margin-top:0;">
              Browse the locally indexed Louisiana law library by category, bundle, and document.
            </p>
            <p>
              This browser uses the text already stored in <code>out/index.sqlite</code> and shows a
              case-law-focused "What Was Learned" view for Louisiana Supreme Court opinions.
              Legislative bill rows are tinted by outcome when bill records have been indexed.
            </p>
            <p>
              Refresh workflow:
            </p>
            <ul>
              <li><code>python scripts\\download_louisiana_laws.py --categories all</code></li>
              <li><code>python scripts\\download_louisiana_case_law.py --years all</code></li>
              <li><code>python scripts\\download_louisiana_bills.py --session all</code></li>
              <li><code>python scripts\\build_search_index.py --rebuild</code></li>
            </ul>
            <p style="margin-top:20px; color:#6b5c45;">
              2026 Vincent Larkin
            </p>
            """
        )
        layout.addWidget(browser, 1)
        buttons = QDialogButtonBox(QDialogButtonBox.Close, parent=self)
        buttons.rejected.connect(self.reject)
        buttons.accepted.connect(self.accept)
        layout.addWidget(buttons)


class LawBrowserWindow(QMainWindow):
    DOC_COLUMNS = ["Citation", "Status", "Title"]

    def __init__(self, db_path: str) -> None:
        super().__init__()
        self.setWindowTitle("LA Law Browser")
        self.resize(1600, 950)

        self._db_path = db_path
        self._summary_cache: dict[int, str] = {}
        self._formatted_cache: dict[int, str] = {}
        self._plain_text_cache: dict[int, str] = {}
        self._metadata_cache: dict[int, str] = {}
        self._detail_cache: dict[int, dict[str, str]] = {}
        self._result_rows: list[dict[str, object]] = []
        self._category_rows: list[dict[str, object]] = []

        self._build_ui()
        self._apply_fonts()
        self._apply_styles()
        self._wire_events()
        self._reload_library()

    def _apply_fonts(self) -> None:
        body_font = QFont("Segoe UI", 10)
        text_font = QFont("Consolas", 10)
        text_font.setStyleHint(QFont.Monospace)
        self.setFont(body_font)
        self.summary_browser.setFont(body_font)
        self.formatted_browser.setFont(body_font)
        self.metadata_browser.setFont(body_font)
        self.full_text_edit.setFont(text_font)

    def _apply_styles(self) -> None:
        self.setStyleSheet(
            """
            QMainWindow, QWidget {
              background: #f5f1e8;
              color: #1f1b16;
              font-size: 12px;
            }
            QGroupBox {
              border: 1px solid #d6cbb8;
              border-radius: 10px;
              margin-top: 12px;
              padding-top: 12px;
              background: #fbf8f2;
              font-weight: 600;
            }
            QGroupBox::title {
              subcontrol-origin: margin;
              left: 10px;
              padding: 0 4px;
            }
            QLineEdit, QPlainTextEdit, QListWidget, QTableWidget, QTextBrowser, QTabWidget::pane {
              background: #fffdf8;
              border: 1px solid #d6cbb8;
              border-radius: 8px;
              selection-background-color: #b65b2c;
              selection-color: #ffffff;
            }
            QComboBox {
              background: #fffdf8;
              border: 1px solid #d6cbb8;
              border-radius: 8px;
              padding: 5px 8px;
              selection-background-color: #b65b2c;
              selection-color: #ffffff;
            }
            QPushButton {
              background: #ece4d6;
              border: 1px solid #cfbfa7;
              border-radius: 8px;
              padding: 6px 10px;
            }
            QPushButton:hover {
              background: #e4d7c2;
            }
            QPushButton:pressed {
              background: #dac9ae;
            }
            QPushButton:disabled {
              color: #8f887b;
              background: #eee8dd;
            }
            QTableWidget {
              gridline-color: #e6dccd;
              alternate-background-color: #f7f2e9;
            }
            QHeaderView::section {
              background: #e8decd;
              border: 0;
              border-right: 1px solid #d9ceba;
              border-bottom: 1px solid #d9ceba;
              padding: 6px 8px;
              font-weight: 600;
            }
            QStatusBar {
              background: #efe7d9;
              border-top: 1px solid #dacfbf;
            }
            QTabBar::tab {
              background: #ece4d6;
              border: 1px solid #cfbfa7;
              border-bottom: 0;
              padding: 7px 12px;
              margin-right: 2px;
              border-top-left-radius: 8px;
              border-top-right-radius: 8px;
            }
            QTabBar::tab:selected {
              background: #fffdf8;
            }
            """
        )

    def _build_ui(self) -> None:
        root = QWidget()
        self.setCentralWidget(root)
        main = QVBoxLayout(root)

        db_row = QHBoxLayout()
        self.db_path_edit = QLineEdit(self._db_path)
        self.db_path_edit.setPlaceholderText("Path to SQLite index (out/index.sqlite)")
        self.browse_btn = QPushButton("Browse DB...")
        self.reload_btn = QPushButton("Reload")
        self.info_btn = QPushButton("Info")
        db_row.addWidget(QLabel("Index DB:"))
        db_row.addWidget(self.db_path_edit, 1)
        db_row.addWidget(self.browse_btn)
        db_row.addWidget(self.reload_btn)
        db_row.addWidget(self.info_btn)
        main.addLayout(db_row)

        splitter = QSplitter(Qt.Horizontal)
        main.addWidget(splitter, 1)

        nav_panel = QWidget()
        nav_layout = QVBoxLayout(nav_panel)
        splitter.addWidget(nav_panel)

        categories_group = QGroupBox("Categories")
        categories_layout = QVBoxLayout(categories_group)
        section_row = QHBoxLayout()
        self.section_combo = QComboBox()
        self.section_combo.addItem(SECTION_ALL, SECTION_ALL)
        section_row.addWidget(QLabel("Section:"))
        section_row.addWidget(self.section_combo, 1)
        categories_layout.addLayout(section_row)
        self.categories_list = QListWidget()
        self.categories_list.setSelectionMode(QListWidget.SingleSelection)
        categories_layout.addWidget(self.categories_list)
        nav_layout.addWidget(categories_group, 1)

        bundles_group = QGroupBox("Bundles")
        bundles_layout = QVBoxLayout(bundles_group)
        self.bundles_filter = QLineEdit()
        self.bundles_filter.setPlaceholderText("Filter bundles")
        self.bundles_list = QListWidget()
        self.bundles_list.setSelectionMode(QListWidget.SingleSelection)
        bundles_layout.addWidget(self.bundles_filter)
        bundles_layout.addWidget(self.bundles_list, 1)
        nav_layout.addWidget(bundles_group, 2)

        docs_group = QGroupBox("Documents")
        docs_layout = QVBoxLayout(docs_group)
        self.docs_filter = QLineEdit()
        self.docs_filter.setPlaceholderText("Filter citation or title within the selected bundle")
        docs_controls = QHBoxLayout()
        self.status_filter_combo = QComboBox()
        for label, value in BILL_STATUS_OPTIONS:
            self.status_filter_combo.addItem(label, value)
        self.status_filter_combo.setEnabled(False)
        self.sort_combo = QComboBox()
        for label, value in DOC_SORT_OPTIONS:
            self.sort_combo.addItem(label, value)
        docs_controls.addWidget(QLabel("Status:"))
        docs_controls.addWidget(self.status_filter_combo, 1)
        docs_controls.addWidget(QLabel("Sort:"))
        docs_controls.addWidget(self.sort_combo, 1)
        self.docs_table = QTableWidget(0, len(self.DOC_COLUMNS))
        self.docs_table.setHorizontalHeaderLabels(self.DOC_COLUMNS)
        self.docs_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.docs_table.setSelectionMode(QTableWidget.SingleSelection)
        self.docs_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.docs_table.setAlternatingRowColors(True)
        self.docs_table.setWordWrap(False)
        self.docs_table.setAutoScroll(False)
        self.docs_table.verticalHeader().setVisible(False)
        self.docs_table.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        self.docs_table.horizontalHeader().setStretchLastSection(True)
        docs_layout.addWidget(self.docs_filter)
        docs_layout.addLayout(docs_controls)
        docs_layout.addWidget(self.docs_table, 1)
        nav_layout.addWidget(docs_group, 3)

        detail_panel = QWidget()
        detail_layout = QVBoxLayout(detail_panel)
        splitter.addWidget(detail_panel)

        self.detail_title = QLabel("Select a document to browse.")
        self.detail_title.setWordWrap(True)
        self.detail_title.setTextInteractionFlags(Qt.TextSelectableByMouse)
        detail_layout.addWidget(self.detail_title)

        path_row = QHBoxLayout()
        self.source_edit = QLineEdit()
        self.source_edit.setReadOnly(True)
        self.source_edit.setPlaceholderText("Source URL")
        self.open_source_btn = QPushButton("Open Source")
        self.open_source_btn.setEnabled(False)
        path_row.addWidget(QLabel("Source:"))
        path_row.addWidget(self.source_edit, 1)
        path_row.addWidget(self.open_source_btn)
        detail_layout.addLayout(path_row)

        local_row = QHBoxLayout()
        self.local_edit = QLineEdit()
        self.local_edit.setReadOnly(True)
        self.local_edit.setPlaceholderText("Local file path when available")
        self.open_local_btn = QPushButton("Open Local File")
        self.open_local_btn.setEnabled(False)
        local_row.addWidget(QLabel("Local:"))
        local_row.addWidget(self.local_edit, 1)
        local_row.addWidget(self.open_local_btn)
        detail_layout.addLayout(local_row)

        self.tabs = QTabWidget()
        self.summary_browser = QTextBrowser()
        self.summary_browser.setOpenExternalLinks(True)
        self.formatted_browser = QTextBrowser()
        self.formatted_browser.setOpenExternalLinks(True)
        self.full_text_edit = QPlainTextEdit()
        self.full_text_edit.setReadOnly(True)
        self.full_text_edit.setLineWrapMode(QPlainTextEdit.WidgetWidth)
        self.full_text_edit.setWordWrapMode(QTextOption.WrapAtWordBoundaryOrAnywhere)
        self.full_text_edit.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.metadata_browser = QTextBrowser()
        self.metadata_browser.setOpenExternalLinks(True)
        self.tabs.addTab(self.summary_browser, "Summary")
        self.tabs.addTab(self.formatted_browser, "Formatted")
        self.tabs.addTab(self.full_text_edit, "Plain Text")
        self.tabs.addTab(self.metadata_browser, "Metadata")
        detail_layout.addWidget(self.tabs, 1)

        splitter.setStretchFactor(0, 0)
        splitter.setStretchFactor(1, 1)
        splitter.setSizes([520, 1080])

        status = QStatusBar()
        self.setStatusBar(status)
        self.statusBar().showMessage("Ready")

    def _wire_events(self) -> None:
        self.browse_btn.clicked.connect(self._browse_db)
        self.reload_btn.clicked.connect(self._reload_library)
        self.info_btn.clicked.connect(self._show_info_dialog)
        self.section_combo.currentIndexChanged.connect(self._populate_categories)
        self.categories_list.itemSelectionChanged.connect(self._on_category_changed)
        self.bundles_filter.textChanged.connect(self._reload_bundles)
        self.bundles_list.itemSelectionChanged.connect(self._on_bundle_changed)
        self.docs_filter.textChanged.connect(self._reload_documents)
        self.status_filter_combo.currentIndexChanged.connect(self._reload_documents)
        self.sort_combo.currentIndexChanged.connect(self._reload_documents)
        self.docs_table.itemSelectionChanged.connect(self._on_document_selected)
        self.docs_table.cellDoubleClicked.connect(lambda _row, _col: self._open_selected_local_or_source())
        self.tabs.currentChanged.connect(lambda _idx: self._render_current_detail_tab())
        self.open_source_btn.clicked.connect(self._open_source)
        self.open_local_btn.clicked.connect(self._open_local)

    def _show_info_dialog(self) -> None:
        dialog = InfoDialog(self)
        dialog.exec()

    def _browse_db(self) -> None:
        start_dir = str(Path(self.db_path_edit.text() or ".").parent)
        file_name, _ = QFileDialog.getOpenFileName(
            self,
            "Select index database",
            start_dir,
            "SQLite files (*.sqlite *.db);;All files (*.*)",
        )
        if file_name:
            self.db_path_edit.setText(file_name)
            self._reload_library()

    def _clear_detail(self, message: str) -> None:
        self.detail_title.setText(message)
        self.source_edit.setText("")
        self.local_edit.setText("")
        self.summary_browser.setHtml(f"<p>{html.escape(message)}</p>")
        self.formatted_browser.setHtml(f"<p>{html.escape(message)}</p>")
        self.full_text_edit.setPlainText("")
        self.metadata_browser.setHtml("<p>No document selected.</p>")
        self.open_source_btn.setEnabled(False)
        self.open_local_btn.setEnabled(False)

    def _reload_library(self) -> None:
        db_path = self.db_path_edit.text().strip()
        if not db_path:
            return
        if not Path(db_path).exists():
            self.statusBar().showMessage(
                f"Index not found at {db_path}. Build it with: python scripts/build_search_index.py --rebuild"
            )
            self.categories_list.clear()
            self.bundles_list.clear()
            self.status_filter_combo.setEnabled(False)
            self.docs_table.setRowCount(0)
            self._clear_detail("Index not found.")
            return

        self._db_path = db_path
        self._summary_cache.clear()
        self._formatted_cache.clear()
        self._plain_text_cache.clear()
        self._metadata_cache.clear()
        self._detail_cache.clear()
        self._result_rows.clear()
        self._category_rows.clear()

        try:
            con = _connect(db_path)
            try:
                rows = con.execute(
                    """
                    SELECT category, COUNT(*) AS doc_count
                    FROM docs_fts
                    WHERE COALESCE(category, '') <> ''
                    GROUP BY category
                    ORDER BY category
                    """
                ).fetchall()
            finally:
                con.close()
        except Exception as exc:
            QMessageBox.critical(self, "DB Error", f"Failed to read categories: {exc}")
            return

        section_counts: dict[str, int] = {}
        for row in rows:
            category = row["category"] or ""
            count = int(row["doc_count"] or 0)
            section = _category_section(category)
            self._category_rows.append({"category": category, "count": count, "section": section})
            section_counts[section] = section_counts.get(section, 0) + count

        self._category_rows.sort(key=lambda row: (_section_sort_key(str(row["section"])), str(row["category"])))
        self._populate_section_combo(section_counts)
        self._populate_categories()

        if not self._category_rows:
            self.bundles_list.clear()
            self.status_filter_combo.setEnabled(False)
            self.docs_table.setRowCount(0)
            self._clear_detail("The index is empty.")
            self.statusBar().showMessage("Loaded index but found 0 categories.")
            return

        row_height = self.categories_list.sizeHintForRow(0)
        if row_height <= 0:
            row_height = max(22, self.categories_list.fontMetrics().height() + 8)
        visible_rows = min(self.categories_list.count(), 8)
        self.categories_list.setMinimumHeight((row_height * visible_rows) + 12)

        self.statusBar().showMessage(f"Loaded library index: {db_path}")

    def _populate_section_combo(self, section_counts: dict[str, int]) -> None:
        current = self.section_combo.currentData() or SECTION_ALL
        self.section_combo.blockSignals(True)
        self.section_combo.clear()
        total = sum(section_counts.values())
        self.section_combo.addItem(f"{SECTION_ALL} ({total:,})", SECTION_ALL)
        for section in sorted(section_counts, key=_section_sort_key):
            self.section_combo.addItem(f"{section} ({section_counts[section]:,})", section)
        index = self.section_combo.findData(current)
        self.section_combo.setCurrentIndex(index if index >= 0 else 0)
        self.section_combo.blockSignals(False)

    def _populate_categories(self) -> None:
        selected_section = self.section_combo.currentData() or SECTION_ALL
        previous_category = self._selected_category()
        self.categories_list.blockSignals(True)
        self.categories_list.clear()
        for row in self._category_rows:
            if selected_section != SECTION_ALL and row["section"] != selected_section:
                continue
            category = str(row["category"])
            count = int(row["count"] or 0)
            item = QListWidgetItem(f"{category} ({count:,})")
            item.setData(Qt.UserRole, category)
            self.categories_list.addItem(item)
            if category == previous_category:
                self.categories_list.setCurrentItem(item)
        if self.categories_list.count() and self.categories_list.currentRow() < 0:
            self.categories_list.setCurrentRow(0)
        self.categories_list.blockSignals(False)

        if self.categories_list.count() == 0:
            self.bundles_list.clear()
            self.docs_table.setRowCount(0)
            self.status_filter_combo.setEnabled(False)
            self._clear_detail("No categories in this section.")
            return
        self._reload_bundles()

    def _selected_category(self) -> str:
        item = self.categories_list.currentItem()
        if item is None:
            return ""
        return str(item.data(Qt.UserRole) or "")

    def _selected_bundle(self) -> str:
        item = self.bundles_list.currentItem()
        if item is None:
            return ""
        return str(item.data(Qt.UserRole) or "")

    def _on_category_changed(self) -> None:
        self._reload_bundles()

    def _reload_bundles(self) -> None:
        category = self._selected_category()
        self.bundles_list.clear()
        self.docs_table.setRowCount(0)
        self._clear_detail("Select a document to browse.")
        is_bill_category = _is_bill(category)
        self.status_filter_combo.blockSignals(True)
        self.status_filter_combo.setEnabled(is_bill_category)
        if not is_bill_category:
            self.status_filter_combo.setCurrentIndex(0)
        self.status_filter_combo.blockSignals(False)
        if not category:
            return

        filter_text = self.bundles_filter.text().strip().lower()
        try:
            con = _connect(self._db_path)
            try:
                rows = con.execute(
                    """
                    SELECT bundle, COUNT(*) AS doc_count
                    FROM docs_fts
                    WHERE category = ? AND COALESCE(bundle, '') <> ''
                    GROUP BY bundle
                    ORDER BY bundle
                    """,
                    (category,),
                ).fetchall()
            finally:
                con.close()
        except Exception as exc:
            QMessageBox.critical(self, "DB Error", f"Failed to read bundles: {exc}")
            return

        sorted_rows = sorted(rows, key=lambda row: _bundle_sort_key(category, row["bundle"] or ""))
        for row in sorted_rows:
            bundle = row["bundle"] or ""
            if filter_text and filter_text not in bundle.lower():
                continue
            count = int(row["doc_count"] or 0)
            item = QListWidgetItem(f"{bundle} ({count:,})")
            item.setData(Qt.UserRole, bundle)
            self.bundles_list.addItem(item)

        if self.bundles_list.count() > 0:
            self.bundles_list.setCurrentRow(0)
        else:
            self.statusBar().showMessage(f"No bundles matched for {category}.")

    def _on_bundle_changed(self) -> None:
        self._reload_documents()

    def _reload_documents(self) -> None:
        category = self._selected_category()
        bundle = self._selected_bundle()
        self.docs_table.blockSignals(True)
        self.docs_table.setRowCount(0)
        self.docs_table.blockSignals(False)
        self._result_rows = []
        self._clear_detail("Select a document to browse.")
        if not category or not bundle:
            return

        is_bill_category = _is_bill(category)
        filter_text = self.docs_filter.text().strip()
        params: list[object] = [category, bundle]
        where_parts: list[str] = []
        if filter_text:
            like = f"%{filter_text}%"
            where_parts.append("(citation LIKE ? OR title LIKE ?)")
            params.extend([like, like])
        if is_bill_category:
            selected_status = str(self.status_filter_combo.currentData() or "")
            if selected_status:
                where_parts.append("status_label = ?")
                params.append(selected_status)
        where = ""
        if where_parts:
            where = "AND " + " AND ".join(where_parts)

        try:
            con = _connect(self._db_path)
            try:
                rows = con.execute(
                    f"""
                    SELECT rowid AS row_id, doc_id, citation, title, status_group, status_label
                    FROM docs_fts
                    WHERE category = ? AND bundle = ? {where}
                    """,
                    params,
                ).fetchall()
            finally:
                con.close()
        except Exception as exc:
            QMessageBox.critical(self, "DB Error", f"Failed to read documents: {exc}")
            return

        docs = [
            {
                "row_id": int(row["row_id"]),
                "doc_id": row["doc_id"] or "",
                "citation": row["citation"] or "",
                "title": row["title"] or "",
                "status_group": row["status_group"] or "",
                "status_label": row["status_label"] or "",
            }
            for row in rows
        ]
        sort_mode = str(self.sort_combo.currentData() or "citation")
        if sort_mode == "status":
            docs.sort(
                key=lambda row: (
                    _bill_status_sort_key(str(row.get("status_label", ""))),
                    _citation_sort_key(str(row["citation"]), str(row["title"])),
                )
            )
        elif sort_mode == "title":
            docs.sort(key=lambda row: (str(row["title"]).strip().upper(), _citation_sort_key(str(row["citation"]), "")))
        else:
            docs.sort(key=lambda row: _citation_sort_key(str(row["citation"]), str(row["title"])))
        self._result_rows = docs

        self.docs_table.blockSignals(True)
        self.docs_table.setUpdatesEnabled(False)
        try:
            self.docs_table.setRowCount(len(docs))
            for row_idx, row in enumerate(docs):
                row_color = _bill_status_color(
                    category,
                    str(row.get("status_group", "")),
                    str(row.get("status_label", "")),
                    bundle,
                )
                citation_item = QTableWidgetItem(str(row["citation"]))
                status_item = QTableWidgetItem(str(row.get("status_label", "")))
                title_item = QTableWidgetItem(str(row["title"]))
                citation_item.setData(Qt.UserRole, row["row_id"])
                status_item.setData(Qt.UserRole, row["row_id"])
                title_item.setData(Qt.UserRole, row["row_id"])
                if row_color is not None:
                    citation_item.setBackground(row_color)
                    status_item.setBackground(row_color)
                    title_item.setBackground(row_color)
                self.docs_table.setItem(row_idx, 0, citation_item)
                self.docs_table.setItem(row_idx, 1, status_item)
                self.docs_table.setItem(row_idx, 2, title_item)

            self.docs_table.resizeColumnsToContents()
            if self.docs_table.columnWidth(2) < 680:
                self.docs_table.setColumnWidth(2, 680)
            self.docs_table.horizontalScrollBar().setValue(0)
            if docs:
                self.docs_table.selectRow(0)
                self.docs_table.setCurrentCell(0, 0)
                self.docs_table.horizontalScrollBar().setValue(0)
        finally:
            self.docs_table.setUpdatesEnabled(True)
            self.docs_table.blockSignals(False)

        status_suffix = ""
        if is_bill_category and str(self.status_filter_combo.currentData() or ""):
            status_suffix = f" matching {self.status_filter_combo.currentText()}"
        self.statusBar().showMessage(f"{len(docs):,} document(s){status_suffix} in {bundle}")
        if docs:
            self._on_document_selected()

    def _resolve_out_root(self) -> Path:
        return Path(self._db_path).resolve().parent

    def _resolve_local_file_path(self, raw_path: str) -> str:
        value = raw_path.strip()
        if not value:
            return ""
        path = Path(value)
        if not path.is_absolute():
            path = self._resolve_out_root() / path
        return str(path)

    def _candidate_meta_paths(self, detail: dict[str, str]) -> list[Path]:
        doc_id = detail.get("doc_id", "").strip()
        category = detail.get("category", "").strip()
        bundle = detail.get("bundle", "").strip()
        out_root = self._resolve_out_root()
        candidates: list[Path] = []

        local_file = detail.get("local_file", "").strip()
        if local_file:
            local_path = Path(self._resolve_local_file_path(local_file))
            candidates.append(local_path.with_suffix(".json"))

        if doc_id and category:
            cat_dir = out_root / safe_name(category)
            if bundle:
                candidates.append(cat_dir / safe_name(bundle) / "sections" / f"{doc_id}.json")
            candidates.append(cat_dir / "sections" / f"{doc_id}.json")
        return candidates

    def _load_sidecar_metadata(self, detail: dict[str, str]) -> dict[str, str]:
        if _is_bill(detail.get("category", "")):
            return self._load_bill_sidecar_metadata(detail)

        for path in self._candidate_meta_paths(detail):
            if not path.exists():
                continue
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            out: dict[str, str] = {}
            for key, value in raw.items():
                if isinstance(value, str):
                    out[key] = value
            return out
        return {}

    def _load_bill_sidecar_metadata(self, detail: dict[str, str]) -> dict[str, str]:
        wanted = [
            "author",
            "session_id",
            "session_name",
            "chamber",
            "chamber_label",
            "current_status",
            "final_disposition",
            "bill_status_group",
            "bill_status_label",
            "bill_print_url",
            "pdf_label",
            "pdf_url",
        ]
        for path in self._candidate_meta_paths(detail):
            if not path.exists():
                continue
            try:
                with path.open("r", encoding="utf-8") as handle:
                    raw = handle.read(16_384)
            except Exception:
                continue
            out: dict[str, str] = {}
            for key in wanted:
                value = _extract_json_string_field(raw, key)
                if value:
                    out[key] = value
            if out:
                return out
        return {}

    def _fetch_detail(self, row_id: int) -> dict[str, str] | None:
        cached = self._detail_cache.get(row_id)
        if cached is not None:
            return cached

        try:
            con = _connect(self._db_path)
            try:
                row = con.execute(
                    """
                    SELECT doc_id, citation, title, category, bundle, status_group, status_label, url, local_file, text
                    FROM docs_fts
                    WHERE rowid = ?
                    LIMIT 1
                    """,
                    (row_id,),
                ).fetchone()
            finally:
                con.close()
        except Exception as exc:
            self.statusBar().showMessage(f"Detail load failed: {exc}")
            return None

        if row is None:
            return None

        detail = {
            "doc_id": row["doc_id"] or "",
            "citation": row["citation"] or "",
            "title": row["title"] or "",
            "category": row["category"] or "",
            "bundle": row["bundle"] or "",
            "status_group": row["status_group"] or "",
            "status_label": row["status_label"] or "",
            "bill_status_label": row["status_label"] or "",
            "url": row["url"] or "",
            "local_file": row["local_file"] or "",
            "text": row["text"] or "",
        }
        detail.update(self._load_sidecar_metadata(detail))
        self._detail_cache[row_id] = detail
        return detail

    def _selected_row_id(self) -> int:
        idx = self.docs_table.currentRow()
        if idx < 0 or idx >= len(self._result_rows):
            return -1
        row_id = self._result_rows[idx].get("row_id")
        return int(row_id) if isinstance(row_id, int) else -1

    def _render_current_detail_tab(self) -> None:
        row_id = self._selected_row_id()
        if row_id < 0:
            return
        detail = self._fetch_detail(row_id)
        if detail is None:
            return

        current = self.tabs.currentWidget()
        if current is self.summary_browser:
            summary_html = self._summary_cache.get(row_id)
            if summary_html is None:
                summary_html = _build_summary_html(detail)
                self._summary_cache[row_id] = summary_html
            self.summary_browser.setHtml(summary_html)
        elif current is self.formatted_browser:
            formatted_html = self._formatted_cache.get(row_id)
            if formatted_html is None:
                formatted_html = _build_formatted_text_html(detail)
                self._formatted_cache[row_id] = formatted_html
            self.formatted_browser.setHtml(formatted_html)
        elif current is self.full_text_edit:
            plain_text = self._plain_text_cache.get(row_id)
            if plain_text is None:
                if _is_bill(detail.get("category", "")):
                    plain_text = _format_bill_text_for_display(detail.get("text", ""))
                else:
                    plain_text = _format_full_text_for_display(detail.get("text", ""))
                self._plain_text_cache[row_id] = plain_text
            self.full_text_edit.setPlainText(plain_text)
        elif current is self.metadata_browser:
            metadata_html = self._metadata_cache.get(row_id)
            if metadata_html is None:
                metadata_html = _build_metadata_html(detail)
                self._metadata_cache[row_id] = metadata_html
            self.metadata_browser.setHtml(metadata_html)

    def _on_document_selected(self) -> None:
        row_id = self._selected_row_id()
        if row_id < 0:
            self._clear_detail("Select a document to browse.")
            return

        current_row = self.docs_table.currentRow()
        if current_row >= 0:
            anchor_item = self.docs_table.item(current_row, 0)
            if anchor_item is not None and self.docs_table.currentColumn() != 0:
                self.docs_table.setCurrentItem(anchor_item)
            self.docs_table.horizontalScrollBar().setValue(0)

        detail = self._fetch_detail(row_id)
        if detail is None:
            self._clear_detail("Could not load the selected document.")
            return

        title_bits = [bit for bit in [detail.get("citation", "").strip(), detail.get("title", "").strip()] if bit]
        scope_bits = [bit for bit in [detail.get("category", "").strip(), detail.get("bundle", "").strip()] if bit]
        status_label = detail.get("bill_status_label", "").strip() or detail.get("status_label", "").strip()
        header = " | ".join(title_bits)
        if scope_bits:
            header = f"{header}\n{' / '.join(scope_bits)}" if header else " / ".join(scope_bits)
        if status_label:
            header = f"{header}\nStatus: {status_label}" if header else f"Status: {status_label}"
        self.detail_title.setText(header or "Document")

        source_url = detail.get("pdf_url", "").strip() or detail.get("url", "").strip()
        local_path = self._resolve_local_file_path(detail.get("local_file", ""))
        self.source_edit.setText(source_url)
        self.local_edit.setText(local_path)
        self.open_source_btn.setEnabled(bool(source_url))
        self.open_local_btn.setEnabled(bool(local_path and Path(local_path).exists()))

        self._render_current_detail_tab()
        self.statusBar().showMessage(f"Loaded {detail.get('citation', '').strip() or detail.get('title', '').strip()}")

    def _open_url(self, url: str) -> None:
        if not url:
            return
        QDesktopServices.openUrl(QUrl(url))

    def _open_local_path(self, path: str) -> None:
        if not path:
            return
        file_path = Path(path)
        if not file_path.exists():
            self.statusBar().showMessage(f"Local file not found: {path}")
            return
        QDesktopServices.openUrl(QUrl.fromLocalFile(str(file_path)))

    def _open_source(self) -> None:
        self._open_url(self.source_edit.text().strip())

    def _open_local(self) -> None:
        self._open_local_path(self.local_edit.text().strip())

    def _open_selected_local_or_source(self) -> None:
        local_path = self.local_edit.text().strip()
        if local_path:
            self._open_local_path(local_path)
            return
        self._open_source()


def main(argv: list[str] | None = None) -> int:
    default_db = _resolve_default_db_path()
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=default_db, help=f"SQLite DB path (default: {default_db})")
    args = parser.parse_args(argv)

    app = QApplication([])
    app.setFont(QFont("Segoe UI", 10))
    window = LawBrowserWindow(args.db)
    window.show()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())
