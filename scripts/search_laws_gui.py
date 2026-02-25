"""
Desktop GUI for searching Louisiana laws with near-live results.

Features:
- Fast FTS5 search using the existing out/index.sqlite index.
- Optional regex mode.
- Filter scope by category and bundle, or search all.
- Double-click result URL to open in browser.

Run:
  python scripts/search_laws_gui.py
  python scripts/search_laws_gui.py --db out/index.sqlite
"""

from __future__ import annotations

import argparse
import re
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path

from PySide6.QtCore import QThread, QTimer, Qt, QUrl, Signal
from PySide6.QtGui import QColor, QDesktopServices, QTextCharFormat, QTextCursor
from PySide6.QtWidgets import (
    QApplication,
    QCheckBox,
    QFileDialog,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QMainWindow,
    QMessageBox,
    QPlainTextEdit,
    QPushButton,
    QSpinBox,
    QSplitter,
    QStatusBar,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)


@dataclass
class SearchFilters:
    db_path: str
    query: str
    regex_mode: bool
    case_sensitive: bool
    categories: list[str]
    bundles: list[str]
    limit: int


def _connect(db_path: str) -> sqlite3.Connection:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    return con


def _sql_in_clause(values: list[str]) -> tuple[str, list[str]]:
    if not values:
        return "", []
    placeholders = ",".join("?" for _ in values)
    return f"({placeholders})", list(values)


def _snippet_for_regex(text: str, match: re.Match[str], pad_left: int = 90, pad_right: int = 140) -> str:
    start = max(0, match.start() - pad_left)
    end = min(len(text), match.end() + pad_right)
    chunk = text[start:end].replace("\n", " ").replace("\r", " ")

    # Insert lightweight markers for the matched span.
    rel_start = max(0, match.start() - start)
    rel_end = min(len(chunk), rel_start + (match.end() - match.start()))
    if rel_start < len(chunk):
        chunk = chunk[:rel_start] + "[" + chunk[rel_start:rel_end] + "]" + chunk[rel_end:]

    if start > 0:
        chunk = "... " + chunk
    if end < len(text):
        chunk = chunk + " ..."
    return chunk


def _run_search(filters: SearchFilters) -> tuple[list[dict[str, object]], float]:
    t0 = time.perf_counter()
    query = filters.query.strip()
    if not query:
        return [], 0.0

    where_parts: list[str] = []
    params: list[object] = []

    if filters.categories:
        in_clause, in_params = _sql_in_clause(filters.categories)
        where_parts.append(f"category IN {in_clause}")
        params.extend(in_params)

    if filters.bundles:
        in_clause, in_params = _sql_in_clause(filters.bundles)
        where_parts.append(f"bundle IN {in_clause}")
        params.extend(in_params)

    where_sql = ""
    if where_parts:
        where_sql = " AND " + " AND ".join(where_parts)

    out: list[dict[str, object]] = []
    con = _connect(filters.db_path)
    try:
        if not filters.regex_mode:
            sql = f"""
            SELECT
              rowid AS row_id,
              doc_id,
              category,
              bundle,
              citation,
              title,
              url,
              snippet(docs_fts, 5, '[', ']', ' ... ', 12) AS snippet
            FROM docs_fts
            WHERE docs_fts MATCH ? {where_sql}
            ORDER BY bm25(docs_fts)
            LIMIT ?;
            """
            rows = con.execute(sql, [query, *params, int(filters.limit)]).fetchall()
            for r in rows:
                out.append(
                    {
                        "row_id": int(r["row_id"]),
                        "doc_id": r["doc_id"] or "",
                        "citation": r["citation"] or "",
                        "title": r["title"] or "",
                        "category": r["category"] or "",
                        "bundle": r["bundle"] or "",
                        "url": r["url"] or "",
                        "snippet": (r["snippet"] or "").strip(),
                    }
                )
        else:
            flags = 0 if filters.case_sensitive else re.IGNORECASE
            pattern = re.compile(query, flags)
            scan_limit = max(1000, int(filters.limit) * 60)
            sql = f"""
            SELECT
              rowid AS row_id,
              doc_id,
              category,
              bundle,
              citation,
              title,
              url,
              text
            FROM docs_fts
            WHERE 1=1 {where_sql}
            LIMIT ?;
            """
            rows = con.execute(sql, [*params, scan_limit]).fetchall()
            for r in rows:
                haystacks = [
                    r["citation"] or "",
                    r["title"] or "",
                    r["text"] or "",
                ]
                match_obj: re.Match[str] | None = None
                matched_text = ""
                for hay in haystacks:
                    m = pattern.search(hay)
                    if m is not None:
                        match_obj = m
                        matched_text = hay
                        break
                if match_obj is None:
                    continue

                snippet = _snippet_for_regex(matched_text, match_obj)
                out.append(
                    {
                        "row_id": int(r["row_id"]),
                        "doc_id": r["doc_id"] or "",
                        "citation": r["citation"] or "",
                        "title": r["title"] or "",
                        "category": r["category"] or "",
                        "bundle": r["bundle"] or "",
                        "url": r["url"] or "",
                        "snippet": snippet,
                    }
                )
                if len(out) >= filters.limit:
                    break
    finally:
        con.close()

    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    return out, elapsed_ms


class SearchThread(QThread):
    completed = Signal(int, object, float, str)

    def __init__(self, request_id: int, filters: SearchFilters) -> None:
        super().__init__()
        self.request_id = request_id
        self.filters = filters

    def run(self) -> None:
        try:
            rows, elapsed_ms = _run_search(self.filters)
            self.completed.emit(self.request_id, rows, elapsed_ms, "")
        except Exception as exc:  # show user-friendly errors in GUI
            self.completed.emit(self.request_id, [], 0.0, str(exc))


class SearchWindow(QMainWindow):
    COLUMNS = ["Citation", "Title", "Category", "Bundle", "Snippet", "URL"]
    MAX_PREVIEW_CHARS = 350_000
    MAX_HIGHLIGHT_SPANS = 1200

    def __init__(self, db_path: str) -> None:
        super().__init__()
        self.setWindowTitle("LA Law Live Search")
        self.resize(1500, 900)

        self._request_counter = 0
        self._active_thread: SearchThread | None = None
        self._pending_filters: SearchFilters | None = None
        self._db_path = db_path
        self._result_rows: list[dict[str, object]] = []
        self._detail_cache: dict[int, dict[str, str]] = {}

        self._build_ui()
        self._wire_events()
        self._reload_scope_lists()

    def _build_ui(self) -> None:
        root = QWidget()
        self.setCentralWidget(root)
        main = QVBoxLayout(root)

        # Database row
        db_row = QHBoxLayout()
        self.db_path_edit = QLineEdit(self._db_path)
        self.db_path_edit.setPlaceholderText("Path to SQLite FTS index (out/index.sqlite)")
        self.browse_btn = QPushButton("Browse DB...")
        self.reload_scope_btn = QPushButton("Reload Filters")
        db_row.addWidget(QLabel("Index DB:"))
        db_row.addWidget(self.db_path_edit, 1)
        db_row.addWidget(self.browse_btn)
        db_row.addWidget(self.reload_scope_btn)
        main.addLayout(db_row)

        # Query row
        query_row = QHBoxLayout()
        self.query_edit = QLineEdit()
        self.query_edit.setPlaceholderText('Type search (e.g. "capital punishment" or regex)')
        self.regex_cb = QCheckBox("Regex")
        self.case_cb = QCheckBox("Case sensitive")
        self.case_cb.setEnabled(False)
        self.limit_spin = QSpinBox()
        self.limit_spin.setRange(1, 1000)
        self.limit_spin.setValue(100)
        self.search_btn = QPushButton("Search Now")
        query_row.addWidget(QLabel("Query:"))
        query_row.addWidget(self.query_edit, 1)
        query_row.addWidget(self.regex_cb)
        query_row.addWidget(self.case_cb)
        query_row.addWidget(QLabel("Limit:"))
        query_row.addWidget(self.limit_spin)
        query_row.addWidget(self.search_btn)
        main.addLayout(query_row)

        # Filters + results
        splitter = QSplitter(Qt.Horizontal)
        main.addWidget(splitter, 1)

        filters_panel = QWidget()
        filters_layout = QVBoxLayout(filters_panel)
        splitter.addWidget(filters_panel)

        categories_group = QGroupBox("Categories")
        categories_layout = QVBoxLayout(categories_group)
        cat_buttons = QHBoxLayout()
        self.cat_all_btn = QPushButton("All")
        self.cat_none_btn = QPushButton("None")
        cat_buttons.addWidget(self.cat_all_btn)
        cat_buttons.addWidget(self.cat_none_btn)
        categories_layout.addLayout(cat_buttons)
        self.categories_list = QListWidget()
        self.categories_list.setSelectionMode(QListWidget.MultiSelection)
        categories_layout.addWidget(self.categories_list)
        filters_layout.addWidget(categories_group, 1)

        bundles_group = QGroupBox("Bundles")
        bundles_layout = QVBoxLayout(bundles_group)
        bun_buttons = QHBoxLayout()
        self.bun_all_btn = QPushButton("All")
        self.bun_none_btn = QPushButton("None")
        bun_buttons.addWidget(self.bun_all_btn)
        bun_buttons.addWidget(self.bun_none_btn)
        bundles_layout.addLayout(bun_buttons)
        self.bundles_list = QListWidget()
        self.bundles_list.setSelectionMode(QListWidget.MultiSelection)
        bundles_layout.addWidget(self.bundles_list)
        filters_layout.addWidget(bundles_group, 1)

        self.results_table = QTableWidget(0, len(self.COLUMNS))
        self.results_table.setHorizontalHeaderLabels(self.COLUMNS)
        self.results_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.results_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.results_table.setAlternatingRowColors(True)
        self.results_table.setWordWrap(False)
        self.results_table.horizontalHeader().setStretchLastSection(True)
        right_splitter = QSplitter(Qt.Vertical)
        right_splitter.addWidget(self.results_table)

        preview_panel = QWidget()
        preview_layout = QVBoxLayout(preview_panel)
        self.preview_meta = QLabel("Select a result to view full local text.")
        self.preview_meta.setWordWrap(True)
        preview_layout.addWidget(self.preview_meta)

        preview_url_row = QHBoxLayout()
        self.preview_url_edit = QLineEdit()
        self.preview_url_edit.setReadOnly(True)
        self.preview_url_edit.setPlaceholderText("Source URL (optional)")
        self.open_source_btn = QPushButton("Open Source URL")
        self.open_source_btn.setEnabled(False)
        preview_url_row.addWidget(QLabel("Source URL:"))
        preview_url_row.addWidget(self.preview_url_edit, 1)
        preview_url_row.addWidget(self.open_source_btn)
        preview_layout.addLayout(preview_url_row)

        self.preview_text = QPlainTextEdit()
        self.preview_text.setReadOnly(True)
        self.preview_text.setPlaceholderText("Full local document text will appear here")
        preview_layout.addWidget(self.preview_text, 1)

        right_splitter.addWidget(preview_panel)
        right_splitter.setStretchFactor(0, 1)
        right_splitter.setStretchFactor(1, 1)
        right_splitter.setSizes([430, 410])

        splitter.addWidget(right_splitter)
        splitter.setStretchFactor(0, 0)
        splitter.setStretchFactor(1, 1)
        splitter.setSizes([330, 1100])

        status = QStatusBar()
        self.setStatusBar(status)
        self.statusBar().showMessage("Ready")

        self._debounce = QTimer(self)
        self._debounce.setInterval(180)
        self._debounce.setSingleShot(True)

    def _wire_events(self) -> None:
        self.browse_btn.clicked.connect(self._browse_db)
        self.reload_scope_btn.clicked.connect(self._reload_scope_lists)
        self.search_btn.clicked.connect(self._queue_search)
        self.query_edit.textChanged.connect(lambda _text: self._debounce.start())
        self._debounce.timeout.connect(self._queue_search)
        self.limit_spin.valueChanged.connect(lambda _: self._queue_search())

        self.regex_cb.toggled.connect(self._on_regex_toggled)
        self.regex_cb.toggled.connect(lambda _: self._queue_search())
        self.case_cb.toggled.connect(lambda _: self._queue_search())

        self.cat_all_btn.clicked.connect(lambda: self._select_all(self.categories_list, True))
        self.cat_none_btn.clicked.connect(lambda: self._select_all(self.categories_list, False))
        self.bun_all_btn.clicked.connect(lambda: self._select_all(self.bundles_list, True))
        self.bun_none_btn.clicked.connect(lambda: self._select_all(self.bundles_list, False))

        self.categories_list.itemSelectionChanged.connect(self._on_categories_changed)
        self.bundles_list.itemSelectionChanged.connect(self._queue_search)

        self.results_table.cellDoubleClicked.connect(self._open_result_url)
        self.results_table.itemSelectionChanged.connect(self._on_result_selected)
        self.open_source_btn.clicked.connect(self._open_preview_url)

    def _on_regex_toggled(self, checked: bool) -> None:
        self.case_cb.setEnabled(checked)

    def _selected_texts(self, widget: QListWidget) -> list[str]:
        return [item.text() for item in widget.selectedItems()]

    def _select_all(self, widget: QListWidget, value: bool) -> None:
        for i in range(widget.count()):
            item = widget.item(i)
            item.setSelected(value)
        self._queue_search()

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
            self._reload_scope_lists()

    def _reload_scope_lists(self) -> None:
        db_path = self.db_path_edit.text().strip()
        if not db_path:
            return
        if not Path(db_path).exists():
            self.statusBar().showMessage(
                f"Index not found at {db_path}. Build it with: python scripts/build_search_index.py --rebuild"
            )
            self.categories_list.clear()
            self.bundles_list.clear()
            self.results_table.setRowCount(0)
            self._clear_preview("Local preview unavailable: index not found.")
            return

        try:
            con = _connect(db_path)
            try:
                total_rows = con.execute("SELECT COUNT(*) FROM docs_fts").fetchone()[0]
                categories = [
                    r[0]
                    for r in con.execute(
                        "SELECT DISTINCT category FROM docs_fts WHERE COALESCE(category, '') <> '' ORDER BY category"
                    ).fetchall()
                ]
                bundles = [
                    r[0]
                    for r in con.execute(
                        "SELECT DISTINCT bundle FROM docs_fts WHERE COALESCE(bundle, '') <> '' ORDER BY bundle"
                    ).fetchall()
                ]
            finally:
                con.close()
        except Exception as exc:
            QMessageBox.critical(self, "DB Error", f"Failed to read index: {exc}")
            return

        self._db_path = db_path
        self.categories_list.clear()
        self.bundles_list.clear()
        for c in categories:
            QListWidgetItem(c, self.categories_list)
        for b in bundles:
            QListWidgetItem(b, self.bundles_list)

        if total_rows == 0:
            self.statusBar().showMessage(
                "Index loaded but empty (0 docs). Rebuild with: python scripts/build_search_index.py --rebuild"
            )
            self.results_table.setRowCount(0)
            self._clear_preview("This index has 0 documents.")
            return

        self._select_all(self.categories_list, True)
        self._select_all(self.bundles_list, True)
        self.statusBar().showMessage(f"Loaded index: {db_path} ({total_rows} docs)")
        self._queue_search()

    def _on_categories_changed(self) -> None:
        db_path = self.db_path_edit.text().strip()
        if not db_path or not Path(db_path).exists():
            return

        selected_categories = self._selected_texts(self.categories_list)
        if not selected_categories:
            self.bundles_list.clear()
            self._queue_search()
            return

        in_clause, params = _sql_in_clause(selected_categories)
        sql = f"""
        SELECT DISTINCT bundle
        FROM docs_fts
        WHERE COALESCE(bundle, '') <> '' AND category IN {in_clause}
        ORDER BY bundle
        """

        prev_selected = set(self._selected_texts(self.bundles_list))
        try:
            con = _connect(db_path)
            try:
                bundles = [r[0] for r in con.execute(sql, params).fetchall()]
            finally:
                con.close()
        except Exception as exc:
            QMessageBox.critical(self, "DB Error", f"Failed to read bundles: {exc}")
            return

        self.bundles_list.clear()
        for b in bundles:
            item = QListWidgetItem(b, self.bundles_list)
            if b in prev_selected:
                item.setSelected(True)

        # If nothing remained selected after category narrowing, default to all.
        if self.bundles_list.count() > 0 and not self._selected_texts(self.bundles_list):
            self._select_all(self.bundles_list, True)
        else:
            self._queue_search()

    def _build_filters(self) -> SearchFilters:
        return SearchFilters(
            db_path=self.db_path_edit.text().strip(),
            query=self.query_edit.text(),
            regex_mode=self.regex_cb.isChecked(),
            case_sensitive=self.case_cb.isChecked(),
            categories=self._selected_texts(self.categories_list),
            bundles=self._selected_texts(self.bundles_list),
            limit=int(self.limit_spin.value()),
        )

    def _queue_search(self) -> None:
        filters = self._build_filters()
        self._pending_filters = filters
        if self._active_thread is None:
            self._start_next_search()

    def _start_next_search(self) -> None:
        filters = self._pending_filters
        if filters is None:
            return
        self._pending_filters = None

        db_path = filters.db_path.strip()
        if not db_path:
            return
        if not Path(db_path).exists():
            self.statusBar().showMessage(
                f"Index not found at {db_path}. Build it with: python scripts/build_search_index.py --rebuild"
            )
            self.results_table.setRowCount(0)
            return

        self._request_counter += 1
        request_id = self._request_counter
        self.statusBar().showMessage("Searching...")

        thread = SearchThread(request_id, filters)
        thread.completed.connect(self._on_search_completed)
        thread.finished.connect(thread.deleteLater)
        self._active_thread = thread
        thread.start()

    def _on_search_completed(self, request_id: int, rows: list[dict[str, object]], elapsed_ms: float, err: str) -> None:
        self._active_thread = None

        if request_id != self._request_counter:
            if self._pending_filters is not None:
                self._start_next_search()
            return

        if err:
            self.statusBar().showMessage(f"Search error: {err}")
            self.results_table.setRowCount(0)
            self._clear_preview("Search error. Fix query and try again.")
        else:
            self._populate_table(rows)
            self.statusBar().showMessage(f"{len(rows)} result(s) in {elapsed_ms:.1f} ms")

        if self._pending_filters is not None:
            self._start_next_search()

    def _populate_table(self, rows: list[dict[str, object]]) -> None:
        rows = self._sort_rows_by_citation(rows)
        self._result_rows = rows
        self._detail_cache.clear()
        self.results_table.setRowCount(0)
        self.results_table.setRowCount(len(rows))
        if not rows:
            self._clear_preview("No results. Try a broader query.")
            return

        for r_idx, row in enumerate(rows):
            values = [
                str(row.get("citation", "")),
                str(row.get("title", "")),
                str(row.get("category", "")),
                str(row.get("bundle", "")),
                str(row.get("snippet", "")),
                str(row.get("url", "")),
            ]
            for c_idx, val in enumerate(values):
                item = QTableWidgetItem(val)
                if c_idx == 5:
                    item.setForeground(Qt.blue)
                self.results_table.setItem(r_idx, c_idx, item)
        self.results_table.resizeColumnsToContents()
        if self.results_table.columnWidth(4) > 640:
            self.results_table.setColumnWidth(4, 640)
        self.results_table.selectRow(0)
        self._on_result_selected()

    def _clear_preview(self, message: str) -> None:
        self.preview_meta.setText(message)
        self.preview_url_edit.setText("")
        self.open_source_btn.setEnabled(False)
        self.preview_text.setPlainText("")
        self.preview_text.setExtraSelections([])

    def _fts_highlight_terms(self, query: str) -> list[str]:
        # Accept quoted phrases and plain terms; skip FTS operators.
        terms: list[str] = []
        for phrase in re.findall(r'"([^"]+)"', query):
            phrase = phrase.strip()
            if phrase:
                terms.append(phrase)

        operator_words = {"AND", "OR", "NOT", "NEAR"}
        for word in re.findall(r"[A-Za-z0-9_]+", query):
            if word.upper() in operator_words:
                continue
            if len(word) < 2:
                continue
            terms.append(word)

        # Preserve order, dedupe.
        seen: set[str] = set()
        out: list[str] = []
        for t in terms:
            key = t.casefold()
            if key in seen:
                continue
            seen.add(key)
            out.append(t)
        return out[:12]

    def _citation_sort_key(self, citation: str, title: str) -> tuple[object, ...]:
        raw = (citation or "").strip().upper()
        if not raw:
            return (2, (1, (title or "").strip().upper()))

        # Natural-ish sort for citations like "RS 14:30" and "RS 34:242.1".
        parts: list[tuple[int, object]] = []
        for tok in re.findall(r"[A-Z]+|\d+", raw):
            if tok.isdigit():
                parts.append((0, int(tok)))
            else:
                parts.append((1, tok))
        return (0, tuple(parts), (title or "").strip().upper())

    def _sort_rows_by_citation(self, rows: list[dict[str, object]]) -> list[dict[str, object]]:
        return sorted(
            rows,
            key=lambda row: self._citation_sort_key(
                str(row.get("citation", "")),
                str(row.get("title", "")),
            ),
        )

    def _build_highlight_patterns(self) -> list[re.Pattern[str]]:
        query = self.query_edit.text().strip()
        if not query:
            return []

        if self.regex_cb.isChecked():
            flags = 0 if self.case_cb.isChecked() else re.IGNORECASE
            try:
                return [re.compile(query, flags)]
            except re.error:
                return []

        flags = re.IGNORECASE
        patterns: list[re.Pattern[str]] = []
        for term in self._fts_highlight_terms(query):
            try:
                patterns.append(re.compile(re.escape(term), flags))
            except re.error:
                continue
        return patterns

    def _find_match_spans(self, text: str, max_spans: int) -> list[tuple[int, int]]:
        spans: list[tuple[int, int]] = []
        seen: set[tuple[int, int]] = set()
        for pat in self._build_highlight_patterns():
            for m in pat.finditer(text):
                if m.start() == m.end():
                    continue
                span = (m.start(), m.end())
                if span in seen:
                    continue
                seen.add(span)
                spans.append(span)
                if len(spans) >= max_spans:
                    break
            if len(spans) >= max_spans:
                break
        spans.sort(key=lambda x: x[0])
        return spans

    def _choose_preview_text(self, full_text: str) -> tuple[str, bool]:
        if len(full_text) <= self.MAX_PREVIEW_CHARS:
            return full_text, False

        # Keep preview snappy for huge docs; center around first hit when possible.
        patterns = self._build_highlight_patterns()
        first_match_at = -1
        for pat in patterns:
            m = pat.search(full_text)
            if m is None:
                continue
            if first_match_at < 0 or m.start() < first_match_at:
                first_match_at = m.start()

        if first_match_at >= 0:
            start = max(0, first_match_at - 5000)
        else:
            start = 0
        end = min(len(full_text), start + self.MAX_PREVIEW_CHARS)
        chunk = full_text[start:end]
        if start > 0:
            chunk = f"[... preview starts at char {start:,} ...]\n\n" + chunk
        if end < len(full_text):
            chunk += f"\n\n[... preview truncated at char {end:,} / {len(full_text):,} ...]"
        return chunk, True

    def _set_preview_with_highlights(self, text: str) -> int:
        self.preview_text.setPlainText(text)
        spans = self._find_match_spans(text, self.MAX_HIGHLIGHT_SPANS)
        if not spans:
            self.preview_text.setExtraSelections([])
            return 0

        fmt = QTextCharFormat()
        fmt.setBackground(QColor("#fff59d"))
        fmt.setForeground(QColor("#111111"))

        selections: list[QTextEdit.ExtraSelection] = []
        doc = self.preview_text.document()
        for start, end in spans:
            cursor = QTextCursor(doc)
            cursor.setPosition(start)
            cursor.setPosition(end, QTextCursor.KeepAnchor)
            sel = QTextEdit.ExtraSelection()
            sel.cursor = cursor
            sel.format = fmt
            selections.append(sel)

        self.preview_text.setExtraSelections(selections)
        jump_cursor = QTextCursor(doc)
        jump_cursor.setPosition(spans[0][0])
        self.preview_text.setTextCursor(jump_cursor)
        self.preview_text.centerCursor()
        return len(spans)

    def _open_url(self, url: str) -> None:
        if not url:
            return
        QDesktopServices.openUrl(QUrl(url))

    def _open_preview_url(self) -> None:
        self._open_url(self.preview_url_edit.text().strip())

    def _fetch_detail_by_row_id(self, row_id: int) -> dict[str, str] | None:
        try:
            con = _connect(self.db_path_edit.text().strip())
            try:
                row = con.execute(
                    """
                    SELECT citation, title, category, bundle, url, text
                    FROM docs_fts
                    WHERE rowid = ?
                    LIMIT 1;
                    """,
                    (int(row_id),),
                ).fetchone()
            finally:
                con.close()
        except Exception as exc:
            self.statusBar().showMessage(f"Local preview load failed: {exc}")
            return None

        if row is None:
            return None

        return {
            "citation": row["citation"] or "",
            "title": row["title"] or "",
            "category": row["category"] or "",
            "bundle": row["bundle"] or "",
            "url": row["url"] or "",
            "text": row["text"] or "",
        }

    def _on_result_selected(self) -> None:
        idx = self.results_table.currentRow()
        if idx < 0 or idx >= len(self._result_rows):
            self._clear_preview("Select a result to view full local text.")
            return

        row = self._result_rows[idx]
        row_id_obj = row.get("row_id")
        row_id = -1
        if isinstance(row_id_obj, int):
            row_id = row_id_obj
        elif isinstance(row_id_obj, str):
            try:
                row_id = int(row_id_obj)
            except ValueError:
                row_id = -1
        if row_id < 0:
            self._clear_preview("Unable to load local text for this row.")
            return

        detail = self._detail_cache.get(row_id)
        if detail is None:
            detail = self._fetch_detail_by_row_id(row_id)
            if detail is None:
                self._clear_preview("Could not load local text for this result.")
                return
            self._detail_cache[row_id] = detail

        title = detail["title"].strip()
        citation = detail["citation"].strip()
        category = detail["category"].strip()
        bundle = detail["bundle"].strip()
        header = " | ".join(part for part in [citation, title] if part)
        scope = " / ".join(part for part in [category, bundle] if part)
        if scope:
            header = f"{header}\n{scope}" if header else scope
        self.preview_meta.setText(header or "Local document preview")
        self.preview_url_edit.setText(detail["url"])
        self.open_source_btn.setEnabled(bool(detail["url"].strip()))
        preview_text, was_truncated = self._choose_preview_text(detail["text"])
        hit_count = self._set_preview_with_highlights(preview_text)
        if was_truncated:
            self.statusBar().showMessage(
                f"Large statute preview truncated for speed; highlighted {hit_count} match(es)."
            )

    def _open_result_url(self, row: int, _col: int) -> None:
        if row < 0 or row >= len(self._result_rows):
            return
        self._open_url(str(self._result_rows[row].get("url", "")).strip())


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="out/index.sqlite", help="SQLite DB path (default: out/index.sqlite)")
    args = p.parse_args(argv)

    app = QApplication([])
    w = SearchWindow(args.db)
    w.show()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())
