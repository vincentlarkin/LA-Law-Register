"""
BayouLex Compendium TUI.

Run this file directly with no command-line arguments. It is the human-facing
front door for downloading, building, packaging, smoke testing, and launching
BayouLex while the older scripts remain lower-level machinery.
"""

from __future__ import annotations

import datetime as _dt
import os
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PYTHON = sys.executable
DEFAULT_DATASET = ROOT / "data" / "bayoulex-content.sqlite"
DEFAULT_OFFLINE_ROOT = ROOT / "data" / "offline"


def _run(label: str, command: list[str]) -> bool:
    print("\n" + "=" * 78)
    print(label)
    print("=" * 78)
    print("Starting. This can take a while for full-content jobs.\n")
    try:
        proc = subprocess.run(command, cwd=ROOT)
    except KeyboardInterrupt:
        print(f"\n[canceled] {label}")
        return False
    if proc.returncode == 0:
        print(f"\n[ok] {label}")
        return True
    print(f"\n[error] {label} failed. Exit code: {proc.returncode}")
    return False


def _pause() -> None:
    input("\nPress Enter to return to the menu...")


def _dataset_version() -> str:
    default = _dt.datetime.now().strftime("%Y%m%d")
    raw = input(f"Dataset version [{default}]: ").strip()
    return raw or default


def _confirm_long_job(label: str) -> bool:
    print(f"\n{label}")
    print("This may download a lot of data and can run for a long time.")
    answer = input("Continue? [y/N]: ").strip().lower()
    return answer in {"y", "yes"}


def _download_laws() -> bool:
    return _run(
        "Download Louisiana codes, constitution, and legislative rules",
        [PYTHON, "scripts/download_louisiana_laws.py", "--categories", "all"],
    )


def _download_cases() -> bool:
    return _run(
        "Download Louisiana Supreme Court decisions",
        [PYTHON, "scripts/download_louisiana_case_law.py"],
    )


def _download_bills() -> bool:
    return _run(
        "Download Louisiana legislative bills and act text",
        [
            PYTHON,
            "scripts/download_louisiana_bills.py",
            "--session",
            "all",
            "--include-document-text",
            "--document-statuses",
            "law",
        ],
    )


def _download_all() -> bool:
    for step in (_download_laws, _download_cases, _download_bills):
        if not step():
            return False
    return True


def _build_legacy_index() -> bool:
    return _run(
        "Build legacy local search index",
        [PYTHON, "scripts/build_search_index.py", "--rebuild"],
    )


def _build_canonical_dataset(version: str | None = None) -> bool:
    version = version or _dataset_version()
    return _run(
        "Build canonical BayouLex SQLite dataset",
        [
            PYTHON,
            "scripts/build_bayoulex_dataset.py",
            "--out",
            "out",
            "--db",
            str(DEFAULT_DATASET.relative_to(ROOT)),
            "--dataset-version",
            version,
        ],
    )


def _package_offline(version: str | None = None) -> bool:
    version = version or _dataset_version()
    return _run(
        "Package offline snapshot chunks",
        [
            PYTHON,
            "scripts/package_offline_snapshot.py",
            "--db",
            str(DEFAULT_DATASET.relative_to(ROOT)),
            "--dataset-version",
            version,
        ],
    )


def _run_full_compendium() -> None:
    if not _confirm_long_job("Get everything"):
        return
    version = _dataset_version()
    if not _download_all():
        return
    if not _build_legacy_index():
        return
    if not _build_canonical_dataset(version):
        return
    _package_offline(version)


def _run_api_smoke() -> None:
    dotnet = _dotnet_path()
    dataset = DEFAULT_DATASET
    if not dataset.exists():
        print("\nNo canonical dataset found. Choose 'Build canonical API dataset' first.")
        return

    env = os.environ.copy()
    env["BAYOULEX_DATASET_PATH"] = str(dataset)
    env["BAYOULEX_OFFLINE_ROOT"] = str(DEFAULT_OFFLINE_ROOT)
    env["BAYOULEX_PUBLIC_BASE_URL"] = "http://127.0.0.1:5087/bayoulex/v1"
    print("\nStarting local BayouLex API at http://127.0.0.1:5087")
    print("Leave this window open while testing the Windows app.")
    print("In the app, set API to: http://127.0.0.1:5087/bayoulex/v1/")
    print("Stop the server with Ctrl+C.\n")
    subprocess.run(
        [
            dotnet,
            "run",
            "--project",
            "src/BayouLex.Api/BayouLex.Api.csproj",
            "--urls",
            "http://127.0.0.1:5087",
        ],
        cwd=ROOT,
        env=env,
    )


def _run_windows_client() -> None:
    dotnet = _dotnet_path()
    try:
        subprocess.run(
            [
                dotnet,
                "run",
                "--project",
                "src/BayouLex.Client.Windows/BayouLex.Client.Windows.csproj",
            ],
            cwd=ROOT,
        )
    except KeyboardInterrupt:
        print("\n[canceled] Windows client run")


def _run_legacy_search_gui() -> None:
    _run("Launch legacy Python search GUI", [PYTHON, "scripts/search_laws_gui.py"])


def _run_legacy_browser_gui() -> None:
    _run("Launch legacy Python browser GUI", [PYTHON, "scripts/law_browser_gui.py"])


def _dotnet_path() -> str:
    local = Path.home() / ".dotnet-sdk-10" / "dotnet.exe"
    if local.exists():
        return str(local)
    return "dotnet"


def _print_status() -> None:
    out_index = ROOT / "out" / "index.sqlite"
    dataset = DEFAULT_DATASET
    offline = DEFAULT_OFFLINE_ROOT
    print("\nCurrent BayouLex data status")
    print("-" * 32)
    print(f"Legacy index:      {'present' if out_index.exists() else 'missing'}  {out_index}")
    print(f"Canonical dataset: {'present' if dataset.exists() else 'missing'}  {dataset}")
    print(f"Offline chunks:    {'present' if offline.exists() else 'missing'}  {offline}")


def _print_api_explanation() -> None:
    print(
        r"""
How the API works
-----------------
The API does not scrape Louisiana websites when someone searches. BayouLex has a
refresh/build step and a serving step.

Refresh/build step:
  1. Download source content into out/.
  2. Build one SQLite database at data/bayoulex-content.sqlite.
  3. Optionally package that database into offline chunks under data/offline/.

Serving step:
  The ASP.NET API runs continuously and opens that SQLite database read-only.
  Search/detail requests are answered from SQLite. Refreshing content is a
  separate action that you run manually from this menu, or later on a schedule.

Local smoke test:
  1. Choose "Run local API smoke server" and leave that window open.
  2. Open another terminal.
  3. Run this same menu and choose "Run C# Windows client".
  4. In the app, use: http://127.0.0.1:5087/bayoulex/v1/
  5. Click Refresh, then search for something like "capital".

The Windows app starts in API mode. Its "Download Data for Offline" button uses
the API's offline manifest/chunks when those package files exist.
"""
    )


def _download_menu() -> None:
    while True:
        print("\nDownload Source Content")
        print("=======================")
        print("1. Download all supported source content")
        print("2. Download codes, constitution, and rules")
        print("3. Download Louisiana Supreme Court decisions")
        print("4. Download legislative bills and act text")
        print("5. Back")
        choice = input("\nChoose: ").strip()

        if choice == "1":
            if _confirm_long_job("Download all supported source content"):
                _download_all()
            _pause()
        elif choice == "2":
            if _confirm_long_job("Download codes, constitution, and rules"):
                _download_laws()
            _pause()
        elif choice == "3":
            if _confirm_long_job("Download Louisiana Supreme Court decisions"):
                _download_cases()
            _pause()
        elif choice == "4":
            if _confirm_long_job("Download legislative bills and act text"):
                _download_bills()
            _pause()
        elif choice == "5":
            return
        else:
            print("Choose a number from 1 to 5.")


def main() -> int:
    while True:
        print("\nBayouLex Compendium")
        print("===================")
        print("1. Get everything: download all, build indexes, package offline data")
        print("2. Download source content")
        print("3. Build legacy Python index")
        print("4. Build canonical API dataset")
        print("5. Package offline dataset chunks")
        print("6. Run local API smoke server")
        print("7. Run C# Windows client")
        print("8. Launch legacy Python search GUI")
        print("9. Launch legacy Python browser GUI")
        print("10. Explain API and smoke test")
        print("11. Show data status")
        print("12. Exit")
        choice = input("\nChoose: ").strip()

        try:
            if choice == "1":
                _run_full_compendium()
                _pause()
            elif choice == "2":
                _download_menu()
            elif choice == "3":
                _build_legacy_index()
                _pause()
            elif choice == "4":
                _build_canonical_dataset()
                _pause()
            elif choice == "5":
                _package_offline()
                _pause()
            elif choice == "6":
                _run_api_smoke()
                _pause()
            elif choice == "7":
                _run_windows_client()
                _pause()
            elif choice == "8":
                _run_legacy_search_gui()
                _pause()
            elif choice == "9":
                _run_legacy_browser_gui()
                _pause()
            elif choice == "10":
                _print_api_explanation()
                _pause()
            elif choice == "11":
                _print_status()
                _pause()
            elif choice == "12":
                return 0
            else:
                print("Choose a number from 1 to 12.")
        except KeyboardInterrupt:
            print("\nCanceled. Back at the main menu.")


if __name__ == "__main__":
    raise SystemExit(main())
