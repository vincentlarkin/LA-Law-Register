"""
Dev helper: understand Louisiana Laws TOC WebForms postback behavior.

Run:
  python scripts/dev/test_toc_postback.py
"""

from __future__ import annotations

import sys

import requests
from bs4 import BeautifulSoup


def _get_session() -> requests.Session:
    # Some environments set a local proxy that breaks direct HTTPS; don't trust env by default.
    s = requests.Session()
    s.trust_env = False
    s.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            )
        }
    )
    return s


def _parse_hidden_inputs(html: str) -> dict[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    form = soup.find("form")
    if not form:
        raise RuntimeError("No <form> found")
    data: dict[str, str] = {}
    for inp in form.find_all("input"):
        name = inp.get("name")
        if not name:
            continue
        data[name] = inp.get("value", "")
    return data


def main() -> int:
    url = "https://www.legis.la.gov/legis/Laws_Toc.aspx?folder=75&level=Parent"
    event_target = "ctl00$ctl00$PageBody$PageContent$ListViewTOC1$ctrl0$LinkButton1a"
    update_panel = "ctl00$ctl00$PageBody$PageContent$UpdatePanelToc"
    script_manager = "ctl00$ctl00$PageBody$PageContent$ScriptManager1"

    s = _get_session()
    r = s.get(url, timeout=30)
    print("GET", r.status_code, "len", len(r.text))
    data = _parse_hidden_inputs(r.text)

    # 1) Normal full postback
    data_full = dict(data)
    data_full["__EVENTTARGET"] = event_target
    data_full["__EVENTARGUMENT"] = ""
    r_full = s.post(url, data=data_full, timeout=30)
    print("POST full", r_full.status_code, "len", len(r_full.text), "identical", r_full.text == r.text)

    # 2) Async postback (what UpdatePanel does)
    data_async = dict(data)
    data_async["__EVENTTARGET"] = event_target
    data_async["__EVENTARGUMENT"] = ""
    data_async[script_manager] = f"{update_panel}|{event_target}"
    data_async["__ASYNCPOST"] = "true"
    r_async = s.post(
        url,
        data=data_async,
        headers={
            "X-MicrosoftAjax": "Delta=true",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": url,
        },
        timeout=30,
    )
    print("POST async", r_async.status_code, "len", len(r_async.text))
    print("async head:", repr(r_async.text[:200]))

    # Heuristic: async response contains updated HTML fragments; see if TITLE 1 disappears.
    print("async contains TITLE 1:", "TITLE 1" in r_async.text)
    print("async contains CHAPTER:", "CHAPTER" in r_async.text or "Chapter" in r_async.text)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

