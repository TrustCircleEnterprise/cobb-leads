"""
Cobb County, GA — Motivated Seller Lead Scraper
Uses the LandmarkWeb Search/GetSearchResultsExport endpoint.
"""

import csv
import io
import json
import logging
import re
import sys
import time
import zipfile
import urllib3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import requests
from bs4 import BeautifulSoup

try:
    from dbfread import DBF
except ImportError:
    DBF = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("cobb_scraper")

LANDMARK_BASE  = "https://superiorcourtclerk.cobbcounty.gov/LandmarkWeb"
LOOK_BACK_DAYS = 7
MAX_RETRIES    = 3
RETRY_DELAY    = 3

REPO_ROOT     = Path(__file__).parent.parent
DASHBOARD_DIR = REPO_ROOT / "dashboard"
DATA_DIR      = REPO_ROOT / "data"
for d in (DASHBOARD_DIR, DATA_DIR):
    d.mkdir(parents=True, exist_ok=True)

TARGET_TYPES = {
    "LP", "NOFC", "TAXDEED", "JUD", "CCJ", "DRJUD",
    "LNCORPTX", "LNIRS", "LNFED", "LN", "LNMECH",
    "LNHOA", "MEDLN", "PRO", "NOC", "RELLP",
}

DOC_TYPE_MAP = {
    "LP":       ("Lis Pendens",            "lis_pendens"),
    "NOFC":     ("Notice of Foreclosure",  "foreclosure"),
    "TAXDEED":  ("Tax Deed",               "tax_deed"),
    "JUD":      ("Judgment",               "judgment"),
    "CCJ":      ("Certified Judgment",     "judgment"),
    "DRJUD":    ("Domestic Judgment",      "judgment"),
    "LNCORPTX": ("Corp Tax Lien",          "tax_lien"),
    "LNIRS":    ("IRS Lien",               "tax_lien"),
    "LNFED":    ("Federal Lien",           "tax_lien"),
    "LN":       ("Lien",                   "lien"),
    "LNMECH":   ("Mechanic's Lien",        "lien"),
    "LNHOA":    ("HOA Lien",               "lien"),
    "MEDLN":    ("Medicaid Lien",          "lien"),
    "PRO":      ("Probate",                "probate"),
    "NOC":      ("Notice of Commencement", "noc"),
    "RELLP":    ("Release Lis Pendens",    "release"),
}

_parcel_index: dict[str, dict] = {}


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", str(s or "")).strip().upper()

def _col(row: dict, *names: str) -> str:
    for n in names:
        v = row.get(n, "")
        if v:
            return str(v).strip()
    return ""

def _name_variants(owner_raw: str) -> list[str]:
    parts = re.split(r",\s*", owner_raw.strip(), maxsplit=1)
    if len(parts) == 2:
        last, first = parts
        return list({_norm(owner_raw), _norm(f"{first} {last}"), _norm(f"{last} {first}")})
    return [_norm(owner_raw)]

def lookup_parcel(owner: str) -> Optional[dict]:
    return _parcel_index.get(_norm(owner))

def build_parcel_index(dbf_path: Path) -> dict:
    if DBF is None:
        return {}
    idx: dict[str, dict] = {}
    try:
        table = DBF(str(dbf_path), encoding="latin-1", ignore_missing_memofile=True)
        for row in table:
            try:
                owner_raw = _col(row, "OWNER", "OWN1", "OWNERNAME")
                if not owner_raw:
                    continue
                rec = {
                    "prop_address": _col(row, "SITE_ADDR", "SITEADDR"),
                    "prop_city":    _col(row, "SITE_CITY", "SITECITY"),
                    "prop_state":   "GA",
                    "prop_zip":     _col(row, "SITE_ZIP", "SITEZIP"),
                    "mail_address": _col(row, "ADDR_1", "MAILADR1"),
                    "mail_city":    _col(row, "CITY", "MAILCITY"),
                    "mail_state":   _col(row, "STATE", "MAILSTATE"),
                    "mail_zip":     _col(row, "ZIP", "MAILZIP"),
                }
                for v in _name_variants(owner_raw):
                    if v:
                        idx[v] = rec
            except Exception:
                pass
    except Exception as e:
        log.warning(f"DBF read error: {e}")
    log.info(f"Parcel index: {len(idx):,} variants")
    return idx

def download_parcel_dbf() -> Optional[Path]:
    cache_dir = REPO_ROOT / ".cache"
    cache_dir.mkdir(exist_ok=True)
    dbf_path = cache_dir / "parcels.dbf"
    if dbf_path.exists() and (time.time() - dbf_path.stat().st_mtime) < 86400:
        return dbf_path
    urls = [
        "https://gis.cobbcountyga.gov/download/parcels.zip",
        "https://gis.cobbcountyga.gov/download/Cobb_Parcels.zip",
        "https://www.cobbcountyga.gov/images/gis/data/parcels.zip",
    ]
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})
    for url in urls:
        try:
            r = session.get(url, timeout=60, verify=False, stream=True)
            if r.status_code == 200:
                zdata = b"".join(r.iter_content(65536))
                with zipfile.ZipFile(io.BytesIO(zdata)) as zf:
                    dbf_files = [n for n in zf.namelist() if n.lower().endswith(".dbf")]
                    if dbf_files:
                        dbf_path.write_bytes(zf.read(dbf_files[0]))
                        log.info(f"Parcel DBF from {url}")
                        return dbf_path
        except Exception as e:
            log.warning(f"Parcel download failed {url}: {e}")
    log.warning("Parcel data unavailable")
    return None


def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
    })
    url = f"{LANDMARK_BASE}/search/index?theme=.blue&section=searchCriteriaRecordDate&quickSearchSelection="
    try:
        r = s.get(url, timeout=30, verify=False)
        log.info(f"Session: {r.status_code}, cookies: {list(s.cookies.keys())}")
    except Exception as e:
        log.warning(f"Session error: {e}")
    return s


def get_token(s: requests.Session) -> str:
    try:
        url = f"{LANDMARK_BASE}/search/index?theme=.blue&section=searchCriteriaRecordDate&quickSearchSelection="
        r = s.get(url, timeout=30, verify=False)
        soup = BeautifulSoup(r.text, "lxml")
        t = soup.find("input", {"name": "__RequestVerificationToken"})
        if t:
            v = t.get("value", "")
            log.info(f"Token found: {v[:20]}...")
            return v
        # Check all inputs
        inputs = soup.find_all("input", type="hidden")
        log.info(f"Hidden inputs: {[(i.get('name',''), i.get('value','')[:15]) for i in inputs[:5]]}")
    except Exception as e:
        log.warning(f"Token error: {e}")
    return ""


def search_landmark(s: requests.Session, date_from: str, date_to: str) -> list[dict]:
    """
    Two-step: RecordDateSearch to set session, then GetSearchResultsExport for CSV data.
    """
    records = []
    token = get_token(s)

    referer = f"{LANDMARK_BASE}/search/index?theme=.blue&section=searchCriteriaRecordDate&quickSearchSelection="
    ajax_headers = {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": referer,
        "Origin": "https://superiorcourtclerk.cobbcounty.gov",
        "Accept": "*/*",
        "Connection": "keep-alive",
    }

    # Step 1: RecordDateSearch
    search_data = {
        "beginDate": date_from,
        "endDate": date_to,
        "exclude": "false",
        "ReturnIndexGroups": "false",
        "recordCount": "500",
        "townName": "",
        "mobileHomesOnly": "false",
    }
    if token:
        search_data["__RequestVerificationToken"] = token

    for attempt in range(MAX_RETRIES):
        try:
            log.info(f"RecordDateSearch attempt {attempt+1}")
            r = s.post(
                f"{LANDMARK_BASE}/Search/RecordDateSearch",
                data=search_data,
                headers=ajax_headers,
                timeout=60,
                verify=False,
            )
            log.info(f"RecordDateSearch: {r.status_code}, len={len(r.text)}")
            if r.status_code == 200:
                break
            time.sleep(RETRY_DELAY)
        except Exception as e:
            log.warning(f"RecordDateSearch error: {e}")
            time.sleep(RETRY_DELAY)

    time.sleep(2)

    # Step 2: Try GetSearchResultsExport (CSV export)
    export_headers = {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Referer": referer,
        "Origin": "https://superiorcourtclerk.cobbcounty.gov",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Connection": "keep-alive",
    }

    # Build column headers for export
    col_names = ["","","Status","","Grantor","Grantee","Recording Date","Doc Type","Book Type","Book","Page","Clerk File No","","DocLinks"]
    export_data = {
        "cols": json.dumps([{"i": i} for i in range(len(col_names))]),
        "headers": json.dumps(col_names),
        "time": datetime.now().isoformat(),
    }

    try:
        log.info("Trying GetSearchResultsExport")
        r = s.post(
            f"{LANDMARK_BASE}/Search/GetSearchResultsExport",
            data=export_data,
            headers=export_headers,
            timeout=60,
            verify=False,
        )
        log.info(f"Export: {r.status_code}, len={len(r.content)}, type={r.headers.get('Content-Type','')}")
        log.info(f"Export preview: {r.text[:500]}")

        if r.status_code == 200 and r.content:
            records = parse_export(r.text, date_from, date_to)
            log.info(f"Export records: {len(records)}")
            if records:
                return records
    except Exception as e:
        log.warning(f"Export error: {e}")

    # Step 3: Fallback — try GetSearchResults with JSON
    log.info("Falling back to GetSearchResults JSON")
    try:
        params = {"draw": "1", "start": "0", "length": "500",
                  "search[value]": "", "search[regex]": "false",
                  "order[0][column]": "7", "order[0][dir]": "asc"}
        for i in range(26):
            params[f"columns[{i}][data]"] = str(i)
            params[f"columns[{i}][name]"] = ""
            params[f"columns[{i}][searchable]"] = "true"
            params[f"columns[{i}][orderable]"] = "false"
            params[f"columns[{i}][search][value]"] = ""
            params[f"columns[{i}][search][regex]"] = "false"

        r = s.post(
            f"{LANDMARK_BASE}/Search/GetSearchResults",
            data=params,
            headers={**ajax_headers, "Accept": "application/json, text/javascript, */*; q=0.01"},
            timeout=60,
            verify=False,
        )
        log.info(f"GetSearchResults: {r.status_code}, len={len(r.content)}")
        log.info(f"Preview: {r.text[:500]}")

        if r.status_code == 200 and r.content:
            data = r.json()
            rows = data.get("data", [])
            log.info(f"JSON rows: {len(rows)}, total: {data.get('recordsTotal', 0)}")
            for row in rows:
                rec = parse_row(row)
                if rec:
                    records.append(rec)
    except Exception as e:
        log.warning(f"GetSearchResults fallback error: {e}")

    return records


def parse_export(text: str, date_from: str, date_to: str) -> list[dict]:
    """Parse the HTML export table."""
    soup = BeautifulSoup(text, "lxml")
    records = []

    table = None
    for t in soup.find_all("table"):
        if len(t.find_all("tr")) >= 2:
            table = t
            break
    if not table:
        log.info("No table in export response")
        return records

    rows = table.find_all("tr")
    headers = [th.get_text(strip=True).lower() for th in rows[0].find_all(["th","td"])]
    log.info(f"Export headers: {headers}")

    def ci(*names):
        for n in names:
            for i, h in enumerate(headers):
                if n in h:
                    return i
        return None

    idx_type    = ci("type", "doc type", "document")
    idx_grantor = ci("grantor", "owner", "direct")
    idx_grantee = ci("grantee", "reverse")
    idx_filed   = ci("recording", "filed", "date")
    idx_docnum  = ci("clerk", "file", "number", "cfn")

    for row in rows[1:]:
        cells = row.find_all(["td","th"])
        if not cells:
            continue
        def cell(i):
            if i is None or i >= len(cells):
                return ""
            return cells[i].get_text(strip=True)

        raw_type = cell(idx_type).upper()
        matched_type = None
        for t in TARGET_TYPES:
            if t == raw_type or raw_type.startswith(t):
                matched_type = t
                break
        if not matched_type:
            continue

        label, cat = DOC_TYPE_MAP.get(matched_type, (raw_type, "other"))
        link_tag = row.find("a", href=True)
        href = ""
        if link_tag:
            href = link_tag["href"]
            if not href.startswith("http"):
                href = f"{LANDMARK_BASE}{href}"

        records.append({
            "doc_num":      cell(idx_docnum),
            "doc_type":     matched_type,
            "filed":        _norm_date(cell(idx_filed)),
            "cat":          cat,
            "cat_label":    label,
            "owner":        cell(idx_grantor),
            "grantee":      cell(idx_grantee),
            "amount":       0.0,
            "legal":        "",
            "clerk_url":    href,
            "prop_address": "", "prop_city": "", "prop_state": "GA", "prop_zip": "",
            "mail_address": "", "mail_city": "", "mail_state": "", "mail_zip": "",
            "flags": [], "score": 0,
        })
    return records


def parse_row(row: dict) -> Optional[dict]:
    grantor  = clean(row.get("5", ""))
    filed    = clean(row.get("7", ""))
    raw_type = clean(row.get("8", "")).upper().strip()
    doc_num  = clean(row.get("12", ""))
    row_id   = str(row.get("DT_RowId", ""))

    clerk_url = ""
    if row_id:
        doc_id = row_id.replace("doc_", "").split("_")[0]
        clerk_url = f"{LANDMARK_BASE}/document/index?theme=.blue&id={doc_id}"

    matched_type = None
    for t in TARGET_TYPES:
        if t == raw_type or raw_type.startswith(t):
            matched_type = t
            break
    if not matched_type:
        return None

    label, cat = DOC_TYPE_MAP.get(matched_type, (raw_type, "other"))
    return {
        "doc_num":      doc_num.replace("nobreak_", ""),
        "doc_type":     matched_type,
        "filed":        _norm_date(filed.replace("nobreak_", "")),
        "cat":          cat,
        "cat_label":    label,
        "owner":        grantor,
        "grantee":      clean(row.get("6", "")),
        "amount":       0.0,
        "legal":        clean(row.get("15", "")),
        "clerk_url":    clerk_url,
        "prop_address": "", "prop_city": "", "prop_state": "GA", "prop_zip": "",
        "mail_address": "", "mail_city": "", "mail_state": "", "mail_zip": "",
        "flags": [], "score": 0,
    }


def clean(s) -> str:
    if s is None:
        return ""
    text = re.sub(r"<[^>]+>", " ", str(s))
    text = re.sub(r"\\u[0-9a-fA-F]{4}", lambda m: chr(int(m.group()[2:], 16)), text)
    return re.sub(r"\s+", " ", text).strip()


def _norm_date(raw: str) -> str:
    raw = re.sub(r"<[^>]+>", "", raw).strip()
    for fmt in ("%m/%d/%Y", "%m-%d-%Y", "%Y-%m-%d", "%m/%d/%y", "%B %d, %Y"):
        try:
            return datetime.strptime(raw.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return raw.strip()


WEEK_AGO = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")


def compute_flags_and_score(rec: dict, all_records: list[dict]) -> tuple[list[str], int]:
    flags: list[str] = []
    score = 30
    dt    = rec.get("doc_type", "")
    cat   = rec.get("cat", "")
    owner = rec.get("owner", "")
    filed = rec.get("filed", "")

    if dt in ("LP", "RELLP"):   flags.append("Lis pendens")
    if dt == "NOFC":             flags.append("Pre-foreclosure")
    if cat == "judgment":        flags.append("Judgment lien")
    if cat == "tax_lien":        flags.append("Tax lien")
    if dt == "LNMECH":           flags.append("Mechanic lien")
    if cat == "probate":         flags.append("Probate / estate")
    if owner and re.search(r"\b(LLC|INC|CORP|LTD|TRUST|ESTATE)\b", owner.upper()):
        flags.append("LLC / corp owner")
    if filed >= WEEK_AGO:        flags.append("New this week")

    score += len(flags) * 10
    owner_key  = _norm(owner)
    owner_docs = {r["doc_type"] for r in all_records if _norm(r.get("owner","")) == owner_key}
    if "LP" in owner_docs and "NOFC" in owner_docs:
        score += 20
    amt = rec.get("amount", 0) or 0
    if amt > 100_000:   score += 15
    elif amt > 50_000:  score += 10
    if filed >= WEEK_AGO: score += 5
    if rec.get("prop_address") or rec.get("mail_address"): score += 5
    return flags, min(score, 100)


def enrich_record(rec: dict) -> dict:
    parcel = lookup_parcel(rec.get("owner", ""))
    if parcel:
        for k, v in parcel.items():
            if v:
                rec[k] = v
    return rec


def _split_name(full: str) -> tuple[str, str]:
    full = full.strip()
    if "," in full:
        p = full.split(",", 1)
        return p[1].strip().title(), p[0].strip().title()
    p = full.split()
    return (p[0].title(), " ".join(p[1:]).title()) if len(p) >= 2 else (full.title(), "")


def write_outputs(records: list[dict], date_from: str, date_to: str):
    with_address = sum(1 for r in records if r.get("prop_address") or r.get("mail_address"))
    payload = {
        "fetched_at":   datetime.utcnow().isoformat() + "Z",
        "source":       "Cobb County Superior Court Clerk",
        "date_range":   {"from": date_from, "to": date_to},
        "total":        len(records),
        "with_address": with_address,
        "records":      records,
    }
    for path in [DASHBOARD_DIR / "records.json", DATA_DIR / "records.json"]:
        path.write_text(json.dumps(payload, indent=2, default=str))
        log.info(f"Wrote {len(records)} records → {path}")

    ghl_path = DATA_DIR / "ghl_export.csv"
    fieldnames = [
        "First Name","Last Name","Mailing Address","Mailing City","Mailing State","Mailing Zip",
        "Property Address","Property City","Property State","Property Zip",
        "Lead Type","Document Type","Date Filed","Document Number","Amount/Debt Owed",
        "Seller Score","Motivated Seller Flags","Source","Public Records URL",
    ]
    with ghl_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in records:
            first, last = _split_name(r.get("owner",""))
            writer.writerow({
                "First Name": first, "Last Name": last,
                "Mailing Address": r.get("mail_address",""),
                "Mailing City":    r.get("mail_city",""),
                "Mailing State":   r.get("mail_state",""),
                "Mailing Zip":     r.get("mail_zip",""),
                "Property Address":r.get("prop_address",""),
                "Property City":   r.get("prop_city",""),
                "Property State":  r.get("prop_state","GA"),
                "Property Zip":    r.get("prop_zip",""),
                "Lead Type":       r.get("cat_label",""),
                "Document Type":   r.get("doc_type",""),
                "Date Filed":      r.get("filed",""),
                "Document Number": r.get("doc_num",""),
                "Amount/Debt Owed":r.get("amount",""),
                "Seller Score":    r.get("score",0),
                "Motivated Seller Flags": "; ".join(r.get("flags",[])),
                "Source":          "Cobb County Superior Court Clerk",
                "Public Records URL": r.get("clerk_url",""),
            })
    log.info(f"GHL CSV → {ghl_path}")


def main():
    global _parcel_index

    today     = datetime.now()
    date_to   = today.strftime("%m/%d/%Y")
    date_from = (today - timedelta(days=LOOK_BACK_DAYS)).strftime("%m/%d/%Y")
    log.info(f"Scraping {date_from} → {date_to}")

    dbf_path = download_parcel_dbf()
    if dbf_path and dbf_path.exists():
        _parcel_index = build_parcel_index(dbf_path)

    session = make_session()
    records = search_landmark(session, date_from, date_to)
    log.info(f"Total matching records: {len(records)}")

    for rec in records:
        try:
            enrich_record(rec)
            flags, score = compute_flags_and_score(rec, records)
            rec["flags"] = flags
            rec["score"] = score
        except Exception as e:
            log.warning(f"Scoring error: {e}")
            rec["flags"] = []
            rec["score"] = 30

    records.sort(key=lambda r: r.get("score", 0), reverse=True)
    write_outputs(records, date_from, date_to)
    log.info("✅ Done")


if __name__ == "__main__":
    main()
