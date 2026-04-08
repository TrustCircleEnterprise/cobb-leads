"""
Cobb County, GA — Motivated Seller Lead Scraper
Uses the Landmark portal at superiorcourtclerk.cobbcounty.gov/landmark
"""

import asyncio
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
    from playwright.async_api import async_playwright
except ImportError:
    print("playwright not installed")
    sys.exit(1)

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

LANDMARK_URL = "https://superiorcourtclerk.cobbcounty.gov/landmark"
SEARCH_URL   = f"{LANDMARK_URL}/web/search/DOCSEARCH303S1"
PARCEL_BASE  = "https://gis.cobbcountyga.gov"
LOOK_BACK_DAYS = 7
MAX_RETRIES    = 3
RETRY_DELAY    = 3

REPO_ROOT     = Path(__file__).parent.parent
DASHBOARD_DIR = REPO_ROOT / "dashboard"
DATA_DIR      = REPO_ROOT / "data"
for d in (DASHBOARD_DIR, DATA_DIR):
    d.mkdir(parents=True, exist_ok=True)

DOC_TYPES = {
    "LP":       ("Lis Pendens",           "lis_pendens"),
    "NOFC":     ("Notice of Foreclosure", "foreclosure"),
    "TAXDEED":  ("Tax Deed",              "tax_deed"),
    "JUD":      ("Judgment",              "judgment"),
    "CCJ":      ("Certified Judgment",    "judgment"),
    "DRJUD":    ("Domestic Judgment",     "judgment"),
    "LNCORPTX": ("Corp Tax Lien",         "tax_lien"),
    "LNIRS":    ("IRS Lien",              "tax_lien"),
    "LNFED":    ("Federal Lien",          "tax_lien"),
    "LN":       ("Lien",                  "lien"),
    "LNMECH":   ("Mechanic's Lien",       "lien"),
    "LNHOA":    ("HOA Lien",              "lien"),
    "MEDLN":    ("Medicaid Lien",         "lien"),
    "PRO":      ("Probate",               "probate"),
    "NOC":      ("Notice of Commencement","noc"),
    "RELLP":    ("Release Lis Pendens",   "release"),
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


# ── Parcel helpers (unchanged) ───────────────────────────────────────────────

def build_parcel_index(dbf_path: Path) -> dict:
    if DBF is None:
        return {}
    idx: dict[str, dict] = {}
    try:
        table = DBF(str(dbf_path), encoding="latin-1", ignore_missing_memofile=True)
        for row in table:
            try:
                owner_raw = _col(row, "OWNER", "OWN1", "OWNERNAME")
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
                if not owner_raw:
                    continue
                for v in _name_variants(owner_raw):
                    if v:
                        idx[v] = rec
            except Exception:
                pass
    except Exception as e:
        log.warning(f"DBF read error: {e}")
    log.info(f"Parcel index built: {len(idx):,} name variants")
    return idx


def _name_variants(owner_raw: str) -> list[str]:
    parts = re.split(r",\s*", owner_raw.strip(), maxsplit=1)
    if len(parts) == 2:
        last, first = parts
        return list({_norm(owner_raw), _norm(f"{first} {last}"), _norm(f"{last} {first}")})
    return [_norm(owner_raw)]


def lookup_parcel(owner: str) -> Optional[dict]:
    return _parcel_index.get(_norm(owner))


def download_parcel_dbf() -> Optional[Path]:
    cache_dir = REPO_ROOT / ".cache"
    cache_dir.mkdir(exist_ok=True)
    dbf_path = cache_dir / "parcels.dbf"
    if dbf_path.exists() and (time.time() - dbf_path.stat().st_mtime) < 86400:
        log.info("Using cached parcel DBF")
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
                        log.info(f"Parcel DBF extracted from {url}")
                        return dbf_path
        except Exception as e:
            log.warning(f"Parcel download failed {url}: {e}")

    log.warning("Parcel data unavailable — skipping address enrichment")
    return None


# ── Landmark scraper ─────────────────────────────────────────────────────────

async def scrape_doc_type(page, doc_type: str, date_from: str, date_to: str) -> list[dict]:
    """Search one doc type using the Landmark Document Type Search."""
    label, cat = DOC_TYPES.get(doc_type, (doc_type, "other"))
    records = []

    try:
        # Navigate to Document Type Search
        await page.goto(f"{LANDMARK_URL}/web/search/DOCSEARCH303S1", timeout=30000, wait_until="domcontentloaded")
        await asyncio.sleep(2)

        # Fill document type text box
        dt_box = page.locator("textarea, input[type='text']").first
        await dt_box.fill(doc_type)
        await asyncio.sleep(0.5)

        # Fill begin date
        begin = page.locator("input[id*='beginDate'], input[name*='beginDate'], input[id*='begin'], input[name*='begin']").first
        if await begin.count() == 0:
            begin = page.locator("input[type='text']").nth(1)
        await begin.triple_click()
        await begin.fill(date_from)

        # Fill end date
        end = page.locator("input[id*='endDate'], input[name*='endDate'], input[id*='end'], input[name*='end']").first
        if await end.count() == 0:
            end = page.locator("input[type='text']").nth(2)
        await end.triple_click()
        await end.fill(date_to)

        # Click Submit
        await page.locator("button:has-text('Submit'), input[value='Submit']").first.click()
        await asyncio.sleep(4)

        # Parse results across pages
        while True:
            html = await page.content()
            new_recs = _parse_landmark_results(html, doc_type, label, cat)
            records.extend(new_recs)
            log.info(f"[{doc_type}] Page batch: {len(new_recs)} records")

            # Check for next page
            next_btn = page.locator("a:has-text('Next'), button:has-text('Next')").first
            if await next_btn.count() == 0:
                break
            await next_btn.click()
            await asyncio.sleep(3)

    except Exception as e:
        log.warning(f"[{doc_type}] Error: {e}")

    log.info(f"[{doc_type}] Total: {len(records)} records")
    return records


def _parse_landmark_results(html: str, doc_type: str, label: str, cat: str) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    records = []

    table = soup.find("table")
    if not table:
        return records

    rows = table.find_all("tr")
    if len(rows) < 2:
        return records

    headers = [th.get_text(strip=True).lower() for th in rows[0].find_all(["th", "td"])]
    log.info(f"  Headers: {headers}")

    def ci(*names):
        for n in names:
            for i, h in enumerate(headers):
                if n in h:
                    return i
        return None

    idx_docnum  = ci("clerk", "file", "doc", "number", "instrument")
    idx_filed   = ci("recorded", "filed", "date")
    idx_grantor = ci("grantor", "owner", "from", "name")
    idx_grantee = ci("grantee", "to", "buyer")
    idx_legal   = ci("legal", "description")
    idx_amount  = ci("amount", "consideration")
    idx_type    = ci("type", "document type")

    for row in rows[1:]:
        try:
            cells = row.find_all(["td", "th"])
            if not cells:
                continue

            def cell(idx):
                if idx is None or idx >= len(cells):
                    return ""
                return cells[idx].get_text(strip=True)

            link_tag = row.find("a", href=True)
            href = ""
            if link_tag:
                href = link_tag["href"]
                if not href.startswith("http"):
                    href = f"{LANDMARK_URL}{href}"

            doc_num = cell(idx_docnum) or (link_tag.get_text(strip=True) if link_tag else "")
            grantor = cell(idx_grantor)

            if not doc_num and not grantor:
                continue

            records.append({
                "doc_num":      doc_num,
                "doc_type":     cell(idx_type) or doc_type,
                "filed":        _norm_date(cell(idx_filed)),
                "cat":          cat,
                "cat_label":    label,
                "owner":        grantor,
                "grantee":      cell(idx_grantee),
                "amount":       _parse_amount(cell(idx_amount)),
                "legal":        cell(idx_legal),
                "clerk_url":    href,
                "prop_address": "", "prop_city": "", "prop_state": "GA", "prop_zip": "",
                "mail_address": "", "mail_city": "", "mail_state": "", "mail_zip": "",
                "flags": [], "score": 0,
            })
        except Exception:
            pass

    return records


def _parse_amount(raw: str) -> float:
    try:
        return float(re.sub(r"[^\d.]", "", raw.replace(",", "")) or 0)
    except Exception:
        return 0.0


def _norm_date(raw: str) -> str:
    for fmt in ("%m/%d/%Y", "%m-%d-%Y", "%Y-%m-%d", "%m/%d/%y", "%B %d, %Y"):
        try:
            return datetime.strptime(raw.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return raw.strip()


# ── Scoring ──────────────────────────────────────────────────────────────────

WEEK_AGO = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")


def compute_flags_and_score(rec: dict, all_records: list[dict]) -> tuple[list[str], int]:
    flags: list[str] = []
    score = 30
    dt    = rec.get("doc_type", "")
    cat   = rec.get("cat", "")
    owner = rec.get("owner", "")
    filed = rec.get("filed", "")

    if dt in ("LP", "RELLP"):        flags.append("Lis pendens")
    if dt == "NOFC":                  flags.append("Pre-foreclosure")
    if cat == "judgment":             flags.append("Judgment lien")
    if cat == "tax_lien":             flags.append("Tax lien")
    if dt == "LNMECH":                flags.append("Mechanic lien")
    if cat == "probate":              flags.append("Probate / estate")
    if owner and re.search(r"\b(LLC|INC|CORP|LTD|TRUST|ESTATE)\b", owner.upper()):
        flags.append("LLC / corp owner")
    if filed >= WEEK_AGO:             flags.append("New this week")

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


# ── Output ───────────────────────────────────────────────────────────────────

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


# ── Main ─────────────────────────────────────────────────────────────────────

async def main():
    global _parcel_index

    today     = datetime.now()
    date_to   = today.strftime("%m/%d/%Y")
    date_from = (today - timedelta(days=LOOK_BACK_DAYS)).strftime("%m/%d/%Y")
    log.info(f"Scraping {date_from} → {date_to}")

    dbf_path = download_parcel_dbf()
    if dbf_path and dbf_path.exists():
        _parcel_index = build_parcel_index(dbf_path)

    all_records: list[dict] = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True, args=["--no-sandbox","--disable-dev-shm-usage"])
        ctx = await browser.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
            ignore_https_errors=True,
        )
        page = await ctx.new_page()

        for doc_type in DOC_TYPES.keys():
            for attempt in range(MAX_RETRIES):
                try:
                    recs = await scrape_doc_type(page, doc_type, date_from, date_to)
                    all_records.extend(recs)
                    break
                except Exception as e:
                    log.warning(f"[{doc_type}] attempt {attempt+1} failed: {e}")
                    await asyncio.sleep(RETRY_DELAY)

        await browser.close()

    log.info(f"Total records: {len(all_records)}")

    for rec in all_records:
        try:
            enrich_record(rec)
            flags, score = compute_flags_and_score(rec, all_records)
            rec["flags"] = flags
            rec["score"] = score
        except Exception as e:
            log.warning(f"Scoring error: {e}")
            rec["flags"] = []
            rec["score"] = 30

    all_records.sort(key=lambda r: r.get("score", 0), reverse=True)
    write_outputs(all_records, date_from, date_to)
    log.info("✅ Done")


if __name__ == "__main__":
    asyncio.run(main())
