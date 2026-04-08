"""
Cobb County, GA — Motivated Seller Lead Scraper
Fetches clerk filings for the last 7 days, enriches with parcel data,
scores leads, and writes JSON output for the dashboard.
"""

import asyncio
import csv
import io
import json
import logging
import os
import re
import sys
import time
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
import traceback

import requests
from bs4 import BeautifulSoup

# ── Playwright (async) ──────────────────────────────────────────────────────
try:
    from playwright.async_api import async_playwright, TimeoutError as PWTimeoutError
except ImportError:
    print("playwright not installed – run: pip install playwright && python -m playwright install chromium")
    sys.exit(1)

# ── DBF reader ──────────────────────────────────────────────────────────────
try:
    from dbfread import DBF
except ImportError:
    DBF = None
    print("dbfread not installed – parcel lookups will be skipped")

# ── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("cobb_scraper")

# ── Constants ───────────────────────────────────────────────────────────────
CLERK_BASE = "https://superiorcourtclerk.cobbcounty.gov/records-search"
PARCEL_BASE = "https://gis.cobbcountyga.gov"
LOOK_BACK_DAYS = 7
MAX_RETRIES = 3
RETRY_DELAY = 3  # seconds

REPO_ROOT = Path(__file__).parent.parent
DASHBOARD_DIR = REPO_ROOT / "dashboard"
DATA_DIR = REPO_ROOT / "data"
for d in (DASHBOARD_DIR, DATA_DIR):
    d.mkdir(parents=True, exist_ok=True)

# ── Document type catalogue ──────────────────────────────────────────────────
DOC_TYPES = {
    "LP":      ("Lis Pendens",                    "lis_pendens"),
    "NOFC":    ("Notice of Foreclosure",           "foreclosure"),
    "TAXDEED": ("Tax Deed",                        "tax_deed"),
    "JUD":     ("Judgment",                        "judgment"),
    "CCJ":     ("Certified Judgment",              "judgment"),
    "DRJUD":   ("Domestic Judgment",               "judgment"),
    "LNCORPTX":("Corp Tax Lien",                   "tax_lien"),
    "LNIRS":   ("IRS Lien",                        "tax_lien"),
    "LNFED":   ("Federal Lien",                    "tax_lien"),
    "LN":      ("Lien",                            "lien"),
    "LNMECH":  ("Mechanic's Lien",                 "lien"),
    "LNHOA":   ("HOA Lien",                        "lien"),
    "MEDLN":   ("Medicaid Lien",                   "lien"),
    "PRO":     ("Probate",                         "probate"),
    "NOC":     ("Notice of Commencement",          "noc"),
    "RELLP":   ("Release Lis Pendens",             "release"),
}

# ── Parcel index (built once) ────────────────────────────────────────────────
_parcel_index: dict[str, dict] = {}


# ═══════════════════════════════════════════════════════════════════════════
# PARCEL DATA
# ═══════════════════════════════════════════════════════════════════════════

def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", str(s or "")).strip().upper()


def _col(row: dict, *names: str) -> str:
    for n in names:
        v = row.get(n, "")
        if v:
            return str(v).strip()
    return ""


def build_parcel_index(dbf_path: Path) -> dict:
    if DBF is None:
        return {}
    idx: dict[str, dict] = {}

    try:
        table = DBF(str(dbf_path), encoding="latin-1", ignore_missing_memofile=True)
        for row in table:
            try:
                owner_raw = _col(row, "OWNER", "OWN1", "OWNERNAME")
                site_addr = _col(row, "SITE_ADDR", "SITEADDR", "PROPADDR")
                site_city = _col(row, "SITE_CITY", "SITECITY", "PROPCITY")
                site_zip  = _col(row, "SITE_ZIP",  "SITEZIP",  "PROPZIP")
                mail_adr  = _col(row, "ADDR_1", "MAILADR1", "MAILADDR")
                mail_city = _col(row, "CITY", "MAILCITY")
                mail_st   = _col(row, "STATE", "MAILSTATE")
                mail_zip  = _col(row, "ZIP", "MAILZIP")

                rec = {
                    "prop_address": site_addr,
                    "prop_city":    site_city,
                    "prop_state":   "GA",
                    "prop_zip":     site_zip,
                    "mail_address": mail_adr,
                    "mail_city":    mail_city,
                    "mail_state":   mail_st,
                    "mail_zip":     mail_zip,
                }

                if not owner_raw:
                    continue

                parts = re.split(r",\s*", owner_raw, maxsplit=1)
                if len(parts) == 2:
                    last, first = parts[0].strip(), parts[1].strip()
                    variants = [
                        _norm(owner_raw),
                        _norm(f"{first} {last}"),
                        _norm(f"{last} {first}"),
                    ]
                else:
                    variants = [_norm(owner_raw)]

                for v in variants:
                    if v:
                        idx[v] = rec
            except Exception:
                pass
    except Exception as e:
        log.warning(f"DBF read error: {e}")

    log.info(f"Parcel index built: {len(idx):,} name variants")
    return idx


def lookup_parcel(owner: str) -> Optional[dict]:
    key = _norm(owner)
    return _parcel_index.get(key)


def download_parcel_dbf() -> Optional[Path]:
    cache_dir = REPO_ROOT / ".cache"
    cache_dir.mkdir(exist_ok=True)
    dbf_path = cache_dir / "parcels.dbf"

    if dbf_path.exists():
        age = time.time() - dbf_path.stat().st_mtime
        if age < 86400:
            log.info("Using cached parcel DBF")
            return dbf_path

    candidate_urls = [
        f"{PARCEL_BASE}/arcgis/rest/services/Cobb/ParcelData/MapServer/0/query"
        "?where=1%3D1&outFields=*&f=geojson",
        "https://gis.cobbcountyga.gov/download/parcels.zip",
        "https://gis.cobbcountyga.gov/download/Cobb_Parcels.zip",
        "https://www.cobbcountyga.gov/images/gis/data/parcels.zip",
    ]

    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (compatible; CobbLeadScraper/1.0)"})

    for url in candidate_urls:
        for attempt in range(MAX_RETRIES):
            try:
                log.info(f"Attempting parcel download: {url} (attempt {attempt+1})")
                r = session.get(url, timeout=60, stream=True)
                if r.status_code != 200:
                    break

                content_type = r.headers.get("Content-Type", "")

                if "zip" in content_type or url.endswith(".zip"):
                    zdata = b"".join(r.iter_content(65536))
                    try:
                        with zipfile.ZipFile(io.BytesIO(zdata)) as zf:
                            dbf_files = [n for n in zf.namelist() if n.lower().endswith(".dbf")]
                            if dbf_files:
                                with zf.open(dbf_files[0]) as f:
                                    dbf_path.write_bytes(f.read())
                                log.info(f"Parcel DBF extracted from {url}")
                                return dbf_path
                    except zipfile.BadZipFile:
                        pass

                if "json" in content_type or "geojson" in content_type:
                    data = r.json()
                    _build_index_from_geojson(data)
                    return None

            except Exception as e:
                log.warning(f"Parcel download attempt {attempt+1} failed: {e}")
                time.sleep(RETRY_DELAY)

    log.warning("Could not download parcel data — property enrichment will be skipped")
    return None


def _build_index_from_geojson(data: dict):
    global _parcel_index
    features = data.get("features", [])
    log.info(f"Building parcel index from {len(features)} GeoJSON features")
    for feat in features:
        props = feat.get("properties", {}) or {}
        owner = _col(props, "OWNER", "OWN1", "OWNERNAME", "OWNER_NAME")
        if not owner:
            continue
        rec = {
            "prop_address": _col(props, "SITE_ADDR", "SITEADDR", "ADDRESS"),
            "prop_city":    _col(props, "SITE_CITY", "SITECITY", "CITY"),
            "prop_state":   "GA",
            "prop_zip":     _col(props, "SITE_ZIP", "SITEZIP", "ZIP"),
            "mail_address": _col(props, "ADDR_1", "MAILADR1", "MAIL_ADDR"),
            "mail_city":    _col(props, "CITY", "MAILCITY", "MAIL_CITY"),
            "mail_state":   _col(props, "STATE", "MAILSTATE") or "GA",
            "mail_zip":     _col(props, "ZIP", "MAILZIP", "MAIL_ZIP"),
        }
        for v in _name_variants(owner):
            _parcel_index[v] = rec


def _name_variants(owner_raw: str) -> list[str]:
    parts = re.split(r",\s*", owner_raw.strip(), maxsplit=1)
    if len(parts) == 2:
        last, first = parts
        return list({
            _norm(owner_raw),
            _norm(f"{first} {last}"),
            _norm(f"{last} {first}"),
        })
    return [_norm(owner_raw)]


# ═══════════════════════════════════════════════════════════════════════════
# CLERK PORTAL SCRAPER  (Playwright)
# ═══════════════════════════════════════════════════════════════════════════

async def scrape_clerk(doc_type: str, date_from: str, date_to: str) -> list[dict]:
    records: list[dict] = []
    label, cat = DOC_TYPES.get(doc_type, (doc_type, "other"))

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx = await browser.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"
        )
        page = await ctx.new_page()

        for attempt in range(MAX_RETRIES):
            try:
                log.info(f"[{doc_type}] Loading clerk portal (attempt {attempt+1})")
                await page.goto(CLERK_BASE, timeout=60000, wait_until="networkidle")

                selectors_map = {
                    "doc_type":   ["#DocType", "#ddlDocType", "select[name*='DocType']", "select[name*='doctype']"],
                    "date_from":  ["#DateFrom", "#txtDateFrom", "input[name*='DateFrom']", "input[name*='dateFrom']"],
                    "date_to":    ["#DateTo", "#txtDateTo", "input[name*='DateTo']", "input[name*='dateTo']"],
                    "search_btn": ["#btnSearch", "#Button1", "input[type=submit]", "button[type=submit]"],
                }

                async def find_and_fill(keys: list[str], value: str, field_type="input"):
                    for sel in keys:
                        try:
                            el = page.locator(sel).first
                            if await el.count() > 0:
                                if field_type == "select":
                                    await el.select_option(value=value)
                                else:
                                    await el.fill(value)
                                return True
                        except Exception:
                            pass
                    return False

                await find_and_fill(selectors_map["doc_type"], doc_type, "select")
                await find_and_fill(selectors_map["date_from"], date_from)
                await find_and_fill(selectors_map["date_to"], date_to)

                for sel in selectors_map["search_btn"]:
                    try:
                        btn = page.locator(sel).first
                        if await btn.count() > 0:
                            await btn.click()
                            break
                    except Exception:
                        pass

                await page.wait_for_load_state("networkidle", timeout=30000)

                html = await page.content()
                records = _parse_clerk_results(html, doc_type, label, cat)

                page_num = 1
                while True:
                    next_btn = None
                    for sel in ["a:has-text('Next')", "a:has-text('>')", ".pager a:last-child"]:
                        try:
                            el = page.locator(sel).first
                            if await el.count() > 0:
                                next_btn = el
                                break
                        except Exception:
                            pass

                    if not next_btn:
                        break

                    page_num += 1
                    log.info(f"[{doc_type}] Fetching page {page_num}")
                    await next_btn.click()
                    await page.wait_for_load_state("networkidle", timeout=30000)
                    html = await page.content()
                    new_recs = _parse_clerk_results(html, doc_type, label, cat)
                    if not new_recs:
                        break
                    records.extend(new_recs)

                log.info(f"[{doc_type}] Found {len(records)} records")
                break

            except Exception as e:
                log.warning(f"[{doc_type}] Attempt {attempt+1} error: {e}")
                if attempt == MAX_RETRIES - 1:
                    log.error(f"[{doc_type}] All attempts failed")
                else:
                    await asyncio.sleep(RETRY_DELAY)

        await browser.close()

    return records


def _parse_clerk_results(html: str, doc_type: str, label: str, cat: str) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    records = []

    table = (
        soup.find("table", id=re.compile(r"grid|result|search", re.I))
        or soup.find("table", class_=re.compile(r"grid|result|search", re.I))
        or (soup.find_all("table") or [None])[-1]
    )

    if not table:
        return records

    rows = table.find_all("tr")
    if not rows:
        return records

    headers = [th.get_text(strip=True).lower() for th in rows[0].find_all(["th", "td"])]

    def col_idx(*names):
        for n in names:
            for i, h in enumerate(headers):
                if n in h:
                    return i
        return None

    idx_docnum  = col_idx("doc", "instrument", "book")
    idx_filed   = col_idx("filed", "date", "recorded")
    idx_grantor = col_idx("grantor", "owner", "from")
    idx_grantee = col_idx("grantee", "to", "buyer")
    idx_legal   = col_idx("legal", "description", "property")
    idx_amount  = col_idx("amount", "consideration", "value")

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
            href = link_tag["href"] if link_tag else ""
            if href and not href.startswith("http"):
                href = f"https://superiorcourtclerk.cobbcounty.gov{href}"

            doc_num = cell(idx_docnum) or (link_tag.get_text(strip=True) if link_tag else "")
            filed   = cell(idx_filed)
            grantor = cell(idx_grantor)
            grantee = cell(idx_grantee)
            legal   = cell(idx_legal)
            amount_raw = cell(idx_amount)
            amount = _parse_amount(amount_raw)

            if not doc_num and not grantor:
                continue

            records.append({
                "doc_num":   doc_num,
                "doc_type":  doc_type,
                "filed":     _norm_date(filed),
                "cat":       cat,
                "cat_label": label,
                "owner":     grantor,
                "grantee":   grantee,
                "amount":    amount,
                "legal":     legal,
                "clerk_url": href,
                "prop_address": "", "prop_city": "", "prop_state": "GA", "prop_zip": "",
                "mail_address": "", "mail_city": "", "mail_state": "", "mail_zip": "",
                "flags": [], "score": 0,
            })
        except Exception:
            pass

    return records


def _parse_amount(raw: str) -> float:
    try:
        cleaned = re.sub(r"[^\d.]", "", raw.replace(",", ""))
        return float(cleaned) if cleaned else 0.0
    except Exception:
        return 0.0


def _norm_date(raw: str) -> str:
    for fmt in ("%m/%d/%Y", "%m-%d-%Y", "%Y-%m-%d", "%m/%d/%y", "%B %d, %Y"):
        try:
            return datetime.strptime(raw.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return raw.strip()


# ═══════════════════════════════════════════════════════════════════════════
# LEAD SCORING & FLAGGING
# ═══════════════════════════════════════════════════════════════════════════

WEEK_AGO = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")


def compute_flags_and_score(rec: dict, all_records: list[dict]) -> tuple[list[str], int]:
    flags: list[str] = []
    score = 30

    dt  = rec.get("doc_type", "")
    cat = rec.get("cat", "")
    owner = rec.get("owner", "")

    if dt in ("LP", "RELLP"):
        flags.append("Lis pendens")
    if dt == "NOFC":
        flags.append("Pre-foreclosure")
    if cat == "judgment":
        flags.append("Judgment lien")
    if cat == "tax_lien":
        flags.append("Tax lien")
    if dt == "LNMECH":
        flags.append("Mechanic lien")
    if cat == "probate":
        flags.append("Probate / estate")
    if owner and re.search(r"\b(LLC|INC|CORP|LTD|TRUST|ESTATE)\b", owner.upper()):
        flags.append("LLC / corp owner")
    filed = rec.get("filed", "")
    if filed >= WEEK_AGO:
        flags.append("New this week")

    score += len(flags) * 10

    owner_key = _norm(owner)
    owner_docs = {r["doc_type"] for r in all_records if _norm(r.get("owner", "")) == owner_key}
    if "LP" in owner_docs and "NOFC" in owner_docs:
        score += 20

    amount = rec.get("amount", 0) or 0
    if amount > 100_000:
        score += 15
    elif amount > 50_000:
        score += 10

    if filed >= WEEK_AGO:
        score += 5

    has_addr = bool(rec.get("prop_address") or rec.get("mail_address"))
    if has_addr:
        score += 5

    return flags, min(score, 100)


# ═══════════════════════════════════════════════════════════════════════════
# ENRICHMENT
# ═══════════════════════════════════════════════════════════════════════════

def enrich_record(rec: dict) -> dict:
    parcel = lookup_parcel(rec.get("owner", ""))
    if parcel:
        for k, v in parcel.items():
            if v:
                rec[k] = v
    return rec


# ═══════════════════════════════════════════════════════════════════════════
# OUTPUT
# ═══════════════════════════════════════════════════════════════════════════

def _split_name(full: str) -> tuple[str, str]:
    full = full.strip()
    if "," in full:
        parts = full.split(",", 1)
        return parts[1].strip().title(), parts[0].strip().title()
    parts = full.split()
    if len(parts) >= 2:
        return parts[0].title(), " ".join(parts[1:]).title()
    return full.title(), ""


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
        "First Name", "Last Name", "Mailing Address", "Mailing City", "Mailing State",
        "Mailing Zip", "Property Address", "Property City", "Property State", "Property Zip",
        "Lead Type", "Document Type", "Date Filed", "Document Number", "Amount/Debt Owed",
        "Seller Score", "Motivated Seller Flags", "Source", "Public Records URL",
    ]
    with ghl_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in records:
            first, last = _split_name(r.get("owner", ""))
            writer.writerow({
                "First Name":            first,
                "Last Name":             last,
                "Mailing Address":       r.get("mail_address", ""),
                "Mailing City":          r.get("mail_city", ""),
                "Mailing State":         r.get("mail_state", ""),
                "Mailing Zip":           r.get("mail_zip", ""),
                "Property Address":      r.get("prop_address", ""),
                "Property City":         r.get("prop_city", ""),
                "Property State":        r.get("prop_state", "GA"),
                "Property Zip":          r.get("prop_zip", ""),
                "Lead Type":             r.get("cat_label", ""),
                "Document Type":         r.get("doc_type", ""),
                "Date Filed":            r.get("filed", ""),
                "Document Number":       r.get("doc_num", ""),
                "Amount/Debt Owed":      r.get("amount", ""),
                "Seller Score":          r.get("score", 0),
                "Motivated Seller Flags":"; ".join(r.get("flags", [])),
                "Source":               "Cobb County Superior Court Clerk",
                "Public Records URL":    r.get("clerk_url", ""),
            })
    log.info(f"GHL CSV → {ghl_path}")


# ═══════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════

async def main():
    global _parcel_index

    today     = datetime.now()
    date_to   = today.strftime("%m/%d/%Y")
    date_from = (today - timedelta(days=LOOK_BACK_DAYS)).strftime("%m/%d/%Y")
    log.info(f"Scraping {date_from} → {date_to}")

    log.info("Downloading parcel data…")
    dbf_path = download_parcel_dbf()
    if dbf_path and dbf_path.exists():
        _parcel_index = build_parcel_index(dbf_path)
    else:
        log.warning("Parcel index empty — address enrichment unavailable")

    all_records: list[dict] = []
    doc_types = list(DOC_TYPES.keys())

    batch_size = 3
    for i in range(0, len(doc_types), batch_size):
        batch = doc_types[i:i + batch_size]
        log.info(f"Scraping batch: {batch}")
        tasks = [scrape_clerk(dt, date_from, date_to) for dt in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for dt, res in zip(batch, results):
            if isinstance(res, Exception):
                log.error(f"[{dt}] Failed: {res}")
            else:
                all_records.extend(res)
        await asyncio.sleep(2)

    log.info(f"Total raw records: {len(all_records)}")

    for rec in all_records:
        try:
            enrich_record(rec)
            flags, score = compute_flags_and_score(rec, all_records)
            rec["flags"] = flags
            rec["score"] = score
        except Exception as e:
            log.warning(f"Scoring error for {rec.get('doc_num')}: {e}")
            rec["flags"] = []
            rec["score"] = 30

    all_records.sort(key=lambda r: r.get("score", 0), reverse=True)

    write_outputs(all_records, date_from, date_to)
    log.info("✅ Scrape complete")


if __name__ == "__main__":
    asyncio.run(main())


