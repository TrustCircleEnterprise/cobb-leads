"""
Hillsborough County, FL — Motivated Seller Lead Scraper
Uses Official Records Daily Index files from publicrec.hillsclerk.com
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

try:
    from dbfread import DBF
except ImportError:
    DBF = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("hillsborough_scraper")

INDEX_BASE     = "https://publicrec.hillsclerk.com/OfficialRecords/DailyIndexes"
CLERK_BASE     = "https://publicaccess.hillsclerk.com/oripublicaccess"
LOOK_BACK_DAYS = 14
MAX_RETRIES    = 3
RETRY_DELAY    = 3

REPO_ROOT     = Path(__file__).parent.parent
DASHBOARD_DIR = REPO_ROOT / "dashboard"
DATA_DIR      = REPO_ROOT / "data"
for d in (DASHBOARD_DIR, DATA_DIR):
    d.mkdir(parents=True, exist_ok=True)

# Target doc codes from the D file (col index 3)
TARGET_CODES = {
    "LP", "LIS", "LISP",           # Lis Pendens
    "NF", "NOF", "NOFC",           # Notice of Foreclosure
    "FC", "FORE", "FORECL",        # Foreclosure
    "TD", "TAXD",                  # Tax Deed
    "JUD", "JUDG", "JUDGMENT",     # Judgment
    "CJ", "CERJ",                  # Certified Judgment
    "FJ", "FINJ",                  # Final Judgment
    "ITL", "IRS", "IRSL",          # IRS/Federal Tax Lien
    "STL", "STTL",                 # State Tax Lien
    "FTL", "FEDL",                 # Federal Tax Lien
    "CL", "CLOL", "COL",           # Claim of Lien
    "ML", "MECL", "MECH",          # Mechanic's Lien
    "HL", "HOAL",                  # HOA Lien
    "NOC", "NOCOM",                # Notice of Commencement
    "RLP", "RELLP",                # Release of Lis Pendens
    "SL", "SATL",                  # Satisfaction of Lien
}

# Also match by doc name (col 4) if code doesn't match
TARGET_NAMES = [
    "LIS PENDENS", "NOTICE OF LIS PENDENS",
    "FORECLOSURE", "NOTICE OF FORECLOSURE",
    "TAX DEED",
    "JUDGMENT", "FINAL JUDGMENT", "CERTIFIED JUDGMENT",
    "IRS LIEN", "FEDERAL TAX LIEN", "STATE TAX LIEN", "TAX LIEN",
    "CLAIM OF LIEN", "MECHANIC", "MECHANICS LIEN",
    "HOA LIEN",
    "NOTICE OF COMMENCEMENT",
    "RELEASE OF LIS PENDENS",
]

def classify(code: str, name: str) -> tuple[str, str, str]:
    """Returns (matched_type, label, cat)"""
    code = code.upper().strip()
    name = name.upper().strip()

    if any(x in name for x in ["LIS PENDENS"]) or code in ("LP","LIS","LISP"):
        return "LP", "Lis Pendens", "lis_pendens"
    if any(x in name for x in ["FORECLOSURE"]) or code in ("NF","FC","NOF","NOFC","FORE"):
        return "FC", "Foreclosure", "foreclosure"
    if "TAX DEED" in name or code in ("TD","TAXD"):
        return "TD", "Tax Deed", "tax_deed"
    if any(x in name for x in ["JUDGMENT"]) or code in ("JUD","JUDG","CJ","FJ","CERJ","FINJ"):
        return "JUD", "Judgment", "judgment"
    if any(x in name for x in ["IRS","FEDERAL TAX LIEN","FED TAX"]) or code in ("ITL","IRS","IRSL","FTL","FEDL"):
        return "ITL", "Federal/IRS Tax Lien", "tax_lien"
    if "STATE TAX LIEN" in name or code in ("STL","STTL"):
        return "STL", "State Tax Lien", "tax_lien"
    if any(x in name for x in ["MECHANIC"]) or code in ("ML","MECL","MECH"):
        return "ML", "Mechanic's Lien", "lien"
    if "HOA" in name or code in ("HL","HOAL"):
        return "HL", "HOA Lien", "lien"
    if any(x in name for x in ["CLAIM OF LIEN"]) or code in ("CL","CLOL","COL"):
        return "CL", "Claim of Lien", "lien"
    if "NOTICE OF COMMENCEMENT" in name or code in ("NOC","NOCOM"):
        return "NOC", "Notice of Commencement", "noc"
    if "RELEASE OF LIS PENDENS" in name or code in ("RLP","RELLP"):
        return "RLP", "Release Lis Pendens", "release"
    if any(x in name for x in ["SATISFACTION OF LIEN"]) or code in ("SL","SATL"):
        return "SL", "Satisfaction of Lien", "release"
    return code, name.title(), "other"


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

def download_parcel_dbf() -> Optional[Path]:
    cache_dir = REPO_ROOT / ".cache"
    cache_dir.mkdir(exist_ok=True)
    dbf_path = cache_dir / "parcels.dbf"
    if dbf_path.exists() and (time.time() - dbf_path.stat().st_mtime) < 86400:
        log.info("Using cached parcel data")
        return dbf_path
    urls = [
        "https://gis.hcpafl.org/downloadfiles/Shapefiles/Parcels.zip",
        "https://gis.hcpafl.org/downloadfiles/Parcels.zip",
    ]
    s = requests.Session()
    s.headers.update({"User-Agent": "Mozilla/5.0"})
    for url in urls:
        try:
            log.info(f"Parcel download: {url}")
            r = s.get(url, timeout=120, verify=False, stream=True)
            if r.status_code == 200:
                zdata = b"".join(r.iter_content(65536))
                with zipfile.ZipFile(io.BytesIO(zdata)) as zf:
                    dbf_files = [n for n in zf.namelist() if n.lower().endswith(".dbf")]
                    if dbf_files:
                        dbf_path.write_bytes(zf.read(dbf_files[0]))
                        log.info("Parcel DBF saved")
                        return dbf_path
        except Exception as e:
            log.warning(f"Parcel download failed: {e}")
    log.warning("Parcel data unavailable")
    return None

def build_parcel_index(dbf_path: Path) -> dict:
    if DBF is None:
        return {}
    idx: dict[str, dict] = {}
    try:
        table = DBF(str(dbf_path), encoding="latin-1", ignore_missing_memofile=True)
        for row in table:
            try:
                owner_raw = _col(row, "OWNER", "OWN1", "OWN_NAME")
                if not owner_raw:
                    continue
                rec = {
                    "prop_address": _col(row, "SITE_ADDR", "SITEADDR"),
                    "prop_city":    _col(row, "SITE_CITY", "SITECITY"),
                    "prop_state":   "FL",
                    "prop_zip":     _col(row, "SITE_ZIP", "SITEZIP"),
                    "mail_address": _col(row, "ADDR_1", "MAILADR1"),
                    "mail_city":    _col(row, "CITY", "MAILCITY"),
                    "mail_state":   _col(row, "STATE", "MAILSTATE") or "FL",
                    "mail_zip":     _col(row, "ZIP", "MAILZIP"),
                }
                for v in _name_variants(owner_raw):
                    if v:
                        idx[v] = rec
            except Exception:
                pass
    except Exception as e:
        log.warning(f"DBF error: {e}")
    log.info(f"Parcel index: {len(idx):,} variants")
    return idx


def fetch_file(url: str) -> Optional[str]:
    s = requests.Session()
    s.headers.update({"User-Agent": "Mozilla/5.0"})
    for attempt in range(MAX_RETRIES):
        try:
            r = s.get(url, timeout=30, verify=False)
            if r.status_code == 200:
                return r.text
            if r.status_code == 404:
                return None
            log.warning(f"HTTP {r.status_code} for {url}")
            return None
        except Exception as e:
            log.warning(f"Fetch attempt {attempt+1}: {e}")
            time.sleep(RETRY_DELAY)
    return None


def parse_d_file(content: str) -> list[dict]:
    """
    Format: DDA|29|InstrumentNum|DocCode|DocName|Legal|...|Pages|Date|Time||
    Col 0: DDA
    Col 2: Instrument number
    Col 3: Doc code
    Col 4: Doc name
    Col 5: Legal description
    Col 10: Date (MM/DD/YYYY)
    """
    records = []
    for line in content.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split("|")
        if len(parts) < 5:
            continue
        try:
            instrument = parts[2].strip() if len(parts) > 2 else ""
            doc_code   = parts[3].strip().upper() if len(parts) > 3 else ""
            doc_name   = parts[4].strip().upper() if len(parts) > 4 else ""
            legal      = parts[5].strip() if len(parts) > 5 else ""
            rec_date   = parts[10].strip() if len(parts) > 10 else ""

            # Check if target
            is_target = (
                doc_code in TARGET_CODES or
                any(n in doc_name for n in TARGET_NAMES)
            )
            if not is_target:
                continue

            matched, label, cat = classify(doc_code, doc_name)
            clerk_url = f"{CLERK_BASE}/?instrument={instrument}" if instrument else ""

            records.append({
                "doc_num":      instrument,
                "doc_type":     matched,
                "filed":        _norm_date(rec_date),
                "cat":          cat,
                "cat_label":    label,
                "owner":        "",
                "grantee":      "",
                "amount":       0.0,
                "legal":        legal,
                "clerk_url":    clerk_url,
                "prop_address": "", "prop_city": "", "prop_state": "FL", "prop_zip": "",
                "mail_address": "", "mail_city": "", "mail_state": "", "mail_zip": "",
                "flags": [], "score": 0,
            })
        except Exception as e:
            log.warning(f"Parse error: {e}")
    return records


def parse_p_file(content: str) -> dict[str, dict]:
    """
    Format: DPA|29|InstrumentNum|PartyType|PartyName|...
    PartyType: GR=Grantor, GE=Grantee (or 1/2)
    """
    parties: dict[str, dict] = {}
    for line in content.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split("|")
        if len(parts) < 5:
            continue
        try:
            instrument  = parts[2].strip()
            party_type  = parts[4].strip().upper()  # FRM or TO
            party_name  = parts[5].strip() if len(parts) > 5 else ""

            if instrument not in parties:
                parties[instrument] = {"grantor": "", "grantee": ""}

            if party_type == "FRM" and not parties[instrument]["grantor"]:
                parties[instrument]["grantor"] = party_name
            elif party_type == "TO" and not parties[instrument]["grantee"]:
                parties[instrument]["grantee"] = party_name
        except Exception:
            pass
    return parties


def scrape_day(date_str: str) -> list[dict]:
    """Scrape one day. date_str = YYYYMMDD"""
    d_url = f"{INDEX_BASE}/D{date_str}01id.29"
    d_content = fetch_file(d_url)
    if not d_content:
        return []

    records = parse_d_file(d_content)
    log.info(f"  {date_str}: {len(records)} target records")

    if not records:
        return []

    p_url = f"{INDEX_BASE}/P{date_str}01id.29"
    p_content = fetch_file(p_url)
    if p_content:
        parties = parse_p_file(p_content)
        log.info(f"  Party file: {len(parties)} instruments")
        # Log first party entry to check format
        if parties:
            first = list(parties.items())[0]
            log.info(f"  Sample party: {first}")
        for rec in records:
            instr = rec["doc_num"]
            if instr in parties:
                rec["owner"]   = parties[instr]["grantor"]
                rec["grantee"] = parties[instr]["grantee"]

    return records


def _norm_date(raw: str) -> str:
    raw = raw.strip()
    for fmt in ("%m/%d/%Y", "%Y%m%d", "%Y-%m-%d", "%m-%d-%Y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return raw


WEEK_AGO = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")


def compute_flags_and_score(rec: dict, all_records: list[dict]) -> tuple[list[str], int]:
    flags: list[str] = []
    score = 30
    dt    = rec.get("doc_type", "")
    cat   = rec.get("cat", "")
    owner = rec.get("owner", "")
    filed = rec.get("filed", "")

    if dt == "LP":                   flags.append("Lis pendens")
    if dt in ("NF","FC"):           flags.append("Pre-foreclosure")
    if cat == "judgment":            flags.append("Judgment lien")
    if cat == "tax_lien":            flags.append("Tax lien")
    if dt in ("ML","CL"):           flags.append("Mechanic lien")
    if dt == "HL":                   flags.append("HOA lien")
    if cat == "probate":             flags.append("Probate / estate")
    if owner and re.search(r"\b(LLC|INC|CORP|LTD|TRUST|ESTATE)\b", owner.upper()):
        flags.append("LLC / corp owner")
    if filed >= WEEK_AGO:            flags.append("New this week")

    score += len(flags) * 10
    owner_key  = _norm(owner)
    owner_docs = {r["doc_type"] for r in all_records if _norm(r.get("owner","")) == owner_key}
    if "LP" in owner_docs and any(d in owner_docs for d in ("NF","FC")):
        score += 20
    amt = rec.get("amount", 0) or 0
    if amt > 100_000:   score += 15
    elif amt > 50_000:  score += 10
    if filed >= WEEK_AGO: score += 5
    if rec.get("prop_address") or rec.get("mail_address"): score += 5
    return flags, min(score, 100)


def enrich_record(rec: dict) -> dict:
    # Try grantee first (property owner being sued/liened), then grantor
    for name in [rec.get("grantee",""), rec.get("owner","")]:
        if not name:
            continue
        parcel = lookup_parcel(name)
        if parcel and parcel.get("prop_address"):
            for k, v in parcel.items():
                if v:
                    rec[k] = v
            return rec
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
        "source":       "Hillsborough County Clerk of Circuit Courts",
        "county":       "Hillsborough",
        "state":        "FL",
        "date_range":   {"from": date_from, "to": date_to},
        "total":        len(records),
        "with_address": with_address,
        "records":      records,
    }
    for path in [DASHBOARD_DIR / "records.json", DATA_DIR / "records.json"]:
        path.write_text(json.dumps(payload, indent=2, default=str))
        log.info(f"Wrote {len(records)} records to {path}")

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
                "Property State":  r.get("prop_state","FL"),
                "Property Zip":    r.get("prop_zip",""),
                "Lead Type":       r.get("cat_label",""),
                "Document Type":   r.get("doc_type",""),
                "Date Filed":      r.get("filed",""),
                "Document Number": r.get("doc_num",""),
                "Amount/Debt Owed":r.get("amount",""),
                "Seller Score":    r.get("score",0),
                "Motivated Seller Flags": "; ".join(r.get("flags",[])),
                "Source":          "Hillsborough County Clerk of Circuit Courts",
                "Public Records URL": r.get("clerk_url",""),
            })
    log.info("GHL CSV written")


def business_days_back(n: int) -> list[str]:
    days = []
    d = datetime.now()
    while len(days) < n:
        d -= timedelta(days=1)
        if d.weekday() < 5:
            days.append(d.strftime("%Y%m%d"))
    return days


def main():
    global _parcel_index
    log.info("Hillsborough County FL — Motivated Seller Scraper")

    dbf_path = download_parcel_dbf()
    if dbf_path and dbf_path.exists():
        _parcel_index = build_parcel_index(dbf_path)

    days = business_days_back(LOOK_BACK_DAYS)
    log.info(f"Checking {len(days)} business days: {days[-1]} to {days[0]}")

    all_records: list[dict] = []
    found_days = 0
    for day in reversed(days):
        try:
            recs = scrape_day(day)
            if recs:
                found_days += 1
                all_records.extend(recs)
                log.info(f"Day {day}: {len(recs)} records")
            time.sleep(0.5)
        except Exception as e:
            log.warning(f"Day {day} failed: {e}")

    log.info(f"Found data for {found_days} days")

    seen = set()
    unique = []
    for r in all_records:
        key = r.get("doc_num","") or id(r)
        if key not in seen:
            seen.add(key)
            unique.append(r)
    all_records = unique
    log.info(f"Total unique records: {len(all_records)}")

    for rec in all_records:
        try:
            enrich_record(rec)
            flags, score = compute_flags_and_score(rec, all_records)
            rec["flags"] = flags
            rec["score"] = score
        except Exception as e:
            rec["flags"] = []
            rec["score"] = 30

    all_records.sort(key=lambda r: r.get("score", 0), reverse=True)

    date_from = days[-1]
    date_to   = days[0]
    write_outputs(all_records, date_from, date_to)
    log.info("Done")


if __name__ == "__main__":
    main()
