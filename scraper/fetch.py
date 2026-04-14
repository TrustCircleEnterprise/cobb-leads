"""
Hillsborough County, FL - Motivated Seller Lead Scraper
Fast address matching using last-name index
"""
import json, logging, re, sys, time, requests, urllib3
from datetime import datetime, timedelta
from pathlib import Path
from collections import defaultdict

urllib3.disable_warnings()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
log = logging.getLogger("hillsborough")

REPO_ROOT = Path(__file__).parent.parent
DASH_DIR  = REPO_ROOT / "dashboard"
DATA_DIR  = REPO_ROOT / "data"
CACHE_DIR = REPO_ROOT / ".cache"
DBF_PATH  = CACHE_DIR / "parcels.dbf"
BASE_URL  = "https://publicrec.hillsclerk.com"
DATA_DIR.mkdir(exist_ok=True)

TARGET_DESC = {"LIS PENDENS","CERTIFIED COPY OF COURT JUDGMENT","JUDGMENT","FINAL JUDGMENT","FORECLOSURE","NOTICE OF DEFAULT","TAX DEED","TAX LIEN","IRS LIEN","FEDERAL TAX LIEN","CLAIM OF LIEN","MECHANIC LIEN","HOA LIEN","NOTICE OF COMMENCEMENT","PROBATE","LETTERS OF ADMINISTRATION"}
TARGET_CODE = {"LP","LPE","LIS","CCJ","JUD","JUDG","FJ","DJ","FC","FCL","NOD","TD","TDS","IRS","FTL","STL","TL","CL","ML","HOA","MECH","NOC","PRO","PROB"}

def is_target(code, desc):
    return code.upper() in TARGET_CODE or any(t in desc.upper() for t in TARGET_DESC)

def classify(code, desc):
    c,d = code.upper(), desc.upper()
    if "LIS PENDENS" in d or c in ("LP","LPE","LIS"): return "LP","Lis Pendens","lis_pendens"
    if "FORECLOSURE" in d or "DEFAULT" in d or c in ("FC","FCL","NOD"): return "FC","Foreclosure","foreclosure"
    if "TAX DEED" in d or c in ("TD","TDS"): return "TD","Tax Deed","tax_deed"
    if "JUDGMENT" in d or c in ("CCJ","JUD","JUDG","FJ","DJ"): return "JUD","Judgment","judgment"
    if any(x in d for x in ["TAX LIEN","IRS","FEDERAL TAX","STATE TAX"]) or c in ("IRS","FTL","STL","TL"): return "TL","Tax Lien","tax_lien"
    if any(x in d for x in ["MECHANIC","CLAIM OF LIEN","HOA"]) or c in ("CL","ML","HOA"): return "CL","Claim of Lien","lien"
    if "COMMENCEMENT" in d or c=="NOC": return "NOC","Notice of Commencement","noc"
    if "PROBATE" in d or "LETTERS" in d or c in ("PRO","PROB"): return "PRO","Probate","probate"
    return code, desc.title(), "other"

def load_parcels():
    if not DBF_PATH.exists(): return {}
    try:
        from dbfread import DBF
        log.info("Loading parcel DBF...")
        # Build last-name index for fast lookup
        index = defaultdict(list)
        for rec in DBF(str(DBF_PATH), encoding='latin-1'):
            name = (rec.get('OWNER') or '').strip().upper()
            if not name: continue
            # Skip entities
            if re.search(r'\b(LLC|INC|CORP|LTD|TRUST|BANK|MORTGAGE|STATE|COUNTY|CITY|FEDERAL)\b', name): continue
            last = name.split(',')[0].strip() if ',' in name else name.split()[0].strip()
            if len(last) >= 2:
                index[last].append((name, {
                    "prop_address": (rec.get('SITE_ADDR') or '').strip(),
                    "prop_city":    (rec.get('SITE_CITY') or '').strip(),
                    "prop_state":   "FL",
                    "prop_zip":     str(rec.get('SITE_ZIP') or '').strip(),
                    "mail_address": (rec.get('ADDR_1') or '').strip(),
                    "mail_city":    (rec.get('CITY') or '').strip(),
                    "mail_state":   (rec.get('STATE') or 'FL').strip(),
                    "mail_zip":     str(rec.get('ZIP') or '').strip(),
                }))
        log.info(f"Loaded {sum(len(v) for v in index.values()):,} parcel records into {len(index):,} name buckets")
        return index
    except Exception as e:
        log.warning(f"DBF load failed: {e}")
        return {}

def match_parcel(name, index, threshold=85):
    if not name or not index: return None
    name = name.upper().strip()
    if re.search(r'\b(LLC|INC|CORP|LTD|TRUST|BANK|MORTGAGE|STATE OF|COUNTY|CITY OF|FEDERAL|LVNV|CREDIT|FUNDING)\b', name): return None
    # Get last name for fast bucket lookup
    last = name.split(',')[0].strip() if ',' in name else name.split()[0].strip()
    if len(last) < 2: return None
    candidates = index.get(last, [])
    if not candidates: return None
    try:
        from rapidfuzz import fuzz
        best_score = 0
        best_result = None
        for parcel_name, data in candidates:
            score = fuzz.token_sort_ratio(name, parcel_name)
            if score > best_score:
                best_score = score
                best_result = data
        if best_score >= threshold and best_result and best_result.get("prop_address"):
            return best_result
    except: pass
    return None

def get_files(session):
    r = session.get(BASE_URL + "/OfficialRecords/DailyIndexes/", timeout=15, verify=False)
    if not r.ok: return []
    return re.findall(r'HREF="/OfficialRecords/DailyIndexes/(D\d{8}01id\.29)"', r.text)

def scrape_file(session, fname):
    date_str = f"{fname[1:5]}-{fname[5:7]}-{fname[7:9]}"
    docs = {}
    parties = {}

    # D file = document info
    r = session.get(f"{BASE_URL}/OfficialRecords/DailyIndexes/{fname}", timeout=30, verify=False)
    if r.ok:
        for line in r.text.splitlines():
            p = line.split('|')
            if len(p) < 13 or p[0] != 'DDA': continue
            inst,code,desc,legal = p[2],p[3],p[4],p[5]
            try: date_rec = datetime.strptime(p[10], "%m/%d/%Y").strftime("%Y-%m-%d")
            except: date_rec = date_str
            amount = float(re.sub(r'[^\d.]','',p[12]) or 0)
            docs[inst] = {"doc_num":inst,"code":code,"desc":desc,"legal":legal,"date_rec":date_rec,"amount":amount}

    # P file = party names
    pfname = 'P' + fname[1:]
    r2 = session.get(f"{BASE_URL}/OfficialRecords/DailyIndexes/{pfname}", timeout=30, verify=False)
    if r2.ok:
        for line in r2.text.splitlines():
            p = line.split('|')
            if len(p) < 6 or p[0] != 'DPA': continue
            inst,direction,name = p[2],p[4],p[5]
            if inst not in parties: parties[inst] = {"FRM":[],"TO":[]}
            if direction in parties[inst]: parties[inst][direction].append(name)

    records = []
    for inst,doc in docs.items():
        if not is_target(doc["code"],doc["desc"]): continue
        pt = parties.get(inst,{"FRM":[],"TO":[]})
        grantor = ", ".join(pt["FRM"])
        grantee = ", ".join(pt["TO"])
        matched,label,cat = classify(doc["code"],doc["desc"])
        records.append({
            "doc_num":inst,"doc_type":matched,"filed":doc["date_rec"],
            "cat":cat,"cat_label":label,"owner":grantor,"grantee":grantee,
            "amount":doc["amount"],"legal":doc["legal"],"county":"Hillsborough",
            "clerk_url":f"https://pubrec6.hillsclerk.com/or/instruments/{inst}",
            "prop_address":"","prop_city":"","prop_state":"FL","prop_zip":"",
            "mail_address":"","mail_city":"","mail_state":"","mail_zip":"",
            "flags":[],"score":0,
        })
    return records

WEEK_AGO = (datetime.now()-timedelta(days=7)).strftime("%Y-%m-%d")

def score_record(rec):
    flags,score = [],30
    dt,cat = rec.get("doc_type",""),rec.get("cat","")
    grantee,filed = rec.get("grantee",""),rec.get("filed","")
    if dt=="LP": flags.append("Lis pendens")
    if dt=="FC": flags.append("Pre-foreclosure")
    if cat=="judgment": flags.append("Judgment lien")
    if cat=="tax_lien": flags.append("Tax lien")
    if cat=="lien": flags.append("Mechanic lien")
    if cat=="probate": flags.append("Probate / estate")
    if re.search(r'\b(LLC|INC|CORP|LTD|TRUST|ESTATE)\b', grantee.upper()): flags.append("LLC / corp owner")
    if filed>=WEEK_AGO: flags.append("New this week")
    score += len(flags)*10
    amt = rec.get("amount",0) or 0
    if amt>100000: score+=15
    elif amt>50000: score+=10
    if filed>=WEEK_AGO: score+=5
    if rec.get("prop_address"): score+=10
    return flags, min(score,100)

def main():
    log.info("Hillsborough County FL - Motivated Seller Scraper")
    index = load_parcels()
    session = requests.Session()
    session.headers.update({"User-Agent":"Mozilla/5.0"})

    fnames = get_files(session)
    log.info(f"Found {len(fnames)} index files")

    cutoff = datetime.now()-timedelta(days=21)
    recent = [f for f in fnames if datetime(int(f[1:5]),int(f[5:7]),int(f[7:9]))>=cutoff]
    log.info(f"Processing {len(recent)} recent files")

    all_records = []
    for fname in recent:
        try:
            recs = scrape_file(session, fname)
            log.info(f"  {fname}: {len(recs)} records")
            all_records.extend(recs)
            time.sleep(0.5)
        except Exception as e:
            log.warning(f"  {fname}: {e}")

    seen,unique = set(),[]
    for r in all_records:
        k = r.get("doc_num") or f"{r['owner']}_{r['filed']}"
        if k not in seen: seen.add(k); unique.append(r)
    all_records = unique
    log.info(f"Total unique: {len(all_records)}")

    if index:
        log.info("Matching addresses (fast index)...")
        matched = 0
        for rec in all_records:
            for name in [rec.get("grantee",""), rec.get("owner","")]:
                result = match_parcel(name, index)
                if result and result.get("prop_address") and not result["prop_address"].startswith("0 "):
                    rec.update(result); matched+=1; break
        log.info(f"Matched: {matched}/{len(all_records)}")

    for rec in all_records:
        rec["flags"],rec["score"] = score_record(rec)
    all_records.sort(key=lambda r: r.get("score",0), reverse=True)

    with_addr = sum(1 for r in all_records if r.get("prop_address"))
    payload = {
        "fetched_at": datetime.utcnow().isoformat()+"Z",
        "source": "Hillsborough County Clerk of Circuit Courts",
        "county": "Hillsborough", "state": "FL",
        "date_range": {"from":(datetime.now()-timedelta(days=21)).strftime("%Y-%m-%d"),"to":datetime.now().strftime("%Y-%m-%d")},
        "total": len(all_records), "with_address": with_addr, "records": all_records,
    }

    out = DASH_DIR/"records.json"
    out.write_text(json.dumps(payload,indent=2,default=str))
    (DATA_DIR/"hillsborough.json").write_text(json.dumps(payload,indent=2,default=str))
    log.info(f"Wrote {len(all_records)} records ({with_addr} with address)")
    log.info("Done!")

if __name__=="__main__": main()
