"""
Hillsborough Leads — Post-scrape automation
1. Exports Xleads-ready CSV (Street Address, City, State, Zip)
2. Sends email + text notification
3. Pushes matched leads directly into GHL workflow
"""

import csv
import json
import smtplib
import requests
import time
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from pathlib import Path

GMAIL_USER     = "trustcircleenterprise@gmail.com"
GMAIL_PASS     = "ylrw gtsz eqbv zjjk"
PHONE          = "6783877973"
CARRIER_GATE   = "vtext.com"
GHL_API_KEY    = "pit-43fab20f-2ff3-438f-b074-2a7a78276896"
GHL_LOCATION   = "DCHwcJVyBixvcmK0DO37"
GHL_WORKFLOW   = "48fa268e-a09a-4515-92ad-1ff60bbfd08d"
GHL_API_BASE   = "https://services.leadconnectorhq.com"
REPO_ROOT      = Path(__file__).parent.parent
DATA_DIR       = REPO_ROOT / "data"
DASHBOARD_DIR  = REPO_ROOT / "dashboard"
RECORDS_JSON   = DASHBOARD_DIR / "records.json"
XLEADS_CSV     = DATA_DIR / "xleads_import.csv"

def load_records():
    with open(RECORDS_JSON) as f:
        data = json.load(f)
    return data

def is_llc(name):
    import re
    return bool(re.search(r"\b(LLC|INC|CORP|LTD|TRUST|ESTATE|STATE OF|COUNTY|CITY OF)\b", (name or "").upper()))

def export_xleads_csv(records):
    matched = [r for r in records if r.get("prop_address") and not r["prop_address"].startswith("0 ") and not is_llc(r.get("grantee", ""))]
    with open(XLEADS_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["Street Address", "City", "State", "Zip"])
        writer.writeheader()
        for r in matched:
            writer.writerow({"Street Address": r.get("prop_address", ""), "City": r.get("prop_city", ""), "State": r.get("prop_state", "FL"), "Zip": r.get("prop_zip", "")})
    print(f"Xleads CSV: {len(matched)} records -> {XLEADS_CSV}")
    return len(matched)

def send_notification(total, with_address, xleads_count):
    subject = f"Hillsborough Leads Ready - {datetime.now().strftime('%b %d')}"
    body = f"""Your daily Hillsborough County motivated seller leads are ready!

Summary:
   Total leads scraped:   {total}
   With property address: {with_address}
   Ready for Xleads:      {xleads_count}

Xleads CSV is attached to this email.

Next steps:
1. Upload the attached CSV to Xleads for skip tracing
2. Download the skip traced results from Xleads
3. Run: python3 scraper/import_xleads.py ~/Downloads/xleads_results.csv

Dashboard: https://trustcircleenterprise.github.io/cobb-leads/
"""
    try:
        msg = MIMEMultipart()
        msg["From"] = GMAIL_USER
        msg["To"] = GMAIL_USER
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))
        with open(XLEADS_CSV, "rb") as f:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", f"attachment; filename=xleads_import_{datetime.now().strftime('%Y%m%d')}.csv")
            msg.attach(part)
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(GMAIL_USER, GMAIL_PASS)
            server.send_message(msg)
        print("Email sent!")
    except Exception as e:
        print(f"Email failed: {e}")
    try:
        sms_msg = MIMEText(f"Hillsborough leads ready! {total} total, {with_address} with address, {xleads_count} for Xleads. Check email for CSV.")
        sms_msg["From"] = GMAIL_USER
        sms_msg["To"] = f"{PHONE}@{CARRIER_GATE}"
        sms_msg["Subject"] = ""
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(GMAIL_USER, GMAIL_PASS)
            server.send_message(sms_msg)
        print("Text sent!")
    except Exception as e:
        print(f"Text failed: {e}")

def push_to_ghl(records):
    headers = {"Authorization": f"Bearer {GHL_API_KEY}", "Content-Type": "application/json", "Version": "2021-07-28"}
    matched = [r for r in records if r.get("prop_address") and not r["prop_address"].startswith("0 ") and not is_llc(r.get("grantee", ""))]
    success = 0; failed = 0
    for r in matched:
        grantee = r.get("grantee", "") or r.get("owner", "")
        if "," in grantee:
            parts = grantee.split(",", 1); last = parts[0].strip().title(); first = parts[1].strip().title()
        else:
            parts = grantee.split(); first = parts[0].title() if parts else ""; last = " ".join(parts[1:]).title() if len(parts) > 1 else ""
        contact_payload = {"firstName": first, "lastName": last, "address1": r.get("prop_address", ""), "city": r.get("prop_city", ""), "state": r.get("prop_state", "FL"), "postalCode": r.get("prop_zip", ""), "locationId": GHL_LOCATION, "tags": r.get("flags", []) + ["Hillsborough Motivated Seller"]}
        try:
            res = requests.post(f"{GHL_API_BASE}/contacts/upsert", headers=headers, json=contact_payload, timeout=10)
            if res.status_code in (200, 201):
                contact_id = res.json().get("contact", {}).get("id")
                if contact_id:
                    wf_res = requests.post(f"{GHL_API_BASE}/contacts/{contact_id}/workflow/{GHL_WORKFLOW}", headers=headers, timeout=10)
                    if wf_res.status_code in (200, 201): success += 1
                    else: failed += 1; print(f"Workflow failed for {grantee}: {wf_res.text}")
            else:
                failed += 1; print(f"Contact failed for {grantee}: {res.text}")
            time.sleep(0.2)
        except Exception as e:
            failed += 1; print(f"Error for {grantee}: {e}")
    print(f"GHL: {success} added to workflow, {failed} failed")
    return success, failed

def main():
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] Running post-scrape automation...")
    data = load_records()
    records = data.get("records", [])
    total = data.get("total", len(records))
    with_address = data.get("with_address", 0)
    xleads_count = export_xleads_csv(records)
    print("Pushing to GHL workflow...")
    success, failed = push_to_ghl(records)
    print("Sending notifications...")
    send_notification(total, with_address, xleads_count)
    print(f"Done! {success} contacts pushed to GHL, {xleads_count} ready for Xleads.")

if __name__ == "__main__":
    main()
