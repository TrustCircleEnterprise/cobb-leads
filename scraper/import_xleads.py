import csv, sys, time, re, os, requests
from datetime import datetime
from urllib.parse import quote

GHL_API_KEY="pit-43fab20f-2ff3-438f-b074-2a7a78276896"
GHL_LOCATION_ID="DCHwcJVyBixvcmK0DO37"
GHL_WORKFLOW_ID="48fa268e-a09a-4515-92ad-1ff60bbfd08d"
DEAL_TOOL="https://trustcircleenterprise.github.io/florida-leads/deal.html"
HEADERS={"Authorization":f"Bearer {GHL_API_KEY}","Content-Type":"application/json","Version":"2021-07-28"}

def clean_phone(p):
    if not p: return ""
    digits=re.sub(r"\D","",str(p))
    if len(digits)==10: return "+1"+digits
    if len(digits)==11 and digits[0]=="1": return "+"+digits
    return ""

def valid_email(e):
    return bool(re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$",str(e).strip()))

def get_or_create_contact(row):
    first=(row.get("FirstName") or "").strip()
    last=(row.get("LastName") or "").strip()
    phone=""
    for i in range(1,4):
        pt=str(row.get(f"Contact1Phone_{i}_Type") or "").lower()
        pv=clean_phone(row.get(f"Contact1Phone_{i}") or "")
        pd=str(row.get(f"Contact1Phone_{i}_DNC") or "").lower()
        pl=str(row.get(f"Contact1Phone_{i}_Litigator") or "").lower()
        if pv and pt=="mobile" and pd!="true" and pl!="true":
            phone=pv; break
    raw_email=(row.get("Contact1Email_1") or "").strip()
    email=raw_email if valid_email(raw_email) else ""
    if not phone and not email: return None,"No mobile/email"
    prop_addr=(row.get("PropertyAddress") or "").strip()
    prop_city=(row.get("PropertyCity") or "").strip()
    prop_state=(row.get("PropertyState") or "").strip()
    prop_zip=str(row.get("PropertyPostalCode") or "").strip()
    last_sale=(row.get("LastSalesPrice") or "").strip()

    # Build Deal Tool URL pre-filled with property info
    deal_url=f"{DEAL_TOOL}?address={quote(prop_addr)}&city={quote(prop_city)}&zip={quote(prop_zip)}&seller={quote(first)}&type=land"

    payload={
        "locationId":GHL_LOCATION_ID,
        "firstName":first,
        "lastName":last,
        "phone":phone,
        "address1":prop_addr,
        "city":prop_city,
        "state":prop_state,
        "postalCode":prop_zip,
        "tags":["Motivated Seller","Hillsborough Leads"],
        "source":"TrustCircle Scraper",
        "customFields":[
            {"key":"last_sale_price","field_value":last_sale},
            {"key":"deal_tool_link","field_value":deal_url}
        ]
    }
    if email: payload["email"]=email

    res=requests.post("https://services.leadconnectorhq.com/contacts/upsert",headers=HEADERS,json=payload)
    if res.ok:
        data=res.json()
        cid=data.get("contact",{}).get("id") or data.get("id")
        if cid: return cid,None
    res=requests.post("https://services.leadconnectorhq.com/contacts/",headers=HEADERS,json=payload)
    if res.ok:
        cid=res.json().get("contact",{}).get("id")
        if cid: return cid,None
    return None,res.text

def main():
    if len(sys.argv)<2: print("Usage: python3 import_xleads.py file.csv"); sys.exit(1)
    f=sys.argv[1]
    if not os.path.exists(f): print(f"Not found: {f}"); sys.exit(1)
    with open(f,encoding="utf-8-sig") as fh: rows=list(csv.DictReader(fh))
    print(f"Processing {len(rows)} rows...")
    added=skipped=failed=0
    for i,row in enumerate(rows):
        cid,err=get_or_create_contact(row)
        if not cid:
            if "No mobile" in str(err): skipped+=1
            else: failed+=1; print(f"  Failed {row.get('FirstName')}: {str(err)[:60]}")
        else:
            requests.post(f"https://services.leadconnectorhq.com/contacts/{cid}/workflow/{GHL_WORKFLOW_ID}",headers=HEADERS,json={})
            added+=1
            if added%10==0: print(f"  {added} pushed, {skipped} skipped, {failed} failed...")
        time.sleep(0.2)
    print(f"\n✅ Done! {added} pushed to GHL, {skipped} skipped, {failed} failed")

if __name__=="__main__": main()
