
import requests

sectors = {
    "NIFTY AUTO": "ind_niftyautolist.csv",
    "NIFTY BANK": "ind_niftybanklist.csv",
    "NIFTY FINANCIAL SERVICES": "ind_niftyfinancelist.csv",
    "NIFTY FMCG": "ind_niftyfmcglist.csv",
    "NIFTY HEALTHCARE": "ind_niftyhealthcarelist.csv",
    "NIFTY IT": "ind_niftyitlist.csv",
    "NIFTY MEDIA": "ind_niftymedialist.csv",
    "NIFTY METAL": "ind_niftymetallist.csv",
    "NIFTY PHARMA": "ind_niftypharmalist.csv",
    "NIFTY PRIVATE BANK": "ind_nifty_privatebanklist.csv", # Try underscore?
    "NIFTY PSU BANK": "ind_niftypsubanklist.csv",
    "NIFTY REALTY": "ind_niftyrealtylist.csv",
    "NIFTY CONSUMER DURABLES": "ind_niftyconsumerdurableslist.csv",
    "NIFTY OIL AND GAS": "ind_niftyoilgaslist.csv",
    "NIFTY CHEMICALS": "ind_niftychemicalslist.csv" # Suspicious
}

# Alternatives to try if first fails
alternatives = {
    "NIFTY PRIVATE BANK": ["ind_niftyvtmbanklist.csv", "ind_niftyprivatebanklist.csv"],
    "NIFTY CONSUMER DURABLES": ["ind_niftyconsumerdurableslist.csv", "ind_nifty_consumerdurableslist.csv"],
    "NIFTY FINANCIAL SERVICES": ["ind_niftyfinancelist.csv", "ind_niftyfinancialserviceslist.csv"],
    "NIFTY OIL AND GAS": ["ind_niftyoilgaslist.csv", "ind_nifty_oilgaslist.csv"],
    "NIFTY CHEMICALS": ["ind_niftychemicalslist.csv", "ind_nifty_chemicalslist.csv"]
}

base_url = "https://nsearchives.nseindia.com/content/indices/"

print("Checking URLs...")
headers = {'User-Agent': 'Mozilla/5.0'}

for name, filename in sectors.items():
    found = False
    candidates = [filename] + alternatives.get(name, [])
    
    for fname in candidates:
        url = base_url + fname
        try:
            r = requests.head(url, headers=headers, timeout=5)
            if r.status_code == 200:
                print(f"[OK] {name}: {fname}")
                found = True
                break
        except:
            pass
            
    if not found:
        print(f"[FAIL] {name}")

