
import os
import requests
import threading
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# —————————————————————————————————————————
# Configuration
API            = 'https://appeears.earthdatacloud.nasa.gov/api/'
USER           = 'toddlerjacker'
PASSWORD       = 'Drago423031#'
REQUEST_IDS    = [
    '2ded3a5f-78e1-4973-907a-e64f14e2b43e',
    '16cf47d6-4683-4426-8ad0-6d6ed0a35116',
    
]
DEST_ROOT      = os.path.join(os.getcwd(), 'year')  # parent folder for all bundles
MAX_WORKERS    = 10
REFRESH_MARGIN = 300  # secs before token expiry to auto-refresh
# —————————————————————————————————————————

os.makedirs(DEST_ROOT, exist_ok=True)

class TokenManager:
    def __init__(self, api, user, password, refresh_margin=300):
        self.api = api
        self.user = user
        self.password = password
        self.refresh_margin = refresh_margin
        self.lock = threading.Lock()
        self._refresh()

    def _refresh(self):
        resp = requests.post(f'{self.api}login', auth=(self.user, self.password))
        resp.raise_for_status()
        tok = resp.json()
        self.token = tok['token']
        self.expires = datetime.fromisoformat(tok['expiration'].rstrip('Z'))
        self.headers = {'Authorization': f'Bearer {self.token}'}

    def get_headers(self):
        with self.lock:
            if datetime.utcnow() + timedelta(seconds=self.refresh_margin) > self.expires:
                self._refresh()
            return self.headers

# Initialize manager once
mgr = TokenManager(API, USER, PASSWORD, REFRESH_MARGIN)

# Download logic per bundle

def download_bundle(request_id):
    dest_dir = os.path.join(DEST_ROOT, request_id)
    os.makedirs(dest_dir, exist_ok=True)

    # Fetch bundle metadata
    bundle_url = f'{API}bundle/{request_id}'
    bundle = requests.get(bundle_url, headers=mgr.get_headers())
    bundle.raise_for_status()
    files = {f['file_id']: f['file_name'] for f in bundle.json().get('files', [])}
    print(f"Bundle {request_id}: found {len(files)} files")

    # Resume logic
    downloaded = set(fn for fn in os.listdir(dest_dir) if fn.lower().endswith('.tif'))

    def download_one(file_id, file_name):
        if not file_name.lower().endswith('.tif'):
            return
        name = file_name.split('/',1)[-1]
        if name in downloaded:
            return
        url = f'{API}bundle/{request_id}/{file_id}/{name}'
        for attempt in range(2):
            resp = requests.get(url, headers=mgr.get_headers(), stream=True)
            if resp.status_code == 401 and attempt == 0:
                continue
            resp.raise_for_status()
            outpath = os.path.join(dest_dir, name)
            with open(outpath, 'wb') as fp:
                for chunk in resp.iter_content(8192):
                    fp.write(chunk)
            print(f"✔ {request_id} / {name}")
            return
        print(f"⚠ Failed to download {name} in {request_id}")

    # Parallel execution
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as exe:
        futures = {exe.submit(download_one, fid, fname): fid for fid, fname in files.items()}
        for fut in as_completed(futures):
            fid = futures[fut]
            try:
                fut.result()
            except Exception as e:
                print(f"⚠ Error in bundle {request_id}, file {fid}: {e}")

# Orchestrate all bundles
for rid in REQUEST_IDS:
    try:
        download_bundle(rid)
    except Exception as e:
        print(f"Failed bundle {rid}: {e}")  

print("All bundles processed under:", DEST_ROOT)
