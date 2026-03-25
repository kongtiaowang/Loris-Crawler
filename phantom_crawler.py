#!/usr/bin/env python3

import os
import csv
import argparse
import subprocess
from pathlib import Path
import requests
import getpass
import sys

# =========================
# 0. (Args)  use: python3 phantom_crawler.py --dataset ./dataset --api-base https://phantom.loris.ca/api/v0.0.3
# =========================
parser = argparse.ArgumentParser(description="LORIS → DataLad/Git-annex (STABLE BATCH MODE)")
parser.add_argument("--dataset", required=True)
parser.add_argument("--api-base", required=True)
parser.add_argument("--get", action="store_true", help="Download actual files after registering URLs")
args = parser.parse_args()

DATASET_DIR = Path(args.dataset).expanduser().resolve()
API_BASE = args.api_base.rstrip("/")
MANIFEST = DATASET_DIR / "images_manifest.csv"

# =========================
# 1.  (Login)
# =========================
USERNAME = os.environ.get("LORIS_USERNAME") or input("Username: ")
PASSWORD = os.environ.get("LORIS_PASSWORD") or getpass.getpass("Password: ")

print("Logging in to LORIS...")
resp = requests.post(f"{API_BASE}/login", json={"username": USERNAME, "password": PASSWORD})

if resp.status_code != 200:
    print("Login failed:", resp.text)
    sys.exit(1)

TOKEN = resp.json().get("token")
HEADERS = {"Authorization": f"Bearer {TOKEN}"}
print("Login OK")

# =========================
# 2. DataLad  (Init) - 修复分支名称
# =========================
if not (DATASET_DIR / ".datalad").exists():
    print(f"Creating DataLad dataset at {DATASET_DIR}...")
    subprocess.run(["datalad", "create", "-c", "text2git", str(DATASET_DIR)], check=True)
    
    # 强制将本地默认分支重命名为 main，防止旧版本 Git 默认使用 master
    print("Ensuring default branch is 'main'...")
    subprocess.run(["git", "branch", "-M", "main"], cwd=DATASET_DIR, check=True)

print("Setting annex security configs...")
subprocess.run(
    ["git", "config", "annex.security.allowed-http-addresses", "all"],
    cwd=DATASET_DIR,
    check=True
)


# =========================
# 3. BIDS
# =========================
def bids_path(img):
    subj = f"sub-{img['Candidate']}"
    ses = f"ses-{img['Visit']}"
    scan = img["ScanType"].lower()

    if scan.startswith("t1"):
        modality, suffix = "anat", "T1w"
    elif scan.startswith("t2"):
        modality, suffix = "anat", "T2w"
    elif scan.startswith("fieldmap"):
        modality, suffix = "fmap", "epi"
    elif scan.startswith("dwi"):
        modality, suffix = "dwi", "dwi"
    else:
        modality, suffix = "misc", scan

    path = Path("data") / "loris" / subj / ses / modality
    name = f"{subj}_{ses}_{suffix}.mnc"
    return path / name

# =========================
# 4.  API & Manifest CSV
# =========================
projects = requests.get(f"{API_BASE}/projects", headers=HEADERS).json()["Projects"]

print("\nScanning LORIS API and generating manifest...")
new_entries = 0

with open(MANIFEST, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=["url", "target_path"])
    writer.writeheader()

    for project in projects:
        images = requests.get(f"{API_BASE}/projects/{project}/images", headers=HEADERS).json()["Images"]

        for img in images:
            url = API_BASE + img["Link"]
            url = url.replace("/candidates/", "/opencandidates/")
            
            target = bids_path(img)
            
            writer.writerow({
                "url": url,
                "target_path": str(target)
            })
            new_entries += 1

print(f"Generated manifest with {new_entries} images at {MANIFEST}")

# =========================
# 5. Git-annex  CSV
# =========================
print("\nIngesting URLs into Git-annex via Stream Pipe...")

process = subprocess.Popen(
    ["git", "annex", "addurl", "--batch", "--with-files", "--fast", "--relaxed"],
    cwd=DATASET_DIR,
    stdin=subprocess.PIPE,
    text=True
)

with open(MANIFEST, 'r') as csv_file:
    reader = csv.DictReader(csv_file)
    for row in reader:
        line = f"{row['url']} {row['target_path']}\n"
        process.stdin.write(line)

process.stdin.close()
process.wait()

print("Ingestion complete via git-annex batch!")

# =========================
# 6. get files
# =========================
if args.get:
    print("\nDownloading actual image files via DataLad...")
    subprocess.run(["datalad", "get", "."], cwd=DATASET_DIR)

# =========================
# 7. (Save & Sync) - 修复同步和 GitHub 引导
# =========================
print("\nSaving dataset changes to Git/DataLad...")
subprocess.run(
    ["datalad", "save", "-m", "Add public LORIS data via crawler batch stream"],
    cwd=DATASET_DIR,
    check=True
)

print("\nSyncing git-annex branch...")
subprocess.run(
    ["git", "annex", "sync", "--no-push"], # 防止它在没配 origin 的时候报错
    cwd=DATASET_DIR,
    check=True
)

print("\n✅ ALL DONE!")
print("--------------------------------------------------")
print("To push to GitHub, run these commands manually:")
print(f"  cd {DATASET_DIR}")
print("  git remote add origin https://github.com/yourname/yourrepo.git")
print("  git push -u origin main") # 👈 关键点：这一步显式把 main 设置为默认分支
print("  datalad push --to origin") # 👈 推送大文件元数据和 annex 分支
print("--------------------------------------------------")
