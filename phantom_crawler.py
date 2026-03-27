#!/usr/bin/env python3

import os
import argparse
import subprocess
from pathlib import Path
import requests
import getpass
import sys

# =========================
# 0. Arguments and Path Setup
# =========================
parser = argparse.ArgumentParser(description="LORIS → DataLad/Git-annex (STABLE BATCH MODE)")
parser.add_argument("--dataset", required=True, help="Path to the local DataLad dataset directory")
parser.add_argument("--api-base", required=True, help="LORIS API base URL")
parser.add_argument("--get", action="store_true", help="Download actual files after registering URLs")
args = parser.parse_args()

DATASET_DIR = Path(args.dataset).expanduser().resolve()
API_BASE = args.api_base.rstrip("/")

# =========================
# 1. Authentication (Login)
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
# 2. DataLad Dataset Initialization
# =========================
if not (DATASET_DIR / ".datalad").exists():
    print(f"Creating DataLad dataset at {DATASET_DIR}...")
    subprocess.run(["datalad", "create", str(DATASET_DIR)], check=True)
    
    print("Ensuring default branch is 'main'...")
    subprocess.run(["git", "branch", "-M", "main"], cwd=DATASET_DIR, check=True)

print("Setting annex security and UNLOCKED configs...")
subprocess.run(
    ["git", "config", "annex.security.allowed-http-addresses", "all"],
    cwd=DATASET_DIR,
    check=True
)

subprocess.run(
    ["git", "config", "annex.addunlocked", "true"],
    cwd=DATASET_DIR,
    check=True
)

# =========================
# 3. BIDS Pathing with Physical Name Integrity
# =========================
def bids_path(img):
    subj = f"sub-{img['Candidate']}"
    ses = f"ses-{img['Visit']}"
    scan = img["ScanType"].lower()

    # Extract original filename from LORIS API 'Link' field to prevent naming collisions
    orig_filename = img["Link"].split('/')[-1]

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
    
    # Concatenate BIDS prefix with original filename to guarantee unique file paths
    name = f"{subj}_{ses}_{suffix}_{orig_filename}" 
    return path / name

# =========================
# 4 & 5. Stream Pipe: API Crawl Direct to Git-annex (No local CSV file)
# =========================
print("\nScanning LORIS API and ingesting directly into Git-annex via Stream Pipe...")

projects = requests.get(f"{API_BASE}/projects", headers=HEADERS).json()["Projects"]

# start git-annex addurl 
process = subprocess.Popen(
    ["git", "annex", "addurl", "--batch", "--with-files", "--fast", "--relaxed"],
    cwd=DATASET_DIR,
    stdin=subprocess.PIPE,
    text=True
)

new_entries = 0

for project in projects:
    images = requests.get(f"{API_BASE}/projects/{project}/images", headers=HEADERS).json()["Images"]

    for img in images:
        url = API_BASE + img["Link"]
        url = url.replace("/candidates/", "/opencandidates/")
        
        target = bids_path(img)
        
        # write git-annex 
        line = f"{url} {target}\n"
        process.stdin.write(line)
        new_entries += 1

process.stdin.close()
process.wait()

print(f"Stream ingestion complete! Registered {new_entries} URLs to Git-annex.")

# =========================
# 6. File Acquisition (Anti-Duplicate Version)
# =========================
if args.get:
    print("\n[Anti-Duplicate] Checking local physical files before downloading...")
    subprocess.run(["git", "annex", "fsck", "--fast"], cwd=DATASET_DIR)

    print("\nDownloading MISSING image files via DataLad...")
    subprocess.run(["datalad", "get", "."], cwd=DATASET_DIR)

# =========================
# 7. Save & Sync
# =========================
print("\nSaving dataset changes to Git/DataLad...")

subprocess.run(
    ["datalad", "save", "-m", "Add public LORIS data via crawler directly in-memory"],
    cwd=DATASET_DIR,
    check=True
)

print("\nSyncing git-annex branch...")
subprocess.run(
    ["git", "annex", "sync", "--no-push"], 
    cwd=DATASET_DIR,
    check=True
)

print("\n" + "="*60)
print("  ALL DONE!")
print("="*60)

print("\n  [How to Push to a NEW GitHub Repository]")
print("-" * 60)
print(" 1. Go to GitHub and create a NEW EMPTY repository (Do NOT init with README/license).")
print(" 2. Copy your repository URL (e.g., https://github.com/yourname/your-repo.git).")
print(" 3. Run the following commands manually in your terminal:\n")

print(f"    cd {DATASET_DIR}")
print("    git remote add origin https://github.com/YOUR_USERNAME/YOUR_REPO_NAME.git")
print("    git push -u origin main")
print("    datalad push --to origin")
print("-" * 60)
