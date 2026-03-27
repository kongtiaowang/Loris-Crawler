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
# 0. Arguments and Path Setup
# =========================
parser = argparse.ArgumentParser(description="LORIS → DataLad/Git-annex (STABLE BATCH MODE)")
parser.add_argument("--dataset", required=True, help="Path to the local DataLad dataset directory")
parser.add_argument("--api-base", required=True, help="LORIS API base URL")
parser.add_argument("--get", action="store_true", help="Download actual files after registering URLs")
args = parser.parse_args()

DATASET_DIR = Path(args.dataset).expanduser().resolve()
API_BASE = args.api_base.rstrip("/")
MANIFEST = DATASET_DIR / "images_manifest.csv"

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
# 4. API Crawl & Manifest Generation
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
# 5. Git-annex Batch Ingestion via Stream Pipe
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
# 6. File Acquisition (Anti-Duplicate Version)
# =========================
if args.get:
    print("\n[Anti-Duplicate] Checking local physical files before downloading...")
    
    # Force git-annex to scan workspace to sync local untracked physical files to the ledger.
    # This prevents git-annex from re-downloading existing files from the cloud.
    subprocess.run(["git", "annex", "fsck", "--fast"], cwd=DATASET_DIR)

    print("\nDownloading MISSING image files via DataLad...")
    # Only get missing files, never re-download existing ones
    subprocess.run(["datalad", "get", "."], cwd=DATASET_DIR)

# =========================
# 7. Save & Sync
# =========================
print("\nSaving dataset changes to Git/DataLad...")

# Instruct datalad save to keep files unlocked (as real files, not symlinks)
subprocess.run(
    ["datalad", "save", "-m", "Add public LORIS data via crawler", "--unlocked"],
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

