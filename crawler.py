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
parser.add_argument(
    "--ria",
    metavar="URL",
    help="Push downloaded content to a public RIA store (e.g. ssh://user@host/srv/ria or file:///path). Use together with --get.",
)
parser.add_argument(
    "--limit",
    type=int,
    default=0,
    help="Only ingest the first N images (0 = all). Useful for testing.",
)
args = parser.parse_args()

DATASET_DIR = Path(args.dataset).expanduser().resolve()
API_BASE = args.api_base.rstrip("/")

# Auth helper committed into the dataset so every clone can prompt the
# RUNNING USER for their own LORIS username/password (annex.http-headers-command).
SCRIPT_TEMPLATE = """#!/bin/sh
# git-annex web-download auth helper for LORIS API datasets.
# Prints: Authorization: Bearer <token>
# Uses the RUNNING USER's own LORIS account:
#   LORIS_USERNAME / LORIS_PASSWORD env vars, or interactive prompt on /dev/tty.
# NOTE: git-annex runs this command with piped stdio, so interactive input
# must read from /dev/tty, otherwise it blocks forever.
# Token cached in /tmp (1h, keyed by username) to avoid one login per file.
API_BASE="__API_BASE__"

USERNAME="${LORIS_USERNAME:-}"
if [ -z "$USERNAME" ]; then
    printf 'LORIS username: ' >/dev/tty 2>/dev/null || printf 'LORIS username: ' >&2
    if [ -e /dev/tty ]; then read -r USERNAME < /dev/tty; else read -r USERNAME; fi
fi

CACHE="/tmp/loris-token-$(id -u)-$(printf %s "$USERNAME" | cksum | cut -d' ' -f1)"
if [ -f "$CACHE" ] && find "$CACHE" -mmin -60 2>/dev/null | grep -q .; then
    cat "$CACHE"
    exit 0
fi

PASSWORD="${LORIS_PASSWORD:-}"
if [ -z "$PASSWORD" ]; then
    printf 'LORIS password: ' >/dev/tty 2>/dev/null || printf 'LORIS password: ' >&2
    if [ -e /dev/tty ]; then
        stty -echo < /dev/tty 2>/dev/null
        read -r PASSWORD < /dev/tty
        stty echo < /dev/tty 2>/dev/null
        printf '\\n' >/dev/tty 2>/dev/null
    else
        read -r PASSWORD
    fi
fi

# Build the JSON with Python so special characters in the password (quotes,
# backslashes, unicode) can never break the request.
TOKEN=$(LORIS_API_BASE="$API_BASE" LORIS_USERNAME="$USERNAME" LORIS_PASSWORD="$PASSWORD" python3 -c '
import json, os, sys, urllib.request, urllib.error
req = urllib.request.Request(
    os.environ["LORIS_API_BASE"] + "/login",
    data=json.dumps({"username": os.environ["LORIS_USERNAME"],
                     "password": os.environ["LORIS_PASSWORD"]}).encode(),
    headers={"Content-Type": "application/json"})
try:
    resp = urllib.request.urlopen(req, timeout=30)
    print(json.load(resp).get("token") or "")
except urllib.error.HTTPError as e:
    sys.stderr.write("loris-auth.sh: login HTTP %d %s (check username/password)\\n" % (e.code, e.reason))
except Exception as e:
    sys.stderr.write("loris-auth.sh: login error: %s\\n" % e)
')
if [ -z "$TOKEN" ]; then
    exit 1
fi
printf 'Authorization: Bearer %s\\n' "$TOKEN" | tee "$CACHE"
"""

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
    subprocess.run(["datalad", "create", "--force", str(DATASET_DIR)], check=True)

    print("Ensuring default branch is 'main'...")
    subprocess.run(["git", "branch", "-M", "main"], cwd=DATASET_DIR, check=True)

print("Setting annex security and UNLOCKED configs...")
subprocess.run(
    ["git", "config", "annex.security.allowed-http-addresses", "all"],
    cwd=DATASET_DIR,
    check=True
)

# git-annex needs the same bearer token to download the registered URLs later.
# NOTE: this lands in .git/config (local only, not pushed) and the token
# expires - re-run the crawler (or set the header again) when downloads
# start failing with 401.
subprocess.run(
    ["git", "config", "annex.http-headers", f"Authorization: Bearer {TOKEN}"],
    cwd=DATASET_DIR,
    check=True,
)

subprocess.run(
    ["git", "config", "annex.addunlocked", "true"],
    cwd=DATASET_DIR,
    check=True
)

# =========================
# 3. BIDS Pathing with Physical Name Integrity
# =========================
def bids_path(img, project):
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

    # Use the real LORIS project name as namespace (e.g. data/PREVENT-AD/...),
    # so multiple projects never mix in one dataset.
    path = Path("data") / project / subj / ses / modality
    
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
        if args.limit and new_entries >= args.limit:
            break

        url = API_BASE + img["Link"]
        
        target = bids_path(img, project)

        # write git-annex
        line = f"{url} {target}\n"
        process.stdin.write(line)
        new_entries += 1

process.stdin.close()
process.wait()

print(f"Stream ingestion complete! Registered {new_entries} URLs to Git-annex.")
if args.limit:
    print(f"(Limited to --limit {args.limit}; re-run without --limit to ingest everything)")

# =========================
# 6. File Acquisition (Anti-Duplicate Version)
# =========================
if args.get:
    print("\n[Anti-Duplicate] Checking local physical files before downloading...")

    # Force git-annex to scan workspace to sync local untracked physical files to the ledger.
    subprocess.run(["git", "annex", "fsck", "--fast"], cwd=DATASET_DIR)

    print("\nDownloading MISSING image files via DataLad...")
    subprocess.run(["datalad", "get", "."], cwd=DATASET_DIR)

# =========================
# 6.5 Auth helper (committed so every clone can use its own account)
# =========================
# IMPORTANT: this script must be a REGULAR file in git, not an annexed
# symlink - otherwise clones get a broken pointer and git-annex can't run it.
gitattrs = DATASET_DIR / ".gitattributes"
existing_attrs = gitattrs.read_text() if gitattrs.exists() else ""
if "tools/* annex.largefiles=nothing" not in existing_attrs:
    gitattrs.write_text(existing_attrs.rstrip() + "\ntools/* annex.largefiles=nothing\n")

tools_dir = DATASET_DIR / "tools"
tools_dir.mkdir(exist_ok=True)
auth_script = tools_dir / "loris-auth.sh"
if auth_script.exists():
    # If a previous run annexed the script, convert it back to a plain git file
    # (unannex is a no-op error for plain files; check=False tolerates that).
    subprocess.run(["git", "annex", "unannex", str(auth_script)], cwd=DATASET_DIR, check=False)
auth_script.write_text(SCRIPT_TEMPLATE.replace("__API_BASE__", API_BASE))
auth_script.chmod(0o755)
print(f"Wrote auth helper: {auth_script} (committed with the dataset as a regular file)")

# =========================
# 7. Save & Sync
# =========================
print("\nSaving dataset changes to Git/DataLad...")

subprocess.run(
    ["datalad", "save", "-m", "Add public LORIS data via crawler"],
    cwd=DATASET_DIR,
    check=True
)

print("\nSyncing git-annex branch...")
# --no-pull: never fetch/merge remotes (a dataset dir may already have an
# origin with unrelated history, which made the old sync crash).
# check=False: sync is only local bookkeeping; datalad save already committed.
subprocess.run(
    ["git", "annex", "sync", "--no-push", "--no-pull"],
    cwd=DATASET_DIR,
    check=False,
)

# =========================
# 8. Optional: push content to a public RIA store
# =========================
if args.ria:
    print(f"\nCreating RIA sibling at: {args.ria}")
    subprocess.run(
        ["datalad", "create-sibling-ria", "-s", "public", args.ria],
        cwd=DATASET_DIR,
    )
    print("Pushing content to public sibling (uploads all downloaded files)...")
    subprocess.run(
        ["datalad", "push", "--to", "public"],
        cwd=DATASET_DIR,
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

print("\n  [How OTHERS download with their own LORIS account]")
print("-" * 60)
print("  After they clone the repo, one config line, then datalad get")
print("  will ask for THEIR username/password:")
print()
print("    cd <clone-dir>")
print("    git config annex.http-headers-command \"$(pwd)/tools/loris-auth.sh\"")
print("    datalad get .   # prompts: LORIS username / LORIS password")
print("-" * 60)


