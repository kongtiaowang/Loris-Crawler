# Loris-Crawler

A Python tool to ingest MRI images from the **LORIS / PHANTOM API** into a
**DataLad + git-annex dataset** using a **BIDS-like directory structure**.

Designed for research workflows:
- Multi-project ingestion
- Incremental (safe to re-run)
- git-annex–managed large files

---

## 📦 Installation

### 1️⃣ System dependencies

#### Git
```bash
git --version

nstall:

macOS

brew install git


Ubuntu / Debian

sudo apt install git


git-annex (required)
git annex version


Install:

macOS

brew install git-annex


Ubuntu / Debian

sudo apt install git-annex

DataLad
pip install datalad


Optional but recommended for neuroimaging:

pip install datalad-neuroimaging


Verify:

datalad --version

2️⃣ Python requirements

Python 3.8+ recommended

pip installed

Check:

python3 --version
pip3 --version

3️⃣ Python dependencies

Create a virtual environment (recommended):

python3 -m venv venv
source venv/bin/activate


Install dependencies:

pip install requests


(Optional requirements.txt)

requests
datalad
datalad-neuroimaging

🔐 Authentication

The script authenticates using your LORIS username and password to obtain
a Bearer token from the API.

You can set credentials via environment variables:

export LORIS_USERNAME="your_username"
export LORIS_PASSWORD="your_password"


If not set, the script will prompt interactively.

🚀 Usage
Metadata ingest only (no file downloads)
python3 loris-crawler.py \
  --dataset ./dataset \
  --api-base https://phantom.loris.ca/api/v0.0.3


This registers files in git-annex using URLs but does not download them.

Ingest and download files immediately
python3 loris-crawler.py \
  --dataset ./dataset \
  --api-base https://phantom.loris.ca/api/v0.0.3 \
  --get

📁 Output Structure (BIDS-like)
dataset/
├── images_manifest.csv
├── PROJECT/
│   └── sub-963271/
│       └── ses-MNI_SCAN1_20101125/
│           └── anat/
│               └── sub-963271_ses-MNI_SCAN1_20101125_T1w.mnc


Each project is namespaced at the top level

Large files are managed by git-annex

images_manifest.csv tracks ingested files to prevent duplicates

⬇️ Download files later

Download a single file:

cd dataset
datalad get PROJECT/sub-963271/ses-MNI_SCAN1_20101125/anat/sub-963271_ses-MNI_SCAN1_20101125_T1w.mnc


Download everything:

datalad get .
