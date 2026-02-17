#!/usr/bin/env bash
set -euo pipefail

# YATB - Yet Another Backup Tool

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

if [[ ! -d .venv ]]; then
  python3 -m venv .venv
fi

source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

echo "Starting YATB..."
python app.py
