#!/usr/bin/env bash

# run with: bash scripts/generate_employees_1gb.sh

set -euo pipefail

python3 scripts/generate_employees_csv.py --out employees_1gb.csv --bytes $((1024*1024*1024 - 1024*1024))
