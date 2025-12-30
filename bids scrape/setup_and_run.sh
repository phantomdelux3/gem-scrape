#!/bin/bash

set -e

echo "=========================================="
echo "   Gem Bids Scraper Setup & Run (Linux)"
echo "=========================================="
echo

# ---------- STEP 1: Install system dependencies ----------
echo "[INFO] Checking system dependencies..."

sudo apt update -y
sudo apt install -y python3 python3-pip python3-venv

echo "[INFO] Python version:"
python3 --version

# ---------- STEP 2: Create virtual environment ----------
if [ ! -d "venv" ]; then
  echo "[INFO] Creating virtual environment..."
  python3 -m venv venv
fi

echo "[INFO] Activating virtual environment..."
source venv/bin/activate

# ---------- STEP 3: Install Python dependencies ----------
echo "[INFO] Installing Python requirements..."
pip install --upgrade pip
pip install -r requirements.txt

# ---------- STEP 4: Create .env if missing ----------
if [ ! -f ".env" ]; then
  echo
  echo "[WARN] .env file not found! Let's configure your Database."
  echo

  read -p "Enter Database Host (default: localhost): " DB_HOST
  DB_HOST=${DB_HOST:-localhost}

  read -p "Enter Database Port (default: 5432): " DB_PORT
  DB_PORT=${DB_PORT:-5432}

  read -p "Enter Database Name (default: bids_db): " DB_NAME
  DB_NAME=${DB_NAME:-bids_db}

  read -p "Enter Database User (default: postgres): " DB_USER
  DB_USER=${DB_USER:-postgres}

  read -s -p "Enter Database Password: " DB_PASSWORD
  echo

  echo "[INFO] Creating .env file..."

  cat <<EOF > .env
DB_HOST=$DB_HOST
DB_PORT=$DB_PORT
DB_NAME=$DB_NAME
DB_USER=$DB_USER
DB_PASSWORD=$DB_PASSWORD
EOF

  echo "[INFO] .env file created successfully."
fi

# ---------- STEP 5: Run scraper ----------
echo
echo "[INFO] Starting Scraper in TEST mode (10 pages)..."
echo "[INFO] Ensure PostgreSQL is running and .env is configured."
echo

python scrape_bids.py -t

echo
echo "[INFO] Done."
