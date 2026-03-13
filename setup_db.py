"""
One-off script: create database schema and load all existing data.
Run: python setup_db.py
"""
import logging
from db.schema import init_db
from db.load_data import load_all

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

if __name__ == "__main__":
    print("Initializing database schema...")
    init_db()
    print("Schema created.\n")

    print("Loading data...")
    total = load_all()
    print(f"\nDone — {total} papers loaded into corpus.")
