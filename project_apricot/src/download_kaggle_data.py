"""
download_kaggle_data.py
------------------------
Kaggle's own API requires an authenticated request (kaggle.json credentials)
and this project's network access doesn't reach kaggle.com, so this script
pulls the same, widely-mirrored "Superstore Sales" retail dataset from a
public GitHub mirror instead — it's the exact dataset behind several popular
Kaggle notebooks (search "Superstore Sales Dataset" on Kaggle and you'll
recognize the columns immediately).

It then reshapes those REAL transactions into the three source feeds the
architecture diagram calls for:

  1. POS Sales (JSON)   <- real order-line transactions from the dataset,
                            split into micro-batch files to simulate a stream
  2. Warehouse CSV       <- stock levels, generated per REAL product found in
                            the dataset (the source dataset has no inventory
                            data, so this part is necessarily simulated)
  3. Supplier API        <- delivery updates, generated per REAL product
                            (also simulated — no supplier feed exists in the
                            source data)

If you'd rather use the authentic Kaggle CSV directly: download it yourself
from Kaggle, drop it in data_sources/raw/, and point RAW_CSV_PATH below at it
instead of the GitHub URL — the reshape logic works the same either way.
"""

import json
import random
from pathlib import Path

import pandas as pd
import requests

BASE = Path(__file__).resolve().parent.parent
POS_DIR = BASE / "data_sources" / "pos_sales"
WAREHOUSE_DIR = BASE / "data_sources" / "warehouse"
SUPPLIER_DIR = BASE / "data_sources" / "supplier"
RAW_DIR = BASE / "data_sources" / "raw"

SOURCE_URL = "https://raw.githubusercontent.com/curran/data/gh-pages/superstoreSales/superstoreSales.csv"
RAW_CSV_PATH = RAW_DIR / "superstoreSales.csv"

BATCH_SIZE = 700          # rows per simulated POS micro-batch file
SUPPLIERS = ["SUP-A", "SUP-B", "SUP-C"]



def download_raw_csv() -> Path:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    if RAW_CSV_PATH.exists():
        print(f"[download] already have {RAW_CSV_PATH.name}, skipping download.")
        return RAW_CSV_PATH
    print(f"[download] fetching real retail dataset from {SOURCE_URL} ...")
    resp = requests.get(SOURCE_URL, timeout=60)
    resp.raise_for_status()
    RAW_CSV_PATH.write_bytes(resp.content)
    print(f"[download] saved -> {RAW_CSV_PATH}")
    return RAW_CSV_PATH




def load_and_clean_raw() -> pd.DataFrame:
    df = pd.read_csv(RAW_CSV_PATH, encoding="latin1")
    df = df.dropna(subset=["Product Name", "Order Quantity", "Unit Price", "Order Date"])
    df = df[(df["Order Quantity"] > 0) & (df["Unit Price"] > 0)]
    df["Order Date"] = pd.to_datetime(df["Order Date"], errors="coerce")
    df = df.dropna(subset=["Order Date"])
    return df


def build_product_catalog(df: pd.DataFrame) -> pd.DataFrame:
    products = df["Product Name"].drop_duplicates().reset_index(drop=True)
    catalog = pd.DataFrame({
        "product_name": products,
        "product_id": [f"P{idx+1:04d}" for idx in range(len(products))],
    })
    return catalog


def write_pos_batches(df: pd.DataFrame, catalog: pd.DataFrame):
    POS_DIR.mkdir(parents=True, exist_ok=True)
    for f in POS_DIR.glob("*.json"):
        f.unlink()

    merged = df.merge(catalog, left_on="Product Name", right_on="product_name", how="inner")
    merged = merged.sort_values("Order Date").reset_index(drop=True)

    n_batches = 0
    for start in range(0, len(merged), BATCH_SIZE):
        chunk = merged.iloc[start:start + BATCH_SIZE]
        n_batches += 1
        out_file = POS_DIR / f"pos_batch_{n_batches:04d}.json"
        with open(out_file, "w") as f:
            for _, row in chunk.iterrows():
                record = {
                    "txn_id": f"TXN-{int(row['Row ID'])}",
                    "product_id": row["product_id"],
                    "product_name": row["Product Name"],
                    "store_id": str(row.get("Region", "UNKNOWN")),
                    "quantity_sold": int(row["Order Quantity"]),
                    "unit_price": float(row["Unit Price"]),
                    "txn_timestamp": row["Order Date"].isoformat(),
                }
                f.write(json.dumps(record) + "\n")
        print(f"[POS]      wrote {len(chunk)} real transactions -> {out_file.name}")

    print(f"[POS]      total: {len(merged)} real transactions across {n_batches} micro-batches")


def write_warehouse_snapshot(catalog: pd.DataFrame):
    """Simulated stock levels — one row per REAL product from the dataset."""
    WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)
    out_file = WAREHOUSE_DIR / "warehouse.csv"
    rows = []
    for pid in catalog["product_id"]:
        rows.append({
            "product_id": pid,
            "warehouse_stock": random.randint(5, 300),
            "reorder_threshold": random.choice([15, 20, 25, 30]),
        })
    pd.DataFrame(rows).to_csv(out_file, index=False)
    print(f"[WAREHOUSE] wrote stock snapshot for {len(rows)} real products -> {out_file.name}")


def write_supplier_updates(catalog: pd.DataFrame):
    """Simulated supplier delivery tracking — one PO per REAL product."""
    import datetime
    SUPPLIER_DIR.mkdir(parents=True, exist_ok=True)
    out_file = SUPPLIER_DIR / "supplier_updates.json"
    updates = []
    for pid in catalog["product_id"]:
        expected_date = datetime.datetime.now() - datetime.timedelta(days=random.randint(1, 10))
        actual_date = expected_date + datetime.timedelta(days=random.randint(0, 8))
        updates.append({
            "po_id": f"PO-{pid}-{random.randint(1000,9999)}",
            "product_id": pid,
            "supplier_id": random.choice(SUPPLIERS),
            "expected_delivery_date": expected_date.date().isoformat(),
            "actual_delivery_date": actual_date.date().isoformat(),
        })
    with open(out_file, "w") as f:
        json.dump(updates, f, indent=2)
    print(f"[SUPPLIER] wrote {len(updates)} delivery updates for real products -> {out_file.name}")


if __name__ == "__main__":
    download_raw_csv()
    df = load_and_clean_raw()
    print(f"[data] {len(df)} clean real transactions, {df['Product Name'].nunique()} unique products")

    catalog = build_product_catalog(df)
    write_pos_batches(df, catalog)
    write_warehouse_snapshot(catalog)
    write_supplier_updates(catalog)
     
    print("\nReal dataset prepared. Run: python src/pipeline.py")
