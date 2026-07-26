"""
generate_sample_data.py
------------------------
Simulates the three source systems shown in the architecture diagram:

  1. POS Sales (JSON)   -> data_sources/pos_sales/*.json   (dropped as micro-batches,
                            mimicking a real POS system streaming files continuously)
  2. Warehouse CSV       -> data_sources/warehouse/warehouse.csv (stock snapshot)
  3. Supplier API        -> data_sources/supplier/supplier_updates.json
                            (mocked "API response" saved to disk, since PySpark
                            Structured Streaming reads from a directory, not a live socket)

Run this BEFORE the pipeline, and re-run it (or just call generate_pos_batch())
any time you want to simulate a new wave of POS transactions landing in the
"stream" folder for the pipeline to pick up.
"""

import json
import random
import csv
from datetime import datetime, timedelta
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
POS_DIR = BASE / "data_sources" / "pos_sales"
WAREHOUSE_DIR = BASE / "data_sources" / "warehouse"
SUPPLIER_DIR = BASE / "data_sources" / "supplier"

PRODUCTS = [
    {"product_id": "P001", "name": "Running Shoe - Classic", "price": 2499.0, "store_id": "S01"},
    {"product_id": "P002", "name": "Sneaker - UrbanFlex", "price": 3299.0, "store_id": "S01"},
    {"product_id": "P003", "name": "Sandal - CoolStep", "price": 999.0, "store_id": "S02"},
    {"product_id": "P004", "name": "Formal Shoe - Oxford", "price": 4199.0, "store_id": "S02"},
    {"product_id": "P005", "name": "Boot - TrailMaster", "price": 5499.0, "store_id": "S03"},
    {"product_id": "P006", "name": "Sneaker - AirLite", "price": 2799.0, "store_id": "S03"},
    {"product_id": "P007", "name": "Sandal - BeachWalk", "price": 799.0, "store_id": "S01"},
    {"product_id": "P008", "name": "Sneaker - SprintPro", "price": 3599.0, "store_id": "S02"},
]

SUPPLIERS = ["SUP-A", "SUP-B", "SUP-C"]


def generate_pos_batch(batch_id: int, n_records: int = 40, inject_dirty_data: bool = True):
    """Writes one JSON-lines file representing a micro-batch of POS transactions."""
    POS_DIR.mkdir(parents=True, exist_ok=True)
    now = datetime.now()
    records = []
    for i in range(n_records):
        product = random.choice(PRODUCTS)
        qty = random.randint(1, 6)
        record = {
            "txn_id": f"TXN-{batch_id:04d}-{i:04d}",
            "product_id": product["product_id"],
            "product_name": product["name"],
            "store_id": product["store_id"],
            "quantity_sold": qty,
            "unit_price": product["price"],
            "txn_timestamp": (now - timedelta(seconds=random.randint(0, 600))).isoformat(),
        }
        records.append(record)

    if inject_dirty_data:
        # Null quantity (should be dropped in cleaning step)
        records.append({
            "txn_id": f"TXN-{batch_id:04d}-DIRTY1",
            "product_id": "P002",
            "product_name": "Sneaker - UrbanFlex",
            "store_id": "S01",
            "quantity_sold": None,
            "unit_price": 3299.0,
            "txn_timestamp": now.isoformat(),
        })
        # Duplicate record (should be de-duplicated)
        records.append(records[0])
        # Negative / invalid quantity (should be filtered as invalid)
        records.append({
            "txn_id": f"TXN-{batch_id:04d}-DIRTY2",
            "product_id": "P005",
            "product_name": "Boot - TrailMaster",
            "store_id": "S03",
            "quantity_sold": -3,
            "unit_price": 5499.0,
            "txn_timestamp": now.isoformat(),
        })

    out_file = POS_DIR / f"pos_batch_{batch_id:04d}.json"
    with open(out_file, "w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")
    print(f"[POS]      wrote {len(records)} records -> {out_file.name}")


def generate_warehouse_snapshot():
    """Writes the warehouse stock-level CSV (current on-hand quantity per product)."""
    WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)
    out_file = WAREHOUSE_DIR / "warehouse.csv"
    with open(out_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["product_id", "warehouse_stock", "reorder_threshold"])
        for p in PRODUCTS:
            stock = random.randint(5, 150)
            writer.writerow([p["product_id"], stock, 20])
        # Inject a dirty row (missing stock value) to prove the cleaning step works
        writer.writerow(["P099", "", 20])
    print(f"[WAREHOUSE] wrote stock snapshot -> {out_file.name}")


def generate_supplier_updates():
    """Mocks a Supplier delivery-tracking API response, saved as JSON for the pipeline to read."""
    SUPPLIER_DIR.mkdir(parents=True, exist_ok=True)
    out_file = SUPPLIER_DIR / "supplier_updates.json"
    updates = []
    for p in PRODUCTS:
        expected_days = random.randint(-3, 6)  # negative == delivered early
        expected_date = datetime.now() - timedelta(days=random.randint(1, 10))
        actual_date = expected_date + timedelta(days=random.randint(0, 8))
        updates.append({
            "po_id": f"PO-{p['product_id']}-{random.randint(1000,9999)}",
            "product_id": p["product_id"],
            "supplier_id": random.choice(SUPPLIERS),
            "expected_delivery_date": expected_date.date().isoformat(),
            "actual_delivery_date": actual_date.date().isoformat(),
        })
    with open(out_file, "w") as f:
        json.dump(updates, f, indent=2)
    print(f"[SUPPLIER] wrote {len(updates)} delivery updates -> {out_file.name}")


if __name__ == "__main__":
    generate_warehouse_snapshot()
    generate_supplier_updates()
    # Simulate three POS micro-batches landing over time (the streaming source)
    for b in range(1, 4):
        generate_pos_batch(b)
    print("\nSample data generated. Run: python src/pipeline.py")
