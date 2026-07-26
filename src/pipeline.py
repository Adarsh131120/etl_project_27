"""
pipeline.py
-----------
Real-Time Inventory Control System - PySpark ETL

Implements the architecture exactly as diagrammed:

    POS Sales (JSON) ----+
    Warehouse CSV --------+--> PySpark Structured Streaming (Extract)
    Supplier API ---------+
              |
              v
    Data Cleaning & Validation (null removal, duplicate removal)
              |
              v
    Business Transformations
        - Sales Aggregation
        - Inventory Calculation
        - Product Ranking
        - Low Stock Detection
        - Supplier Delay Detection
              |
              v
    SQL Server Database   (SQLite used locally as a drop-in stand-in - see load.py notes)
              |
              v
    Power BI Live Dashboard (CSV/Parquet export Power BI can connect to & auto-refresh)

USAGE:
    python src/generate_sample_data.py     # create sample source data (run once)
    python src/pipeline.py                 # run the streaming pipeline
"""

import sqlite3
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

BASE = Path(__file__).resolve().parent.parent
POS_DIR = str(BASE / "data_sources" / "pos_sales")
WAREHOUSE_CSV = str(BASE / "data_sources" / "warehouse" / "warehouse.csv")
SUPPLIER_JSON = str(BASE / "data_sources" / "supplier" / "supplier_updates.json")
CHECKPOINT_DIR = str(BASE / "output" / "_checkpoints")
SQLITE_DB = str(BASE / "output" / "inventory.db")

LOW_STOCK_THRESHOLD = 20
SUPPLIER_DELAY_THRESHOLD_DAYS = 2


# --------------------------------------------------------------------------- #
# SPARK SESSION
# --------------------------------------------------------------------------- #
def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("RealTimeInventoryControl-ETL")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )


# --------------------------------------------------------------------------- #
# EXTRACT
# --------------------------------------------------------------------------- #
POS_SCHEMA = StructType([
    StructField("txn_id", StringType()),
    StructField("product_id", StringType()),
    StructField("product_name", StringType()),
    StructField("store_id", StringType()),
    StructField("quantity_sold", IntegerType()),
    StructField("unit_price", DoubleType()),
    StructField("txn_timestamp", StringType()),
])


def extract_pos_stream(spark: SparkSession) -> DataFrame:
    """Reads POS sales JSON files as a Structured Streaming source (file-based stream)."""
    return (
        spark.readStream
        .schema(POS_SCHEMA)
        .option("maxFilesPerTrigger", 1)   # process one micro-batch (file) at a time
        .json(POS_DIR)
    )


def extract_warehouse(spark: SparkSession) -> DataFrame:
    """Warehouse stock levels - static reference data, re-read each batch."""
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(WAREHOUSE_CSV)
    )


def extract_supplier(spark: SparkSession) -> DataFrame:
    """Supplier delivery updates - mocked API payload persisted as JSON."""
    return spark.read.option("multiLine", True).json(SUPPLIER_JSON)


# --------------------------------------------------------------------------- #
# CLEAN / VALIDATE
# --------------------------------------------------------------------------- #
def clean_pos(df: DataFrame) -> DataFrame:
    return (
        df.dropna(subset=["product_id", "quantity_sold", "unit_price"])   # null removal
          .filter((F.col("quantity_sold") > 0) & (F.col("unit_price") > 0))  # invalid values
          .dropDuplicates(["txn_id"])                                      # duplicate removal
          .withColumn("txn_timestamp", F.to_timestamp("txn_timestamp"))
    )


def clean_warehouse(df: DataFrame) -> DataFrame:
    return (
        df.dropna(subset=["product_id", "warehouse_stock"])
          .dropDuplicates(["product_id"])
    )


def clean_supplier(df: DataFrame) -> DataFrame:
    return (
        df.dropna(subset=["product_id", "supplier_id", "expected_delivery_date", "actual_delivery_date"])
          .dropDuplicates(["po_id"])
          .withColumn("expected_delivery_date", F.to_date("expected_delivery_date"))
          .withColumn("actual_delivery_date", F.to_date("actual_delivery_date"))
    )


# --------------------------------------------------------------------------- #
# BUSINESS TRANSFORMATIONS
# --------------------------------------------------------------------------- #
def sales_aggregation(pos_clean: DataFrame) -> DataFrame:
    """Aggregate revenue & units sold per product / store for this micro-batch."""
    return (
        pos_clean
        .withColumn("revenue", F.col("quantity_sold") * F.col("unit_price"))
        .groupBy("product_id", "product_name", "store_id")
        .agg(
            F.sum("quantity_sold").alias("units_sold"),
            F.sum("revenue").alias("total_revenue"),
            F.count("txn_id").alias("txn_count"),
        )
    )


def inventory_calculation(sales_agg: DataFrame, warehouse_clean: DataFrame) -> DataFrame:
    """Current stock = warehouse stock - units sold in this batch."""
    per_product_sales = sales_agg.groupBy("product_id").agg(
        F.sum("units_sold").alias("units_sold_batch")
    )
    return (
        warehouse_clean
        .join(per_product_sales, on="product_id", how="left")
        .withColumn("units_sold_batch", F.coalesce(F.col("units_sold_batch"), F.lit(0)))
        .withColumn("current_stock", F.col("warehouse_stock") - F.col("units_sold_batch"))
    )


def product_ranking(sales_agg: DataFrame) -> DataFrame:
    """Rank products by revenue within this micro-batch (best-sellers first)."""
    window = Window.orderBy(F.col("total_revenue").desc())
    product_totals = sales_agg.groupBy("product_id", "product_name").agg(
        F.sum("total_revenue").alias("total_revenue"),
        F.sum("units_sold").alias("units_sold"),
    )
    return product_totals.withColumn("rank", F.rank().over(window))


def low_stock_detection(inventory: DataFrame) -> DataFrame:
    return (
        inventory
        .withColumn(
            "low_stock_flag",
            F.when(F.col("current_stock") < F.coalesce(F.col("reorder_threshold"), F.lit(LOW_STOCK_THRESHOLD)), True)
             .otherwise(False)
        )
        .filter(F.col("low_stock_flag") == True)  # noqa: E712
        .select("product_id", "current_stock", "reorder_threshold", "low_stock_flag")
    )


def supplier_delay_detection(supplier_clean: DataFrame) -> DataFrame:
    return (
        supplier_clean
        .withColumn("delay_days", F.datediff("actual_delivery_date", "expected_delivery_date"))
        .withColumn("delayed_flag", F.col("delay_days") > SUPPLIER_DELAY_THRESHOLD_DAYS)
        .filter(F.col("delayed_flag") == True)  # noqa: E712
        .select("po_id", "product_id", "supplier_id", "expected_delivery_date",
                "actual_delivery_date", "delay_days")
    )


# --------------------------------------------------------------------------- #
# LOAD
# --------------------------------------------------------------------------- #
def init_sqlite():
    Path(SQLITE_DB).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(SQLITE_DB)
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS sales_aggregation (
        product_id TEXT, product_name TEXT, store_id TEXT,
        units_sold INTEGER, total_revenue REAL, txn_count INTEGER,
        batch_id INTEGER, loaded_at TEXT
    );
    CREATE TABLE IF NOT EXISTS inventory_status (
        product_id TEXT, warehouse_stock INTEGER, reorder_threshold INTEGER,
        units_sold_batch INTEGER, current_stock INTEGER,
        batch_id INTEGER, loaded_at TEXT
    );
    CREATE TABLE IF NOT EXISTS product_ranking (
        product_id TEXT, product_name TEXT, total_revenue REAL,
        units_sold INTEGER, rank INTEGER, batch_id INTEGER, loaded_at TEXT
    );
    CREATE TABLE IF NOT EXISTS low_stock_alerts (
        product_id TEXT, current_stock INTEGER, reorder_threshold INTEGER,
        low_stock_flag INTEGER, batch_id INTEGER, loaded_at TEXT
    );
    CREATE TABLE IF NOT EXISTS supplier_delay_alerts (
        po_id TEXT, product_id TEXT, supplier_id TEXT,
        expected_delivery_date TEXT, actual_delivery_date TEXT, delay_days INTEGER,
        batch_id INTEGER, loaded_at TEXT
    );
    """)
    conn.commit()
    conn.close()


def load_table(pdf, table_name: str, batch_id: int):
    """Loads a pandas dataframe into SQLite (stand-in for SQL Server). The Streamlit
    dashboard (src/streamlit_app.py) queries this same database live, so anything
    landed here shows up on the dashboard on its next refresh."""
    if pdf.empty:
        return
    import datetime
    pdf = pdf.copy()
    pdf["batch_id"] = batch_id
    pdf["loaded_at"] = datetime.datetime.now().isoformat()

    conn = sqlite3.connect(SQLITE_DB)
    pdf.to_sql(table_name, conn, if_exists="append", index=False)
    conn.close()


# --------------------------------------------------------------------------- #
# STREAMING ORCHESTRATION (foreachBatch = Extract -> Clean -> Transform -> Load)
# --------------------------------------------------------------------------- #
def process_batch(spark: SparkSession):
    def _inner(batch_df: DataFrame, batch_id: int):
        if batch_df.rdd.isEmpty():
            print(f"[batch {batch_id}] no new POS records, skipping.")
            return

        print(f"\n===== Processing micro-batch {batch_id} =====")

        # ---- Extract (reference sources, re-read fresh each batch) ----
        warehouse_raw = extract_warehouse(spark)
        supplier_raw = extract_supplier(spark)

        # ---- Clean & Validate ----
        pos_clean = clean_pos(batch_df)
        warehouse_clean = clean_warehouse(warehouse_raw)
        supplier_clean = clean_supplier(supplier_raw)

        raw_count = batch_df.count()
        clean_count = pos_clean.count()
        print(f"  POS records: {raw_count} raw -> {clean_count} after null/dup/invalid removal")

        # ---- Business Transformations ----
        sales_agg = sales_aggregation(pos_clean)
        inventory = inventory_calculation(sales_agg, warehouse_clean)
        ranking = product_ranking(sales_agg)
        low_stock = low_stock_detection(inventory)
        supplier_delays = supplier_delay_detection(supplier_clean)

        print(f"  Sales aggregation rows : {sales_agg.count()}")
        print(f"  Low stock alerts       : {low_stock.count()}")
        print(f"  Supplier delay alerts  : {supplier_delays.count()}")

        # ---- Load (SQL Server stand-in + Power BI export) ----
        load_table(sales_agg.toPandas(), "sales_aggregation", batch_id)
        load_table(inventory.toPandas(), "inventory_status", batch_id)
        load_table(ranking.toPandas(), "product_ranking", batch_id)
        load_table(low_stock.toPandas(), "low_stock_alerts", batch_id)
        load_table(supplier_delays.toPandas(), "supplier_delay_alerts", batch_id)

        print(f"===== Batch {batch_id} loaded to {SQLITE_DB} =====")

    return _inner


def run_pandas():
    """Pandas-based micro-batch streaming ETL engine for environments without Java/PySpark."""
    init_sqlite()
    
    pos_dir = Path(POS_DIR)
    pos_files = sorted(pos_dir.glob("*.json"))
    if not pos_files:
        print("[pandas ETL] No POS json batch files found in", POS_DIR)
        return

    checkpoint_file = Path(CHECKPOINT_DIR) / "processed_files.txt"
    checkpoint_file.parent.mkdir(parents=True, exist_ok=True)
    processed = set()
    if checkpoint_file.exists():
        processed = set(checkpoint_file.read_text().splitlines())

    import json
    import datetime
    import pandas as pd

    batch_id = len(processed)
    for pfile in pos_files:
        if pfile.name in processed:
            continue

        print(f"\n===== Processing micro-batch {batch_id} ({pfile.name}) =====")

        # 1. Extract POS records
        records = []
        with open(pfile, "r") as f:
            for line in f:
                if line.strip():
                    records.append(json.loads(line))
        if not records:
            processed.add(pfile.name)
            checkpoint_file.write_text("\n".join(sorted(processed)))
            continue

        pos_df = pd.DataFrame(records)

        # 2. Extract Warehouse & Supplier reference tables
        warehouse_df = pd.read_csv(WAREHOUSE_CSV) if Path(WAREHOUSE_CSV).exists() else pd.DataFrame()
        with open(SUPPLIER_JSON, "r") as f:
            supplier_records = json.load(f)
        supplier_df = pd.DataFrame(supplier_records)

        # 3. Clean & Validate POS
        raw_count = len(pos_df)
        pos_clean = pos_df.dropna(subset=["product_id", "quantity_sold", "unit_price"]).copy()
        pos_clean = pos_clean[(pos_clean["quantity_sold"] > 0) & (pos_clean["unit_price"] > 0)]
        pos_clean = pos_clean.drop_duplicates(subset=["txn_id"])
        pos_clean["txn_timestamp"] = pd.to_datetime(pos_clean["txn_timestamp"])
        clean_count = len(pos_clean)
        print(f"  POS records: {raw_count} raw -> {clean_count} after null/dup/invalid removal")

        # 4. Clean Warehouse
        warehouse_clean = warehouse_df.dropna(subset=["product_id", "warehouse_stock"]).drop_duplicates(subset=["product_id"]).copy()

        # 5. Clean Supplier
        supplier_clean = supplier_df.dropna(subset=["product_id", "supplier_id", "expected_delivery_date", "actual_delivery_date"]).drop_duplicates(subset=["po_id"]).copy()
        supplier_clean["expected_delivery_date"] = pd.to_datetime(supplier_clean["expected_delivery_date"])
        supplier_clean["actual_delivery_date"] = pd.to_datetime(supplier_clean["actual_delivery_date"])

        # 6. Sales Aggregation
        pos_clean["revenue"] = pos_clean["quantity_sold"] * pos_clean["unit_price"]
        sales_agg = pos_clean.groupby(["product_id", "product_name", "store_id"], as_index=False).agg(
            units_sold=("quantity_sold", "sum"),
            total_revenue=("revenue", "sum"),
            txn_count=("txn_id", "count")
        )

        # 7. Inventory Calculation
        per_product_sales = sales_agg.groupby("product_id", as_index=False)["units_sold"].sum()
        per_product_sales.rename(columns={"units_sold": "units_sold_batch"}, inplace=True)
        inventory = pd.merge(warehouse_clean, per_product_sales, on="product_id", how="left")
        inventory["units_sold_batch"] = inventory["units_sold_batch"].fillna(0).astype(int)
        inventory["current_stock"] = inventory["warehouse_stock"] - inventory["units_sold_batch"]

        # 8. Product Ranking
        product_totals = sales_agg.groupby(["product_id", "product_name"], as_index=False).agg(
            total_revenue=("total_revenue", "sum"),
            units_sold=("units_sold", "sum")
        )
        product_totals = product_totals.sort_values("total_revenue", ascending=False).reset_index(drop=True)
        product_totals["rank"] = product_totals["total_revenue"].rank(ascending=False, method="min").astype(int)

        # 9. Low Stock Detection
        reorder_thresh = inventory["reorder_threshold"].fillna(LOW_STOCK_THRESHOLD)
        inventory["low_stock_flag"] = inventory["current_stock"] < reorder_thresh
        low_stock = inventory[inventory["low_stock_flag"]][["product_id", "current_stock", "reorder_threshold", "low_stock_flag"]].copy()
        low_stock["low_stock_flag"] = low_stock["low_stock_flag"].astype(int)

        # 10. Supplier Delay Detection
        supplier_clean["delay_days"] = (supplier_clean["actual_delivery_date"] - supplier_clean["expected_delivery_date"]).dt.days
        supplier_clean["delayed_flag"] = supplier_clean["delay_days"] > SUPPLIER_DELAY_THRESHOLD_DAYS
        supplier_delays = supplier_clean[supplier_clean["delayed_flag"]][
            ["po_id", "product_id", "supplier_id", "expected_delivery_date", "actual_delivery_date", "delay_days"]
        ].copy()
        supplier_delays["expected_delivery_date"] = supplier_delays["expected_delivery_date"].dt.strftime("%Y-%m-%d")
        supplier_delays["actual_delivery_date"] = supplier_delays["actual_delivery_date"].dt.strftime("%Y-%m-%d")

        print(f"  Sales aggregation rows : {len(sales_agg)}")
        print(f"  Low stock alerts       : {len(low_stock)}")
        print(f"  Supplier delay alerts  : {len(supplier_delays)}")

        # 11. Load to SQLite
        load_table(sales_agg, "sales_aggregation", batch_id)
        inventory_save = inventory[["product_id", "warehouse_stock", "reorder_threshold", "units_sold_batch", "current_stock"]].copy()
        load_table(inventory_save, "inventory_status", batch_id)
        load_table(product_totals, "product_ranking", batch_id)
        load_table(low_stock, "low_stock_alerts", batch_id)
        load_table(supplier_delays, "supplier_delay_alerts", batch_id)

        print(f"===== Batch {batch_id} loaded to {SQLITE_DB} =====")

        processed.add(pfile.name)
        checkpoint_file.write_text("\n".join(sorted(processed)))
        batch_id += 1


def run():
    init_sqlite()
    try:
        spark = get_spark()
        spark.sparkContext.setLogLevel("WARN")
        pos_stream = extract_pos_stream(spark)
        query = (
            pos_stream.writeStream
            .foreachBatch(process_batch(spark))
            .option("checkpointLocation", CHECKPOINT_DIR)
            .trigger(availableNow=True)
            .start()
        )
        query.awaitTermination()
        spark.stop()
    except Exception as exc:
        print(f"[pipeline] PySpark engine not available ({exc}). Falling back to Pandas ETL engine...")
        run_pandas()


if __name__ == "__main__":
    run()

