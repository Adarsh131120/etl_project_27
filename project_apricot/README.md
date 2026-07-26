# Real-Time Inventory Control System — PySpark ETL + Streamlit

```
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
SQL Server Database   (SQLite ships as a local stand-in — see below)
          |
          v
Streamlit Live Dashboard
```

## About the data

The POS sales feed is **real data** — 8,399 actual order-line transactions
across 1,263 products from the widely-used "Superstore Sales" retail
dataset (the same one behind many Kaggle notebooks). Kaggle itself needs an
authenticated API call to download from, and this project's sandbox can't
reach kaggle.com, so `src/download_kaggle_data.py` pulls the identical
dataset from its public GitHub mirror instead.

Warehouse stock levels and supplier delivery updates are **simulated**,
generated per real product — the source dataset only covers sales, so there's
no genuine inventory or supplier feed to pull in for those two boxes in the
diagram.

> Want the authentic Kaggle CSV instead of the GitHub mirror? Download it
> from Kaggle yourself, drop it at `data_sources/raw/superstoreSales.csv`,
> and re-run `download_kaggle_data.py` — it'll skip the download step and
> reshape your file the same way.

## Project layout

```
project_apricot/
├── data_sources/
│   ├── raw/               # the downloaded real dataset (superstoreSales.csv)
│   ├── pos_sales/         # real transactions reshaped into POS JSON micro-batches
│   ├── warehouse/         # warehouse.csv — simulated stock, one row per real product
│   └── supplier/          # supplier_updates.json — simulated deliveries, per real product
├── src/
│   ├── download_kaggle_data.py    # fetches + reshapes the real dataset (run first)
│   ├── generate_sample_data.py    # optional: pure synthetic data instead of real data
│   ├── pipeline.py                # the ETL pipeline (Extract -> Clean -> Transform -> Load)
│   ├── streamlit_app.py           # the live dashboard (replaces Power BI)
│   └── sql_server_load_example.py # how to swap SQLite for real SQL Server via JDBC
├── output/
│   └── inventory.db       # SQLite database (SQL Server stand-in) — Streamlit reads this live
└── requirements.txt
```

## How it works

**Extract** — `pipeline.py` opens `data_sources/pos_sales/` as a PySpark
**Structured Streaming** file source. Each JSON file is a micro-batch of
real transactions (this is exactly how POS systems that write logs to a
shared folder/S3/ADLS bucket behave in production). Warehouse CSV and
Supplier API data are read as reference tables on each batch.

**Clean & Validate** — nulls dropped (`dropna`), duplicate transactions
removed (`dropDuplicates`), invalid values (zero/negative quantity or price)
filtered out.

**Transform** — five business transformations run per micro-batch:
| Transformation | What it does |
|---|---|
| Sales Aggregation | units sold + revenue per product/store |
| Inventory Calculation | warehouse stock − units sold this batch = current stock |
| Product Ranking | ranks products by revenue (best-sellers) |
| Low Stock Detection | flags products under the reorder threshold |
| Supplier Delay Detection | flags POs delivered more than 2 days late |

**Load** — each result is written to `output/inventory.db` (SQLite, used
here so the project runs with zero external infrastructure). See
`src/sql_server_load_example.py` for the JDBC swap to point this at a real
SQL Server instance instead.

**Visualize** — `src/streamlit_app.py` connects straight to that SQLite
database and renders: revenue KPIs, top products by revenue, revenue by
store/region, lowest-stock products, the product leaderboard, and live
low-stock / supplier-delay alert tables. A sidebar Refresh button (or the
auto-refresh checkbox) picks up whatever `pipeline.py` has loaded most
recently — this is the "live dashboard" from the diagram.

## Run it

```bash
pip install -r requirements.txt

# 1. Download the real retail dataset and reshape it into POS/warehouse/supplier feeds
python src/download_kaggle_data.py

# 2. Run the streaming ETL pipeline — processes every POS micro-batch file
#    currently in data_sources/pos_sales/, cleans, transforms, and loads it
python src/pipeline.py

# 3. Launch the live dashboard
streamlit run src/streamlit_app.py
```

To simulate new sales arriving in real time, drop more POS JSON files into
`data_sources/pos_sales/` (or re-run `download_kaggle_data.py` against a
larger source file) and run `pipeline.py` again — it only processes files
it hasn't seen yet. Click Refresh in Streamlit to see the new numbers.

For a truly continuous, long-running stream (rather than "process what's
there and stop"), swap `.trigger(availableNow=True)` in `pipeline.py` for
`.trigger(processingTime="10 seconds")` and run it as a long-lived process
alongside Streamlit's auto-refresh checkbox.

## Verified test run (real data)

- 8,399 real transactions, 1,263 unique real products, split into 12
  micro-batches of ~700 rows each
- Each batch: 700 raw POS records → 700 clean (source dataset was already
  free of the nulls/dupes we deliberately inject in the synthetic generator)
- ~650–665 product/store aggregation rows per batch
- 116–134 low-stock alerts per batch, 852 supplier delay alerts (simulated
  supplier data, same every batch since it's a static reference table)
- Streamlit dashboard confirmed serving (HTTP 200) with no runtime errors
