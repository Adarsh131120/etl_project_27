"""
streamlit_app.py
-----------------
Real-Time Inventory Control Dashboard — Enhanced Dark Mode & Interactive Analytics
"""

import json
import random
import sqlite3
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# Setup base paths
BASE = Path(__file__).resolve().parent.parent
if str(BASE) not in sys.path:
    sys.path.insert(0, str(BASE))

SQLITE_DB = BASE / "output" / "inventory.db"
POS_DIR = BASE / "data_sources" / "pos_sales"
WAREHOUSE_CSV = BASE / "data_sources" / "warehouse" / "warehouse.csv"

# Page configuration
st.set_page_config(
    page_title="Real-Time Inventory Control Hub",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom Cyberpunk Dark CSS
st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;600;700;800&display=swap');

    html, body, [class*="css"] {
        font-family: 'Plus Jakarta Sans', -apple-system, BlinkMacSystemFont, sans-serif;
    }

    .stApp {
        background: linear-gradient(135deg, #090D16 0%, #0F172A 50%, #0B0F19 100%);
        color: #F3F4F6;
    }

    /* Glassmorphism metric cards */
    .metric-card {
        background: rgba(30, 41, 59, 0.65);
        border: 1px solid rgba(255, 255, 255, 0.08);
        backdrop-filter: blur(16px);
        border-radius: 16px;
        padding: 22px 24px;
        box-shadow: 0 10px 25px -5px rgba(0, 0, 0, 0.5);
        transition: all 0.3s ease;
        height: 100%;
    }
    .metric-card:hover {
        transform: translateY(-4px);
        border-color: rgba(0, 242, 254, 0.4);
        box-shadow: 0 15px 30px -5px rgba(0, 242, 254, 0.2);
    }
    .metric-title {
        font-size: 0.8rem;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        color: #94A3B8;
        margin-bottom: 8px;
    }
    .metric-value {
        font-size: 2.1rem;
        font-weight: 800;
        line-height: 1.1;
        background: linear-gradient(90deg, #FFFFFF 0%, #00F2FE 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    .metric-value-purple {
        background: linear-gradient(90deg, #FFFFFF 0%, #A855F7 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    .metric-value-rose {
        background: linear-gradient(90deg, #FFFFFF 0%, #FB7185 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    .metric-value-amber {
        background: linear-gradient(90deg, #FFFFFF 0%, #FBBF24 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }

    .metric-badge {
        display: inline-flex;
        align-items: center;
        padding: 4px 12px;
        border-radius: 20px;
        font-size: 0.75rem;
        font-weight: 700;
        margin-top: 12px;
    }
    .badge-cyan { background: rgba(6, 182, 212, 0.15); color: #22D3EE; border: 1px solid rgba(34, 211, 238, 0.3); }
    .badge-purple { background: rgba(168, 85, 247, 0.15); color: #C084FC; border: 1px solid rgba(192, 132, 252, 0.3); }
    .badge-rose { background: rgba(244, 63, 94, 0.15); color: #FB7185; border: 1px solid rgba(251, 113, 133, 0.3); }
    .badge-amber { background: rgba(245, 158, 11, 0.15); color: #FBBF24; border: 1px solid rgba(251, 191, 36, 0.3); }

    /* Live pulse animation */
    .pulse-container {
        display: inline-flex;
        align-items: center;
        background: rgba(16, 185, 129, 0.12);
        border: 1px solid rgba(16, 185, 129, 0.3);
        padding: 6px 14px;
        border-radius: 30px;
        color: #34D399;
        font-size: 0.8rem;
        font-weight: 700;
    }
    .pulse-dot {
        width: 9px;
        height: 9px;
        border-radius: 50%;
        background-color: #10B981;
        box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.7);
        animation: pulse 1.8s infinite;
        margin-right: 8px;
    }
    @keyframes pulse {
        0% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.7); }
        70% { transform: scale(1); box-shadow: 0 0 0 8px rgba(16, 185, 129, 0); }
        100% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(16, 185, 129, 0); }
    }

    /* Tabs styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 12px;
        background-color: rgba(15, 23, 42, 0.6);
        padding: 8px 12px;
        border-radius: 14px;
        border: 1px solid rgba(255, 255, 255, 0.05);
    }
    .stTabs [data-baseweb="tab"] {
        height: 42px;
        border-radius: 10px;
        color: #94A3B8;
        font-weight: 600;
        font-size: 0.9rem;
    }
    .stTabs [aria-selected="true"] {
        background: linear-gradient(90deg, #00F2FE 0%, #4FACFE 100%) !important;
        color: #0F172A !important;
        font-weight: 800 !important;
    }

    /* Sidebar aesthetics */
    div[data-testid="stSidebar"] {
        background: #0B0F19 !important;
        border-right: 1px solid rgba(255, 255, 255, 0.06);
    }

    /* Dark Plotly background override */
    .js-plotly-plot .plotly .main-svg {
        background: transparent !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# --------------------------------------------------------------------------- #
# DATA ENGINE ACCESS
# --------------------------------------------------------------------------- #
def table_exists(conn, name: str) -> bool:
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,)
    )
    return cur.fetchone() is not None


@st.cache_data(ttl=5)
def load_table(table_name: str, refresh_token: int = 0) -> pd.DataFrame:
    if not SQLITE_DB.exists():
        return pd.DataFrame()
    conn = sqlite3.connect(SQLITE_DB)
    try:
        if not table_exists(conn, table_name):
            return pd.DataFrame()
        return pd.read_sql(f"SELECT * FROM {table_name}", conn)
    finally:
        conn.close()


def trigger_etl_pipeline():
    """Executes the ETL pipeline engine."""
    from src.pipeline import run_pandas
    run_pandas()


def simulate_live_batch():
    """Generates a new synthetic micro-batch of POS transactions and runs ETL."""
    POS_DIR.mkdir(parents=True, exist_ok=True)
    existing_files = list(POS_DIR.glob("pos_batch_*.json"))
    batch_num = len(existing_files) + 1
    new_file = POS_DIR / f"pos_batch_{batch_num:04d}.json"

    # Read catalog or warehouse products if available
    products = []
    if WAREHOUSE_CSV.exists():
        df_wh = pd.read_csv(WAREHOUSE_CSV)
        products = df_wh["product_id"].tolist()
    
    if not products:
        products = [f"P{idx:04d}" for idx in range(1, 100)]

    stores = ["Central", "East", "South", "West"]
    txns = []
    now = datetime.now()

    for i in range(random.randint(400, 750)):
        pid = random.choice(products)
        txns.append({
            "txn_id": f"TXN-SIM-{batch_num}-{i+1:04d}",
            "product_id": pid,
            "product_name": f"Product {pid}",
            "store_id": random.choice(stores),
            "quantity_sold": random.randint(1, 15),
            "unit_price": round(random.uniform(10.0, 450.0), 2),
            "txn_timestamp": (now - timedelta(minutes=random.randint(0, 120))).isoformat(),
        })

    with open(new_file, "w") as f:
        for t in txns:
            f.write(json.dumps(t) + "\n")

    # Run ETL
    trigger_etl_pipeline()
    return len(txns), new_file.name


# --------------------------------------------------------------------------- #
# SIDEBAR CONTROLS
# --------------------------------------------------------------------------- #
if "refresh_token" not in st.session_state:
    st.session_state.refresh_token = 0

st.sidebar.markdown("### ⚡ Live Stream Controls")

if st.sidebar.button("🚀 Inject Live POS Batch", help="Simulate real-time stream event"):
    with st.spinner("Streaming transactions into PySpark/Pandas ETL pipeline..."):
        count, fname = simulate_live_batch()
        st.session_state.refresh_token += 1
        st.cache_data.clear()
        st.toast(f"✅ Injected {count} new transactions ({fname}) into pipeline!", icon="⚡")

if st.sidebar.button("🔄 Manual Data Refresh"):
    st.session_state.refresh_token += 1
    st.cache_data.clear()
    st.toast("Dashboard data refreshed!", icon="🔄")

auto_refresh = st.sidebar.checkbox("⏱️ Auto-Refresh (Every 10s)", value=False)

st.sidebar.divider()
st.sidebar.markdown("### 🎛️ Interactive Filters")

# Load raw tables
sales_agg = load_table("sales_aggregation", st.session_state.refresh_token)
inventory = load_table("inventory_status", st.session_state.refresh_token)
ranking = load_table("product_ranking", st.session_state.refresh_token)
low_stock = load_table("low_stock_alerts", st.session_state.refresh_token)
supplier_delays = load_table("supplier_delay_alerts", st.session_state.refresh_token)

# Store Region filter
stores_available = ["All Regions"]
if not sales_agg.empty and "store_id" in sales_agg.columns:
    stores_available += sorted(sales_agg["store_id"].dropna().unique().tolist())

selected_store = st.sidebar.selectbox("Filter by Region / Store", stores_available)

# Custom Reorder Threshold Slider
custom_threshold = st.sidebar.slider(
    "Custom Reorder Threshold (Stock)", min_value=5, max_value=100, value=20, step=5
)

# Product Search
search_query = st.sidebar.text_input("🔍 Search Product Name/ID", "")

st.sidebar.divider()
st.sidebar.caption(f"🗄️ Database: `{SQLITE_DB.name}`")
st.sidebar.caption(f"🟢 Active Micro-Batches: `{sales_agg['batch_id'].nunique() if not sales_agg.empty else 0}`")


# Apply store filter to sales_agg
filtered_sales = sales_agg.copy()
if not filtered_sales.empty and selected_store != "All Regions":
    filtered_sales = filtered_sales[filtered_sales["store_id"] == selected_store]

if not filtered_sales.empty and search_query:
    filtered_sales = filtered_sales[
        filtered_sales["product_name"].str.contains(search_query, case=False, na=False) |
        filtered_sales["product_id"].str.contains(search_query, case=False, na=False)
    ]

# Calculate metrics
total_revenue = filtered_sales["total_revenue"].sum() if not filtered_sales.empty else 0
total_units = filtered_sales["units_sold"].sum() if not filtered_sales.empty else 0
total_txns = filtered_sales["txn_count"].sum() if not filtered_sales.empty else 0

latest_inv = inventory.sort_values("batch_id").drop_duplicates("product_id", keep="last") if not inventory.empty else pd.DataFrame()
if not latest_inv.empty:
    custom_low_stock_count = len(latest_inv[latest_inv["current_stock"] < custom_threshold])
else:
    custom_low_stock_count = 0

total_delays = len(supplier_delays) if not supplier_delays.empty else 0

# --------------------------------------------------------------------------- #
# DASHBOARD HEADER & SYSTEM STATUS
# --------------------------------------------------------------------------- #
top_left, top_right = st.columns([3, 1])

with top_left:
    st.title("⚡ Real-Time Inventory Control Hub")
    st.markdown(
        "<p style='color: #94A3B8; font-size: 1.05rem; margin-top: -10px;'>"
        "PySpark Structured Streaming Engine &nbsp;•&nbsp; Real-Time Analytics &nbsp;•&nbsp; Instant Reorder Alerts"
        "</p>",
        unsafe_allow_html=True,
    )

with top_right:
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(
        """
        <div style="text-align: right;">
            <span class="pulse-container">
                <span class="pulse-dot"></span> PIPELINE ONLINE
            </span>
        </div>
        """,
        unsafe_allow_html=True,
    )

st.markdown("<br>", unsafe_allow_html=True)

# --------------------------------------------------------------------------- #
# METRIC CARDS ROW
# --------------------------------------------------------------------------- #
m1, m2, m3, m4 = st.columns(4)

with m1:
    st.markdown(
        f"""
        <div class="metric-card">
            <div class="metric-title">Total Gross Revenue</div>
            <div class="metric-value">₹{total_revenue:,.0f}</div>
            <span class="metric-badge badge-cyan">Across {selected_store}</span>
        </div>
        """,
        unsafe_allow_html=True,
    )

with m2:
    st.markdown(
        f"""
        <div class="metric-card">
            <div class="metric-title">Units Sold & Transactions</div>
            <div class="metric-value metric-value-purple">{total_units:,.0f}</div>
            <span class="metric-badge badge-purple">{total_txns:,.0f} Orders Processed</span>
        </div>
        """,
        unsafe_allow_html=True,
    )

with m3:
    badge_cls = "badge-rose" if custom_low_stock_count > 50 else "badge-amber"
    st.markdown(
        f"""
        <div class="metric-card">
            <div class="metric-title">Low Stock Alerts</div>
            <div class="metric-value metric-value-rose">{custom_low_stock_count:,}</div>
            <span class="metric-badge {badge_cls}">Threshold &lt; {custom_threshold} Units</span>
        </div>
        """,
        unsafe_allow_html=True,
    )

with m4:
    st.markdown(
        f"""
        <div class="metric-card">
            <div class="metric-title">Supplier Delay Alerts</div>
            <div class="metric-value metric-value-amber">{total_delays:,}</div>
            <span class="metric-badge badge-amber">&gt; 2 Days Late Delivery</span>
        </div>
        """,
        unsafe_allow_html=True,
    )

st.markdown("<br><br>", unsafe_allow_html=True)

# --------------------------------------------------------------------------- #
# MAIN DASHBOARD TABS
# --------------------------------------------------------------------------- #
tab_overview, tab_inventory, tab_suppliers, tab_stream = st.tabs([
    "📈 Executive Overview",
    "📦 Inventory & Stock Health",
    "🚚 Supplier Operations",
    "⚡ Stream Pipeline Monitor"
])

# --------------------------------------------------------------------------- #
# TAB 1: EXECUTIVE OVERVIEW
# --------------------------------------------------------------------------- #
with tab_overview:
    c1, c2 = st.columns([2, 1])

    with c1:
        st.markdown("#### 🚀 Top 15 Products by Revenue")
        if filtered_sales.empty:
            st.info("No sales data matching current filters.")
        else:
            top_products = (
                filtered_sales.groupby("product_name", as_index=False)["total_revenue"]
                .sum()
                .sort_values("total_revenue", ascending=False)
                .head(15)
            )
            fig_bar = px.bar(
                top_products,
                x="total_revenue",
                y="product_name",
                orientation="h",
                color="total_revenue",
                color_continuous_scale="Electric",
                labels={"total_revenue": "Revenue (₹)", "product_name": "Product"},
            )
            fig_bar.update_layout(
                template="plotly_dark",
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                yaxis={"categoryorder": "total ascending"},
                margin=dict(l=10, r=10, t=20, b=20),
                height=420,
                coloraxis_showscale=False,
            )
            st.plotly_chart(fig_bar, use_container_width=True)

    with c2:
        st.markdown("#### 🗺️ Revenue by Region")
        if sales_agg.empty:
            st.info("No sales data loaded.")
        else:
            by_store = sales_agg.groupby("store_id", as_index=False)["total_revenue"].sum()
            fig_donut = px.pie(
                by_store,
                names="store_id",
                values="total_revenue",
                hole=0.5,
                color_discrete_sequence=px.colors.qualitative.Pastel,
            )
            fig_donut.update_layout(
                template="plotly_dark",
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                margin=dict(l=10, r=10, t=20, b=20),
                height=420,
                legend=dict(orientation="h", yanchor="bottom", y=-0.1, xanchor="center", x=0.5),
            )
            st.plotly_chart(fig_donut, use_container_width=True)

    # Batch Trend Line Chart
    st.markdown("#### 📊 Real-Time Micro-Batch Revenue & Units Trend")
    if sales_agg.empty:
        st.info("No micro-batch timeline data available.")
    else:
        batch_trend = sales_agg.groupby("batch_id", as_index=False).agg(
            total_revenue=("total_revenue", "sum"),
            units_sold=("units_sold", "sum"),
            txn_count=("txn_count", "sum")
        ).sort_values("batch_id")

        fig_trend = go.Figure()
        fig_trend.add_trace(go.Scatter(
            x=batch_trend["batch_id"],
            y=batch_trend["total_revenue"],
            name="Revenue (₹)",
            mode="lines+markers",
            line=dict(color="#00F2FE", width=3, shape="spline"),
            marker=dict(size=8, color="#00F2FE"),
            fill="tozeroy",
            fillcolor="rgba(0, 242, 254, 0.08)"
        ))
        fig_trend.add_trace(go.Scatter(
            x=batch_trend["batch_id"],
            y=batch_trend["units_sold"],
            name="Units Sold",
            mode="lines+markers",
            yaxis="y2",
            line=dict(color="#A855F7", width=3, dash="dot"),
            marker=dict(size=6, color="#A855F7")
        ))
        fig_trend.update_layout(
            template="plotly_dark",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(title="Micro-Batch ID", gridcolor="rgba(255,255,255,0.05)"),
            yaxis=dict(title="Revenue (₹)", gridcolor="rgba(255,255,255,0.05)"),
            yaxis2=dict(title="Units Sold", overlaying="y", side="right"),
            margin=dict(l=10, r=10, t=30, b=20),
            height=360,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        st.plotly_chart(fig_trend, use_container_width=True)

# --------------------------------------------------------------------------- #
# TAB 2: INVENTORY & STOCK HEALTH
# --------------------------------------------------------------------------- #
with tab_inventory:
    col_inv1, col_inv2 = st.columns([1, 1])

    with col_inv1:
        st.markdown("#### 🚨 Products Below Reorder Threshold")
        if latest_inv.empty:
            st.info("No inventory records found.")
        else:
            critical_items = latest_inv[latest_inv["current_stock"] < custom_threshold].sort_values("current_stock")
            if critical_items.empty:
                st.success("🎉 All stock levels are currently above the threshold!")
            else:
                critical_items["Stock Status"] = critical_items["current_stock"].apply(
                    lambda x: "🔴 CRITICAL" if x < 10 else "⚠️ LOW STOCK"
                )
                st.dataframe(
                    critical_items[["product_id", "warehouse_stock", "units_sold_batch", "current_stock", "Stock Status"]],
                    column_config={
                        "product_id": "Product ID",
                        "warehouse_stock": "Initial Warehouse Stock",
                        "units_sold_batch": "Batch Sales",
                        "current_stock": st.column_config.NumberColumn(
                            "Current Stock",
                            format="%d 📦",
                        ),
                    },
                    use_container_width=True,
                    height=450,
                )

    with col_inv2:
        st.markdown("#### 📉 Lowest 15 Stock Levels (Visual)")
        if latest_inv.empty:
            st.info("No inventory data.")
        else:
            lowest_15 = latest_inv.sort_values("current_stock").head(15)
            fig_stock = px.bar(
                lowest_15,
                x="current_stock",
                y="product_id",
                orientation="h",
                color="current_stock",
                color_continuous_scale="Sunset",
                labels={"current_stock": "Current Stock Units", "product_id": "Product ID"},
            )
            fig_stock.add_vline(x=custom_threshold, line_dash="dash", line_color="#FB7185", annotation_text="Threshold")
            fig_stock.update_layout(
                template="plotly_dark",
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                yaxis={"categoryorder": "total descending"},
                margin=dict(l=10, r=10, t=20, b=20),
                height=450,
                coloraxis_showscale=False,
            )
            st.plotly_chart(fig_stock, use_container_width=True)

    # Leaderboard ranking
    st.markdown("#### 🏆 Product Revenue Leaderboard & Sales Analytics")
    if ranking.empty:
        st.info("No ranking data.")
    else:
        latest_rank = ranking[ranking["batch_id"] == ranking["batch_id"].max()].sort_values("rank")
        st.dataframe(
            latest_rank[["rank", "product_id", "product_name", "units_sold", "total_revenue"]],
            column_config={
                "rank": st.column_config.NumberColumn("Rank", format="#%d"),
                "total_revenue": st.column_config.NumberColumn("Total Revenue", format="₹%,.2f"),
                "units_sold": "Units Sold",
            },
            use_container_width=True,
            height=300,
        )

# --------------------------------------------------------------------------- #
# TAB 3: SUPPLIER OPERATIONS
# --------------------------------------------------------------------------- #
with tab_suppliers:
    sup_col1, sup_col2 = st.columns([1, 1])

    with sup_col1:
        st.markdown("#### 🚚 Delayed Purchase Orders (> 2 Days)")
        if supplier_delays.empty:
            st.success("No supplier delays detected.")
        else:
            latest_delays = supplier_delays[supplier_delays["batch_id"] == supplier_delays["batch_id"].max()].copy()
            st.dataframe(
                latest_delays[["po_id", "product_id", "supplier_id", "expected_delivery_date", "actual_delivery_date", "delay_days"]].sort_values("delay_days", ascending=False),
                column_config={
                    "po_id": "PO Number",
                    "supplier_id": "Supplier Code",
                    "delay_days": st.column_config.NumberColumn("Delay (Days)", format="%d ⚠️"),
                },
                use_container_width=True,
                height=420,
            )

    with sup_col2:
        st.markdown("#### 🏭 Delay Frequency by Supplier")
        if supplier_delays.empty:
            st.info("No supplier delay metrics.")
        else:
            sup_summary = supplier_delays.groupby("supplier_id", as_index=False).agg(
                delayed_orders=("po_id", "count"),
                avg_delay=("delay_days", "mean")
            )
            fig_sup = px.bar(
                sup_summary,
                x="supplier_id",
                y="delayed_orders",
                color="avg_delay",
                color_continuous_scale="Oranges",
                labels={"delayed_orders": "Delayed POs Count", "supplier_id": "Supplier ID", "avg_delay": "Avg Delay (Days)"},
                text_auto=True,
            )
            fig_sup.update_layout(
                template="plotly_dark",
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                margin=dict(l=10, r=10, t=20, b=20),
                height=420,
            )
            st.plotly_chart(fig_sup, use_container_width=True)

# --------------------------------------------------------------------------- #
# TAB 4: STREAM PIPELINE MONITOR
# --------------------------------------------------------------------------- #
with tab_stream:
    st.markdown("#### ⚡ PySpark / Pandas Structured Streaming Architecture")
    
    st.markdown(
        """
        ```
        POS Sales (JSON Feeds)  ----+
        Warehouse CSV (Stock)   ----+---> Streaming Engine (Extract -> Clean -> 5 Business Transformations)
        Supplier API (Updates)  ----+                    |
                                                         v
                                              SQLite (inventory.db)
                                                         |
                                                         v
                                           Streamlit Cyberpunk Control Hub
        ```
        """
    )

    p1, p2 = st.columns(2)
    with p1:
        st.markdown("##### 📁 POS JSON Stream Input Directory")
        pos_files = sorted(list(POS_DIR.glob("*.json"))) if POS_DIR.exists() else []
        st.write(f"Total POS Batch files generated: `{len(pos_files)}`")
        if pos_files:
            file_details = [{"File Name": f.name, "Size (KB)": round(f.stat().st_size / 1024, 2)} for f in pos_files[-10:]]
            st.dataframe(pd.DataFrame(file_details), use_container_width=True, height=250)

    with p2:
        st.markdown("##### ⚙️ Pipeline Database Metadata")
        if sales_agg.empty:
            st.info("No loaded batch metadata.")
        else:
            batch_summary = sales_agg.groupby("batch_id", as_index=False).agg(
                records_processed=("txn_count", "sum"),
                total_batch_revenue=("total_revenue", "sum"),
                loaded_timestamp=("loaded_at", "first")
            )
            st.dataframe(batch_summary, use_container_width=True, height=250)


# --------------------------------------------------------------------------- #
# AUTO REFRESH HANDLER
# --------------------------------------------------------------------------- #
if auto_refresh:
    time.sleep(10)
    st.cache_data.clear()
    st.rerun()
