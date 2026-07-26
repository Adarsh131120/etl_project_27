"""
sql_server_load_example.py
---------------------------
This project loads data into SQLite by default (output/inventory.db) so it runs
anywhere with zero setup. The diagram calls for SQL Server, so here's the drop-in
replacement for load_table() in pipeline.py once you have a real SQL Server
instance reachable from this machine.

Steps to switch:
  1. Download the Microsoft JDBC Driver for SQL Server (mssql-jdbc-*.jar).
  2. Add it to Spark: SparkSession.builder.config("spark.jars", "/path/to/mssql-jdbc-XX.jar")
  3. Replace load_table() in pipeline.py with write_to_sql_server() below, operating
     on the Spark DataFrame directly (no need to convert to pandas first).
"""

from pyspark.sql import DataFrame

SQL_SERVER_JDBC_URL = "jdbc:sqlserver://<your-server>.database.windows.net:1433;databaseName=InventoryDB"
SQL_SERVER_PROPERTIES = {
    "user": "<username>",
    "password": "<password>",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
}


def write_to_sql_server(df: DataFrame, table_name: str, mode: str = "append"):
    """Writes a Spark DataFrame straight to SQL Server via JDBC — no pandas hop needed."""
    (
        df.write
        .jdbc(
            url=SQL_SERVER_JDBC_URL,
            table=table_name,
            mode=mode,
            properties=SQL_SERVER_PROPERTIES,
        )
    )


# Example usage inside process_batch()/_inner() in pipeline.py:
#
#   write_to_sql_server(sales_agg, "sales_aggregation")
#   write_to_sql_server(inventory, "inventory_status")
#   write_to_sql_server(ranking, "product_ranking")
#   write_to_sql_server(low_stock, "low_stock_alerts")
#   write_to_sql_server(supplier_delays, "supplier_delay_alerts")
#
# Power BI would then connect live to the same SQL Server database
# (Get Data -> SQL Server -> DirectQuery) for the real-time dashboard.
