import logging

import duckdb

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

DATABASE = "data-lake/warehouse/warehouse.duckdb"
INDEX = "sale_date"


def main() -> None:
    logger.info("Connecting to DuckDB database '%s'", DATABASE)
    con = duckdb.connect(DATABASE)

    logger.info("Reading sales data (silver layer)")
    con.read_parquet("data-lake/layers/silver/sales.parquet").set_alias("sales_silver")

    logger.info("Creating table sales")
    con.sql("create or replace table sales as select * from sales_silver")
    logger.info("Creating index on '%s' column to improve performance", INDEX)
    con.sql(f"create index {INDEX}_idx on sales ({INDEX})")


if __name__ == "__main__":
    main()
