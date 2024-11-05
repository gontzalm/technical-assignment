import duckdb
import pytest


@pytest.fixture(scope="class")
def products() -> duckdb.DuckDBPyRelation:
    return duckdb.read_parquet("data-lake/layers/bronze/products.parquet")


@pytest.fixture(scope="class")
def sales() -> duckdb.DuckDBPyRelation:
    return duckdb.read_parquet("data-lake/layers/silver/sales.parquet")


class TestSales:
    def test_not_null_sale_id(self, sales: duckdb.DuckDBPyRelation) -> None:
        assert sales.filter("sale_id is null").count("*").fetchone()[0] == 0

    def test_non_negative_qty_or_price(self, sales: duckdb.DuckDBPyRelation) -> None:
        assert sales.filter("quantity < 0 or price < 0").count("*").fetchone()[0] == 0

    def test_total_sales(self, sales: duckdb.DuckDBPyRelation) -> None:
        assert (
            sales.filter("total_sales != quantity * price").count("*").fetchone()[0]
            == 0
        )

    def test_product_id(
        self, sales: duckdb.DuckDBPyRelation, products: duckdb.DuckDBPyRelation
    ) -> None:
        assert (
            sales.select("product_id").distinct().count("*").fetchone()[0]
            == products.select("product_id").distinct().count("*").fetchone()[0]
        )
