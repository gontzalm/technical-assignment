# technical-assignment

## How To Run

### Locally

Using the [uv](https://docs.astral.sh/uv/) package manager:

1. Make the pipelines executable:

   ```bash
   chmod -R +x pipelines
   ```

1. Install the dependencies:

   ```bash
   uv venv
   uv pip sync requirements.txt
   ```

1. Run the pipeline:

   ```bash
   uv run pipelines/full
   ```

### Docker

1. Build the docker image:

   ```bash
   docker build -t pipeline-runner .
   ```

1. Run the pipeline:

   ```bash
   docker run --rm \
     --mount type=bind,source=$(pwd)/data-lake,target=/app/data-lake \
     pipeline-runner /bin/bash pipelines/full
   ```

## Assumptions

- The raw JSON data is fully delivered prior to a pipeline execution (i.e. the
  target bronze data is replaced during the load phase). If the data was
  delivered in an incremental fashion, I would enable an `append` option in the
  load config file schema and then add the following (probably in another
  method) in the `JsonLoader` class of the [json.py](scripts/utils/load/json.py)
  module:

  ```python
  def load(self) -> None:
      self._read_file()
      self._add_metadata()
      if self.write_config.get("append"):
          # read target parquet and create tmp table
          duckdb.read_json(str(self.target_file)).create("target_tmp")

          # append new data to tmp table
          self._rel.insert_into("target_tmp")

          # point relation attribute to temp table
          self._rel = duckdb.table("target_tmp")

      self._write_parquet()
  ```

## Structure

```bash
.
├── conf -> Config files.
│  ├── load -> Load config files.
│  └── transform -> Transform  config files.
├── data-lake -> Data lake simulating an object store (e.g. S3).
│  ├── landing -> Landing zone (raw data).
│  ├── layers -> Layers (i.e. bronze, silver, gold).
│  └── warehouse -> Data warehouse.
├── pipelines -> Runnable data pipelines.
│  └── full -> Full data pipeline including all the assignment tasks.
├── scripts -> Scripts to load and transform data.
│  ├── utils -> Package including reusable classes.
│  ├── load_duckdb.py -> Script to load silver layer data into DuckDB.
│  ├── load_json.py -> Script to load JSON data into the bronze layer.
│  └── transform_sales.py -> Script to transform the sales data and load it into the silver layer.
├── tests
│  └── test_sales.py -> Data quality checks for the sales data.
├── Dockerfile -> Definition of the Docker image to run pipelines.
├── README.md -> This file (documentation).
├── requirements.in -> Dependencies.
└── requirements.txt -> Compiled (pinned) dependencies.
```

## Challenges

### Packages

For the load utilities (i.e. [load.py](scripts/utils/load.py)), I had to choose
which package to use between:

- [`pyarrow`](https://arrow.apache.org/docs/python/index.html)
- [`polars`](https://docs.pola.rs/api/python/stable/reference/)
- [`duckdb`](https://duckdb.org/docs/api/python/overview)

Taking into account the time constraint, the solution uses `duckdb` because:

- It was already necessary for the _Part 3: Data Warehouse Simulation with
  DuckDB_ and thus already a dependency
- I don't have experience with `pyarrow`, and the API didn't feel
  straightforward at first glance
- I have little experience with `polars`
- `duckdb` covers the functionality requested in the assignment and works great
  out of the box

However, the DuckDB Python API is not that mature and some functionality is not
implemented in the
[Relational API](https://duckdb.org/docs/api/python/relational_api) (in my
opinion the most Pythonic way to interface with DuckDB) and must be accessed
through `duckb.sql()`.

### Modularization

The solution to the assignment (source code) is configurable and reusable via
config files. However, it makes assumptions (e.g. target layers, source data
nature) and lacks rich configuration options. It should be taken as an initial
and immature (but functional) approach to a load/transform framework.

## How To Query the Data Warehouse

1. Start an interactive Python session:

   ```bash
   uv run python
   ```

1. Connect to the DuckDB warehouse:

   ```python
   >>> import duckb
   >>> con = duckdb.connect("data-lake/warehouse/warehouse.duckdb")
   ```

1. Use the Relational API to query the sales table:

   ```python
   # get the sales table as a DuckDB relation
   >>> sales = con.table("sales")

   # describe the relation (schema, basic statistics)
   >>> sales.describe()

   ┌─────────┬────────────────────┬────────────┬──────────────┬──────────┬────────────┬────────────────────┬────────────────────┬────────────────────┐
   │  aggr   │      sale_id       │ product_id │ product_name │ category │ sale_date  │      quantity      │       price        │    total_sales     │
   │ varchar │       double       │  varchar   │   varchar    │ varchar  │  varchar   │       double       │       double       │       double       │
   ├─────────┼────────────────────┼────────────┼──────────────┼──────────┼────────────┼────────────────────┼────────────────────┼────────────────────┤
   │ count   │               67.0 │ 67         │ 67           │ 67       │ 67         │               67.0 │               67.0 │               67.0 │
   │ mean    │ 46.701492537313435 │ NULL       │ NULL         │ NULL     │ NULL       │ 5.1940298507462686 │ 26.836716417910452 │ 141.70985074626864 │
   │ stddev  │  27.61149021110548 │ NULL       │ NULL         │ NULL     │ NULL       │  2.618343253478204 │  11.15648274486832 │ 102.35588167571882 │
   │ min     │                1.0 │ A12        │ Gadget C     │ Gadgets  │ 2023-11-09 │                1.0 │              10.24 │              16.72 │
   │ max     │               98.0 │ C34        │ Widget B     │ Widgets  │ 2024-11-03 │               10.0 │              49.89 │              461.5 │
   │ median  │               45.0 │ NULL       │ NULL         │ NULL     │ NULL       │                5.0 │              25.29 │             123.13 │
   └─────────┴────────────────────┴────────────┴──────────────┴──────────┴────────────┴────────────────────┴────────────────────┴────────────────────┘

   # retrieve the sum of total sales by category in the current year
   >>> sales.filter("year(sale_date) == year(current_date())").aggregate("category, sum(total_sales)", group_expr="category").show()

   ┌──────────┬────────────────────┐
   │ category │  sum(total_sales)  │
   │ varchar  │       double       │
   ├──────────┼────────────────────┤
   │ Widgets  │  4897.750000000001 │
   │ Gadgets  │ 2251.3499999999995 │
   └──────────┴────────────────────┘

   # leveraging the index on `sale_date`
   >>> sales.filter("sale_date between date '2024-01-01' and '2024-12-31'").aggregate("category, sum(total_sales)", group_expr="category").show()
   ```

1. Or directly execute DQL queries through the `sql()` method:

   ```python
   # retrieve the sum of total sales by category and year
   >>> con.sql("select year(sale_date) as year, category, sum(total_sales) from sales group by year(sale_date), category").show()

   ┌───────┬──────────┬────────────────────┐
   │ year  │ category │  sum(total_sales)  │
   │ int64 │ varchar  │       double       │
   ├───────┼──────────┼────────────────────┤
   │  2024 │ Gadgets  │ 2251.3499999999995 │
   │  2023 │ Widgets  │  899.8700000000001 │
   │  2024 │ Widgets  │  4897.750000000001 │
   │  2023 │ Gadgets  │ 1445.5900000000001 │
   └───────┴──────────┴────────────────────┘
   ```
