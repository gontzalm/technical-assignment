import logging
from datetime import UTC, datetime
from functools import cached_property
from pathlib import Path
from typing import Self

import duckdb
import tomllib

logger = logging.getLogger(__name__)


class JsonLoader:
    _CONFIG_DIR = Path("conf/load")
    _LANDING_BUCKET = "landing"
    _TARGET_LAYER = "bronze"

    def __init__(self, source_file: Path, write_config: dict[str, str]) -> None:
        self.source_file = source_file
        self.write_config = write_config
        self._rel: duckdb.DuckDBPyRelation

    @cached_property
    def target_file(self) -> Path:
        return (
            Path("data-lake/layers") / self._TARGET_LAYER / self.write_config["target"]
        )

    def _read_file(self) -> None:
        logger.info(
            "Reading source file '%s' into a DuckDB realation", self.source_file
        )
        self._rel = duckdb.read_json(str(self.source_file))
        logger.info("Successfully read '%i' rows", self._rel.shape[0])

    def _add_metadata(self) -> None:
        self._rel = self._rel.select(
            duckdb.StarExpression(),
            duckdb.ConstantExpression(datetime.now(tz=UTC)).alias(
                "ingestion_timestamp"
            ),
        )

    def _write_parquet(self) -> None:
        logger.info(
            "Writing target file '%s' to '%s' layer",
            self.target_file,
            self._TARGET_LAYER,
        )
        self._rel.write_parquet(
            str(self.target_file), compression=self.write_config.get("compression")
        )

    def load(self) -> None:
        self._read_file()
        self._add_metadata()
        self._write_parquet()

    @classmethod
    def from_config(cls, config_file: str) -> Self:
        config = tomllib.loads((cls._CONFIG_DIR / config_file).read_text())
        return cls(
            Path("data-lake") / cls._LANDING_BUCKET / config["read"]["file"],
            config["write"],
        )
