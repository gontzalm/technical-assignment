import logging
from functools import cached_property
from pathlib import Path
from typing import Self

import duckdb
import tomllib

logger = logging.getLogger(__name__)


class Transformer:
    _CONFIG_DIR = Path("conf/transform")
    _SOURCE_LAYER = "bronze"
    _TARGET_LAYER = "silver"

    def __init__(
        self,
        sources: list[Path],
        transformations: dict[str, dict[str, str | list[str] | dict[str, str]]],
        write_config: dict[str, str],
        preview: bool = True,
    ) -> None:
        self.sources = sources
        self.transformations = transformations
        self.write_config = write_config
        self.preview = preview
        self._relations: dict[str, duckdb.DuckDBPyRelation]

    @cached_property
    def target_file(self) -> Path:
        return (
            Path("data-lake/layers") / self._TARGET_LAYER / self.write_config["target"]
        )

    def _read_sources(self) -> None:
        logger.info("Reading sources '%s'", self.sources)
        self._relations = {
            parquet.stem: duckdb.read_parquet(str(parquet)).set_alias(parquet.stem)
            for parquet in self.sources
        }

    def _filter(self) -> None:
        logger.info("Aplying filters")
        for rel, filters in self.transformations["filter"].items():
            for f in filters:
                logging.info("Aplying filter '%s' to relation '%s'", f, rel)
                self._relations[rel] = self._relations[rel].filter(f)

    def _join(self) -> None:
        join_config = self.transformations["join"]
        logger.info("Performing join operation with config '%s'", join_config)
        self._relations["output"] = (
            self._relations[join_config["base"]]
            .join(
                self._relations[join_config["other"]],
                join_config["on"],
                how=join_config["how"],
            )
            .select(*join_config["select"])
        ).set_alias("output")

    def _add_columns(self, columns: dict[str, str]) -> None:
        logger.info("Addinng columns '%s'", columns)
        self._relations["output"] = self._relations["output"].query(
            "output",
            f"from output select *, {','.join(expr + " as " + alias for alias, expr in columns.items())}",
        )

    # more operators...

    def _perform_operations(self) -> None:
        logger.info("Performing other transformation operations")
        for operator, input_ in self.transformations["operations"].items():
            self.__getattribute__("_" + operator)(input_)

    def _write_parquet(self) -> None:
        logger.info(
            "Writing target file '%s' to '%s' layer",
            self.target_file,
            self._TARGET_LAYER,
        )
        self._relations["output"].write_parquet(
            str(self.target_file), compression=self.write_config.get("compression")
        )

    def transform(self) -> None:
        self._read_sources()
        self._filter()
        self._join()
        self._perform_operations()
        self._write_parquet()
        if self.preview:
            logger.info("Output preview:\n")
            self._relations["output"].show()

    @classmethod
    def from_config(cls, config_file: str) -> Self:
        config = tomllib.loads((cls._CONFIG_DIR / config_file).read_text())
        return cls(
            [
                Path("data-lake/layers") / cls._SOURCE_LAYER / source
                for source in config["read"]["sources"]
            ],
            config["transform"],
            config["write"],
        )
