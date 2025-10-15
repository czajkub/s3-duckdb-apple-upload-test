from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb
from duckdb import DuckDBPyRelation

from app.ducky.config import settings


@dataclass
class DuckDBClient:
    path: Path | str = f"{settings.DUCKDB_FILENAME}"

    def __post_init__(self):
        if self.path.startswith("localhost"):
            self.path = "http://" + self.path

        if self.path.startswith(("http://", "https://")):
            duckdb.sql("""
                    INSTALL httpfs;
                    LOAD httpfs;
                """)
        else:
            self.path = Path(self.path)

    @staticmethod
    def format_response(
        response: DuckDBPyRelation | list[DuckDBPyRelation],
    ) -> list[dict[str, Any]]:
        if isinstance(response, DuckDBPyRelation):
            return response.df().to_dict(orient="records")
        records = [record.df().to_dict(orient="records") for record in response]
        return sum(records, [])
