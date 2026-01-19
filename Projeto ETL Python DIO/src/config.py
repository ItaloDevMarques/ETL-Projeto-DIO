from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Paths:

    ROOT: Path = Path(__file__).resolve().parents[1]

    RAW_DIR: Path = ROOT / "data" / "raw"
    PROCESSED_DIR: Path = ROOT / "data" / "processed"
    OUTPUT_DIR: Path = ROOT / "data" / "output"


DEFAULT_CLIENTES_CSV = "clientes.csv"
DEFAULT_VENDAS_CSV = "vendas.csv"

