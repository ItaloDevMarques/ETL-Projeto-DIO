from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd


@dataclass(frozen=True)
class ExtractResult:
    clientes: pd.DataFrame
    vendas: pd.DataFrame


def _read_csv(path: Path) -> pd.DataFrame:
    
    if not path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {path}")

    # Muitos CSVs exportados (Excel, etc.) vêm com BOM, por isso utf-8-sig
    df = pd.read_csv(path, encoding="utf-8-sig")

    if df.empty:
        raise ValueError(f"Arquivo existe, mas está vazio: {path}")

    return df


def extract(clientes_csv: Path, vendas_csv: Path) -> ExtractResult:

    clientes = _read_csv(clientes_csv)
    vendas = _read_csv(vendas_csv)

    return ExtractResult(clientes=clientes, vendas=vendas)
