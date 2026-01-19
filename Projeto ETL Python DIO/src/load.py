from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


def _save_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8")


def _save_json(data: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def load(
    *,
    clientes_clean: pd.DataFrame,
    vendas_clean: pd.DataFrame,
    analytics_clientes: pd.DataFrame,
    analytics_categorias: pd.DataFrame,
    analytics_estados: pd.DataFrame,
    report: dict,
    processed_dir: Path,
    output_dir: Path,
) -> dict[str, Path]:

    # Saídas transacionais limpas
    clientes_path = processed_dir / "clientes_clean.csv"
    vendas_path = processed_dir / "vendas_clean.csv"

    _save_csv(clientes_clean, clientes_path)
    _save_csv(vendas_clean, vendas_path)

    # Saídas analíticas
    out_clientes = output_dir / "analytics_clientes.csv"
    out_categorias = output_dir / "analytics_categorias.csv"
    out_estados = output_dir / "analytics_estados.csv"

    _save_csv(analytics_clientes, out_clientes)
    _save_csv(analytics_categorias, out_categorias)
    _save_csv(analytics_estados, out_estados)

    # Report
    report_path = output_dir / "pipeline_report.json"
    _save_json(report, report_path)

    return {
        "clientes_clean": clientes_path,
        "vendas_clean": vendas_path,
        "analytics_clientes": out_clientes,
        "analytics_categorias": out_categorias,
        "analytics_estados": out_estados,
        "report": report_path,
    }
