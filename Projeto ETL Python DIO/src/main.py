from __future__ import annotations

import argparse
from pathlib import Path

from config import DEFAULT_CLIENTES_CSV, DEFAULT_VENDAS_CSV, Paths
from extract import extract
from load import load
from logging_utils import setup_logging
from transform import transform


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Pipeline ETL (CSV) de vendas")
    p.add_argument(
        "--clientes",
        type=str,
        default=str(Paths.RAW_DIR / DEFAULT_CLIENTES_CSV),
        help="Path para clientes.csv",
    )
    p.add_argument(
        "--vendas",
        type=str,
        default=str(Paths.RAW_DIR / DEFAULT_VENDAS_CSV),
        help="Path para vendas.csv",
    )
    p.add_argument(
        "--out",
        type=str,
        default=str(Paths.OUTPUT_DIR),
        help="Diretório de saída analítica",
    )
    p.add_argument(
        "--processed",
        type=str,
        default=str(Paths.PROCESSED_DIR),
        help="Diretório de saída transacional limpa",
    )
    p.add_argument(
        "--log",
        type=str,
        default=str(Paths.OUTPUT_DIR / "pipeline.log"),
        help="Arquivo de log",
    )
    return p


def main() -> int:
    args = build_parser().parse_args()

    logger = setup_logging(Path(args.log))

    clientes_path = Path(args.clientes)
    vendas_path = Path(args.vendas)

    logger.info("Extract: lendo clientes=%s vendas=%s", clientes_path, vendas_path)
    extracted = extract(clientes_path, vendas_path)
    logger.info("Extract OK: clientes=%d vendas=%d", len(extracted.clientes), len(extracted.vendas))

    logger.info("Transform: iniciando")
    transformed = transform(extracted.clientes, extracted.vendas)
    logger.info(
        "Transform OK: clientes=%d vendas=%d", len(transformed.clientes_clean), len(transformed.vendas_clean)
    )

    logger.info("Load: gravando saídas")
    written = load(
        clientes_clean=transformed.clientes_clean,
        vendas_clean=transformed.vendas_clean,
        analytics_clientes=transformed.analytics_clientes,
        analytics_categorias=transformed.analytics_categorias,
        analytics_estados=transformed.analytics_estados,
        report=transformed.report,
        processed_dir=Path(args.processed),
        output_dir=Path(args.out),
    )

    for k, v in written.items():
        logger.info("Arquivo gerado [%s]: %s", k, v)

    logger.info("Pipeline finalizado com sucesso")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
