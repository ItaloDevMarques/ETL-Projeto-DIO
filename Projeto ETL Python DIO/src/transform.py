from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class TransformResult:
    clientes_clean: pd.DataFrame
    vendas_clean: pd.DataFrame
    analytics_clientes: pd.DataFrame
    analytics_categorias: pd.DataFrame
    analytics_estados: pd.DataFrame
    report: dict


def _normalize_text_series(s: pd.Series) -> pd.Series:
    # normaliza espaços e caixa
    return (
        s.astype(str)
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
    )


def transform(clientes_raw: pd.DataFrame, vendas_raw: pd.DataFrame) -> TransformResult:

    report: dict[str, int] = {}

    clientes = clientes_raw.copy()
    vendas = vendas_raw.copy()

    # Padroniza nomes de colunas (minúsculo)
    clientes.columns = [c.strip() for c in clientes.columns]
    vendas.columns = [c.strip() for c in vendas.columns]

    # Contagens iniciais
    report["clientes_in"] = int(len(clientes))
    report["vendas_in"] = int(len(vendas))

    # Tipos básicos
    # (não tenta "consertar" valores, apenas converte e deixa NaN quando não dá)
    if "cliente_id" in clientes.columns:
        clientes["cliente_id"] = pd.to_numeric(clientes["cliente_id"], errors="coerce").astype("Int64")
    if "cliente_id" in vendas.columns:
        vendas["cliente_id"] = pd.to_numeric(vendas["cliente_id"], errors="coerce").astype("Int64")

    # Texto
    if "estado" in clientes.columns:
        clientes["estado"] = _normalize_text_series(clientes["estado"]).str.upper()

    if "categoria" in vendas.columns:
        cat = _normalize_text_series(vendas["categoria"]).str.lower()
        # Normalização simples e explícita (você pode expandir)
        cat_map = {
            "eletronico": "Eletrônicos",
            "eletrônico": "Eletrônicos",
            "eletronicos": "Eletrônicos",
            "eletrônicos": "Eletrônicos",
            "moveis": "Móveis",
            "móveis": "Móveis",
        }
        vendas["categoria"] = cat.map(cat_map).fillna(vendas["categoria"].astype(str).str.strip())

    # Datas
    # data_cadastro e data_venda podem vir em formatos diferentes
    if "data_cadastro" in clientes.columns:
        clientes["data_cadastro"] = pd.to_datetime(clientes["data_cadastro"], errors="coerce")

    if "data_venda" in vendas.columns:
        # tenta inferir; se tiver dd/mm/aaaa, isso pode ser interpretado conforme locale
        # no seu CSV, há "10/05/2023" que costuma ser dd/mm/aaaa no BR.
        vendas["data_venda"] = pd.to_datetime(vendas["data_venda"], errors="coerce", dayfirst=True)

    # Valor
    if "valor" in vendas.columns:
        vendas["valor"] = pd.to_numeric(vendas["valor"], errors="coerce")

    # Regras de descarte
    before = len(vendas)
    vendas = vendas.dropna(subset=["cliente_id"])  # sem id de cliente não dá pra usar
    report["vendas_sem_cliente_id"] = int(before - len(vendas))

    # Remove valores inválidos
    before = len(vendas)
    vendas = vendas.dropna(subset=["valor"])
    vendas = vendas[vendas["valor"] > 0]
    report["vendas_valor_invalido"] = int(before - len(vendas))

    # Remove vendas com cliente inexistente
    before = len(vendas)
    valid_client_ids = set(clientes["cliente_id"].dropna().astype(int).tolist())
    vendas = vendas[vendas["cliente_id"].astype("Int64").isin(valid_client_ids)]
    report["vendas_cliente_inexistente"] = int(before - len(vendas))

    # Limpeza final: remove clientes sem id válido
    before = len(clientes)
    clientes = clientes.dropna(subset=["cliente_id"]).copy()
    report["clientes_id_invalido"] = int(before - len(clientes))

    # Contagens finais
    report["clientes_out"] = int(len(clientes))
    report["vendas_out"] = int(len(vendas))

    # Agregados
    # Por cliente
    analytics_clientes = (
        vendas.groupby("cliente_id", as_index=False)
        .agg(
            total_gasto=("valor", "sum"),
            qtd_compras=("venda_id", "count"),
            ticket_medio=("valor", "mean"),
            primeira_compra=("data_venda", "min"),
            ultima_compra=("data_venda", "max"),
        )
    )

    # Enriquecimento com dados do cliente
    if {"cliente_id", "nome", "estado"}.issubset(set(clientes.columns)):
        analytics_clientes = analytics_clientes.merge(
            clientes[["cliente_id", "nome", "estado"]],
            on="cliente_id",
            how="left",
        )

    # Por categoria
    if "categoria" in vendas.columns:
        analytics_categorias = (
            vendas.groupby("categoria", as_index=False)
            .agg(
                total_vendas=("valor", "sum"),
                qtd_vendas=("venda_id", "count"),
                ticket_medio=("valor", "mean"),
            )
            .sort_values("total_vendas", ascending=False)
        )
    else:
        analytics_categorias = pd.DataFrame()

    # Por estado (precisa do merge vendas->clientes)
    if {"cliente_id", "estado"}.issubset(set(clientes.columns)):
        vendas_estado = vendas.merge(clientes[["cliente_id", "estado"]], on="cliente_id", how="left")
        analytics_estados = (
            vendas_estado.groupby("estado", as_index=False)
            .agg(
                total_vendas=("valor", "sum"),
                qtd_vendas=("venda_id", "count"),
                ticket_medio=("valor", "mean"),
            )
            .sort_values("total_vendas", ascending=False)
        )
    else:
        analytics_estados = pd.DataFrame()

    # Padroniza tipos de saída
    vendas_clean = vendas.sort_values(["data_venda", "venda_id"], na_position="last")
    clientes_clean = clientes.sort_values(["cliente_id"])

    return TransformResult(
        clientes_clean=clientes_clean,
        vendas_clean=vendas_clean,
        analytics_clientes=analytics_clientes,
        analytics_categorias=analytics_categorias,
        analytics_estados=analytics_estados,
        report=report,
    )
