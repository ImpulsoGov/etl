# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT

"""Verifica a qualidade dos dados pós-processado do relatório egestor de financiamento."""

import pandas as pd
from prefect import task

from impulsoetl.loggers import habilitar_suporte_loguru


def verifica_diferenca_ctg_uf(
    df_extraido: pd.DataFrame,
    df_tratado: pd.DataFrame,
) -> bool:
    """Verifica se há diferença na contagem de estados"""
    return (
        df_extraido["uf_sigla"].nunique() - df_tratado["uf_sigla"].nunique()
        == 0
    )


def verifica_diferenca_ctg_municpios(
    df_extraido: pd.DataFrame,
    df_tratado: pd.DataFrame,
) -> bool:
    """Verifica se há diferença na contagem de municípios"""
    return (
        df_extraido["municipio_id_sus"].nunique()
        - df_tratado["municipio_id_sus"].nunique()
        == 0
    )


def verifica_diferenca_qtd_registros(
    df_extraido: pd.DataFrame, df_tratado: pd.DataFrame
) -> bool:
    """Verifica se há diferença no somatório de numerador"""
    return (
        df_extraido["municipio_id_sus"].count()
        - df_tratado["municipio_id_sus"].count()
        == 0
    )


@task(
    name="Validar Relatórios de Financiamento",
    description=(
        "Valida os dados dos relatórios de financiamento da Atenção "
        + "Primária à Saúde extraídos e transformados a partir do eGestor "
        + "Atenção Básica."
    ),
    tags=["aps", "egestor", "financiamento", "validar"],
    retries=0,
    retry_delay_seconds=None,
)
def verificar_relatorio_egestor(
    df_extraido: pd.DataFrame,
    df_tratado: pd.DataFrame,
) -> None:
    habilitar_suporte_loguru()
    assert verifica_diferenca_ctg_uf(df_extraido, df_tratado)
    assert verifica_diferenca_ctg_municpios(df_extraido, df_tratado)
    assert verifica_diferenca_qtd_registros(df_extraido, df_tratado)
