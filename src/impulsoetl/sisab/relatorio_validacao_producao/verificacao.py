# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Verifica a qualidade dos dados de validação por ficha por aplicação."""


import pandas as pd
from prefect import task

from impulsoetl.loggers import habilitar_suporte_loguru, logger


def verifica_diferenca_ctg_municpios(
    df_extraido: pd.DataFrame, df_tratado: pd.DataFrame
) -> bool:
    """Verifica se há diferença na contagem de municípios"""
    return (
        df_extraido["IBGE"].nunique()
        - df_tratado["municipio_id_sus"].nunique()
        == 0
    )


def verifica_diferenca_ctg_validacao_tipo(
    df_extraido: pd.DataFrame, df_tratado: pd.DataFrame
) -> bool:
    """Verifica se há diferença na contagem única de tipo de validação"""
    return (
        df_extraido["Validação"].nunique()
        - df_tratado["validacao_nome"].nunique()
        == 0
    )


def verifica_diferenca_qtd_validacao_tipo(
    df_extraido: pd.DataFrame, df_tratado: pd.DataFrame
) -> bool:
    """Verifica se há diferença no contagem total de fichas"""
    return (
        df_extraido["Validação"].count() - df_tratado["validacao_nome"].count()
        == 0
    )


def verifica_nulos(df_tratado: pd.DataFrame) -> bool:
    """Verifica se 'Dataframe' possui algum valor ausente em qualquer coluna"""
    return (
        df_tratado[
            [
                "municipio_id_sus",
                "ficha",
                "aplicacao",
                "validacao_nome",
                "cnes_id",
            ]
        ]
        .isna()
        .sum()
        .sum()
        == 0
    )


@task(
    name="Validar Relatórios de Validação da Produção",
    description=(
        "Valida os dados dos relatórios de validação da produção extraídos "
        + "transformados a partir do portal público do Sistema de Informação "
        + "em Saúde para a Atenção Básica do SUS."
    ),
    tags=["aps", "sisab", "validacao_producao", "validacao"],
    retries=0,
    retry_delay_seconds=None,
)
def verificar_relatorio_validacao_producao(
    df_extraido: pd.DataFrame, df_tratado: pd.DataFrame
) -> None:
    """Testa a qualidade dos dados tratados do relatório de validação da produção.
        Argumentos:
            df_extraido: [`DataFrame`][] contendo os dados capturados no relatório de Indicadores do Sisab
             (conforme retornado pela função
                [`extrair_dados()`][]).
            df_tratado: [`DataFrame`][] contendo os dados tratados após captura no relatório de Indicadores do Sisab
             (conforme retornado pela função
                [`tratamento_dados()`][]).
    Exceções:
        Levanta um erro da classe [`AssertionError`][] quando uma das condições
        testadas não é considerada válida.
    [`AssertionError`]: https://docs.python.org/3/library/exceptions.html#AssertionError
    """
    habilitar_suporte_loguru()
    assert verifica_diferenca_ctg_municpios(df_extraido, df_tratado)
    assert verifica_diferenca_ctg_validacao_tipo(df_extraido, df_tratado)
    assert verifica_diferenca_qtd_validacao_tipo(df_extraido, df_tratado)
    assert verifica_nulos(df_tratado)
    logger.info(" Validação dos dados realizada...")
