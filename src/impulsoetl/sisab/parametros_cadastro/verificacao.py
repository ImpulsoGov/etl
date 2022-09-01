# SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


import pandas as pd


def verificar_qtd_municipios(
    df: pd.DataFrame,
    df_tratado: pd.DataFrame,
) -> bool:
    """Verifica se a quantidade de municípios é superior a 5000."""
    return df["IBGE"].nunique() > 5000


def verificar_diferenca_ctg_municpios(
    df: pd.DataFrame,
    df_tratado: pd.DataFrame,
) -> bool:
    """Verifica se há diferença na contagem de municípios."""
    return (
        df["IBGE"].nunique() - df_tratado["municipio_id_sus"].nunique()
    ) == 0


def verificar_diferenca_mun_betim(
    df: pd.DataFrame,
    df_tratado: pd.DataFrame,
) -> bool:
    """Verifica se há diferença nos parâmetros considerando apenas o município de Betim-MG"""
    return (
        df.query("IBGE == '310670'")["parametro"].astype(int).sum()
        - df_tratado.query("municipio_id_sus == '310670'")["parametro"].sum()
    ) == 0


def verificar_diferenca_qtd_parametro(
    df: pd.DataFrame,
    df_tratado: pd.DataFrame,
) -> bool:
    """Verifica se há diferença no somatório de parâmetros"""
    return (
        df["parametro"].astype(int).sum() - df_tratado["parametro"].sum()
    ) == 0


def verificar_diferenca_ctg_cnes(
    df: pd.DataFrame,
    df_tratado: pd.DataFrame,
) -> bool:
    """Verifica se há diferença na contagem de estabelecimentos"""
    return (df["CNES"].nunique() - df_tratado["cnes_id"].nunique()) == 0


def verificar_diferenca_ctg_ine(
    df: pd.DataFrame,
    df_tratado: pd.DataFrame,
) -> bool:
    """Verifica se há diferença na contagem de equipes"""
    return (df["INE"].nunique() - df_tratado["ine_id"].nunique()) == 0


def verificar_parametros_cadastro(
    df: pd.DataFrame,
    df_tratado: pd.DataFrame,
    nivel_agregacao: str,
) -> None:
    """Testa a qualidade dos dados tratados de parâmetros de cadastro.

    Exceções:
        Levanta um erro da classe [`AssertionError`][] quando uma das condições
        testadas não é considerada válida.

    [`AssertionError`]: https://docs.python.org/3/library/exceptions.html#AssertionError
    """
    assert verificar_qtd_municipios(df, df_tratado) > 5000
    assert verificar_diferenca_ctg_municpios(df, df_tratado) == 0
    assert verificar_diferenca_mun_betim(df, df_tratado) == 0
    assert verificar_diferenca_qtd_parametro(df, df_tratado) == 0
    if nivel_agregacao == "estabelecimentos_equipes":
        assert verificar_diferenca_ctg_cnes(df, df_tratado) == 0
        assert verificar_diferenca_ctg_ine(df, df_tratado) == 0
