# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Verifica a qualidade dos dados de indicadores de desempenho."""


import pandas as pd


def verificar_qtd_municipios(
    df: pd.DataFrame,
    df_tratado: pd.DataFrame,
) -> bool:
    """Verifica se a quantidade de municípios é superior a 5000."""
    return df["ibge"].nunique() > 5000


def verificar_diferenca_ctg_municpios(
    df: pd.DataFrame,
    df_tratado: pd.DataFrame,
) -> bool:
    """Verifica se há diferença na contagem de municípios."""
    return (
        df["ibge"].nunique() - df_tratado["municipio_id_sus"].nunique()
    ) == 0


def verificar_diferenca_mun_betim(
    df: pd.DataFrame,
    df_tratado: pd.DataFrame,
) -> bool:
    """Verifica se há diferença nos indicadores no município de Betim-MG."""
    return (
        df.query("ibge == '310670'")["numerador"].astype(int).sum()
        - df_tratado.query("municipio_id_sus == '310670'")["numerador"].sum()
    ) == 0


def verificar_qtd_uf(df: pd.DataFrame, df_tratado: pd.DataFrame) -> bool:
    """Verifica se a quantidade de unidades federativas é igual a 26."""
    return df["uf"].nunique() >= 26


def verificar_diferenca_qtd_numerador(
    df: pd.DataFrame,
    df_tratado: pd.DataFrame,
) -> bool:
    """Verifica se há diferença no somatório de numerador."""
    return (
        df["numerador"].astype(int).sum() - df_tratado["numerador"].sum()
    ) == 0


def verificar_diferenca_qtd_denominador_informado(
    df: pd.DataFrame,
    df_tratado: pd.DataFrame,
) -> bool:
    """Verifica se há diferença no somatório de denominador informado."""
    return (
        df["denominador_informado"].astype(int).sum()
        - df_tratado["denominador_informado"].sum()
    ) == 0


def verificar_diferenca_qtd_denominador_estimado(
    df: pd.DataFrame, df_tratado: pd.DataFrame
) -> bool:
    """Verifica se há diferença no somatório de denominador estimado."""
    return (
        df["denominador_estimado"].astype(int).sum()
        - df_tratado["denominador_estimado"].sum()
    ) == 0


def verificar_diferenca_qtd_nota(
    df: pd.DataFrame,
    df_tratado: pd.DataFrame,
) -> bool:
    """Verifica se há diferença no somatório de nota."""
    return (
        df["nota"].astype(int).sum() - df_tratado["nota_porcentagem"].sum()
    ) == 0


def verificar_validade_nota(df_tratado: pd.DataFrame):
    """Verifica se os valores de nota está entre 0 e 100."""
    return (
        (df_tratado["nota_porcentagem"].min() >= 0)
        and (df_tratado["nota_porcentagem"].max() <= 100)
    )


def verificar_indicadores_municipios(
    df: pd.DataFrame,
    df_tratado: pd.DataFrame,
) -> None:
    """Testa qualidade dos dados transformados de indicadores municipais.

    Exceções:
        Levanta um erro da classe [`AssertionError`][] quando uma das condições
        testadas não é considerada válida.

    [`AssertionError`]: https://docs.python.org/3/library/exceptions.html#AssertionError
    """
    assert verificar_qtd_municipios(df, df_tratado)
    assert verificar_diferenca_ctg_municpios(df, df_tratado)
    assert verificar_diferenca_mun_betim(df, df_tratado)
    assert verificar_qtd_uf(df, df_tratado)
    assert verificar_diferenca_qtd_numerador(df, df_tratado)
    assert verificar_diferenca_qtd_denominador_informado(df, df_tratado)
    assert verificar_diferenca_qtd_denominador_estimado(df, df_tratado)
    assert verificar_diferenca_qtd_nota(df, df_tratado)
    assert verificar_validade_nota(df_tratado)
