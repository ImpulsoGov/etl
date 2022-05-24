# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


import pandas as pd


def verifica_qtd_municipios(df: pd.DataFrame, df_tratado: pd.DataFrame) -> int:
    """Verifica se a quantidade de municípios é superior a 5000."""
    return df["ibge"].nunique()


def verifica_diferenca_ctg_municpios(
    df: pd.DataFrame, df_tratado: pd.DataFrame
) -> int:
    """Verifica se há diferença na contagem de municípios."""
    return df["ibge"].nunique() - df_tratado["municipio_id_sus"].nunique()


def verifica_diferenca_mun_betim(
    df: pd.DataFrame, df_tratado: pd.DataFrame
) -> int:
    """Verifica se há diferença parametro considerando apenas o município de Betim-MG."""
    return (
        df.query("ibge == '310670'")["numerador"].astype(int).sum()
        - df_tratado.query("municipio_id_sus == '310670'")["numerador"].sum()
    )


def verifica_qtd_uf(df: pd.DataFrame, df_tratado: pd.DataFrame) -> int:
    """Verifica se a quantidade de unidades federativas é igual a 26."""
    return df["uf"].nunique()


def verifica_diferenca_qtd_numerador(
    df: pd.DataFrame, df_tratado: pd.DataFrame
) -> int:
    """Verifica se há diferença no somatório de numerador."""
    return df["numerador"].astype(int).sum() - df_tratado["numerador"].sum()


def verifica_diferenca_qtd_denominador_informado(
    df: pd.DataFrame, df_tratado: pd.DataFrame
) -> int:
    """Verifica se há diferença no somatório de denominador informado."""
    return (
        df["denominador_informado"].astype(int).sum()
        - df_tratado["denominador_informado"].sum()
    )


def verifica_diferenca_qtd_denominador_estimado(
    df: pd.DataFrame, df_tratado: pd.DataFrame
) -> int:
    """Verifica se há diferença no somatório de denominador estimado."""
    return (
        df["denominador_estimado"].astype(int).sum()
        - df_tratado["denominador_estimado"].sum()
    )


def verifica_diferenca_qtd_nota(
    df: pd.DataFrame, df_tratado: pd.DataFrame
) -> int:
    """Verifica se há diferença no somatório de nota"""
    return df["nota"].astype(int).sum() - df_tratado["nota_porcentagem"].sum()


def verifica_validade_nota(df_tratado: pd.DataFrame):
    """Verifica se os valores de nota está entre 0 e 100"""
    for index, row in df_tratado.iterrows():
        return row["nota_porcentagem"] >= 0 and row["nota_porcentagem"] <= 100


def teste_validacao(
    df: pd.DataFrame, df_tratado: pd.DataFrame, indicador: str
):
    assert verifica_qtd_municipios(df, df_tratado) > 5000
    assert verifica_diferenca_ctg_municpios(df, df_tratado) == 0
    assert verifica_diferenca_mun_betim(df, df_tratado) == 0
    assert verifica_qtd_uf(df, df_tratado) >= 26
    assert verifica_diferenca_qtd_numerador(df, df_tratado) == 0
    assert verifica_diferenca_qtd_denominador_informado(df, df_tratado) == 0
    assert verifica_diferenca_qtd_denominador_estimado(df, df_tratado) == 0
    assert verifica_diferenca_qtd_nota(df, df_tratado) == 0
    assert verifica_validade_nota(df_tratado) is True
