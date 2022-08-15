# SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


import pandas as pd


def verifica_qtd_municipios(df: pd.DataFrame, df_tratado: pd.DataFrame) -> int:
    """verifica_qtd_municipios: Verifica se a quantidade de municípios é superior a 5000"""
    return df["IBGE"].nunique()


def verifica_diferenca_ctg_municpios(
    df: pd.DataFrame, df_tratado: pd.DataFrame
) -> int:
    """verifica_diferenca_ctg_municpios: Verifica se há diferença na contagem de municípios"""
    return df["IBGE"].nunique() - df_tratado["municipio_id_sus"].nunique()


def verifica_diferenca_mun_betim(
    df: pd.DataFrame, df_tratado: pd.DataFrame
) -> int:
    """verifica_diferenca_mun_betim: Verifica se há diferença parametro considerando apenas o município de Betim-MG"""
    return (
        df.query("IBGE == '310670'")["parametro"].astype(int).sum()
        - df_tratado.query("municipio_id_sus == '310670'")["parametro"].sum()
    )


def verifica_diferenca_qtd_parametro(
    df: pd.DataFrame, df_tratado: pd.DataFrame
) -> int:
    """verifica_diferenca_qtd_parametro: Verifica se há diferença no somatório de parâmetros"""
    return df["parametro"].astype(int).sum() - df_tratado["parametro"].sum()


def verifica_diferenca_ctg_cnes(
    df: pd.DataFrame, df_tratado: pd.DataFrame
) -> int:
    """verifica_diferenca_ctg_cnes: Verifica se há diferença na contagem de estabelecimentos"""
    return df["CNES"].nunique() - df_tratado["cnes_id"].nunique()


def verifica_diferenca_ctg_ine(
    df: pd.DataFrame, df_tratado: pd.DataFrame
) -> int:
    """verifica_diferenca_ctg_ine: Verifica se há diferença na contagem de equipes"""
    return df["INE"].nunique() - df_tratado["ine_id"].nunique()


def teste_validacao(
    df: pd.DataFrame, df_tratado: pd.DataFrame, nivel_agregacao: str
):
    assert verifica_qtd_municipios(df, df_tratado) > 5000
    assert verifica_diferenca_ctg_municpios(df, df_tratado) == 0
    assert verifica_diferenca_mun_betim(df, df_tratado) == 0
    assert verifica_diferenca_qtd_parametro(df, df_tratado) == 0
    if nivel_agregacao == "estabelecimentos_equipes":
        assert verifica_diferenca_ctg_cnes(df, df_tratado) == 0
        assert verifica_diferenca_ctg_ine(df, df_tratado) == 0
