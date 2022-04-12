# SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


import pandas as pd

""" 
Amostra 01 : Verifica se a quantidade de municípios é superior a 5000
Amostra 02: Verifica se há diferença na contagem de municípios 
Amostra 03: Verifica se há diferença no somatório de cadastros de equipes considerando apenas o município de Betim-MG
Amostra 04: Verifica se  se a quantidade de unidades federativas é superior a 5000
Amostra 05: Verifica se há diferença na soma de cadastros 
Amostra 06: Verifica se há diferença na contagem de estabelecimentos 
Amostra 07: Verifica se há diferença na contagem de equipes 

"""


def amostra_01(df: pd.DataFrame, df_tratado: pd.DataFrame) -> int:
    return df["IBGE"].nunique()


def amostra_02(df: pd.DataFrame, df_tratado: pd.DataFrame) -> int:
    return df["IBGE"].nunique() - df_tratado["municipio_id_sus"].nunique()


def amostra_03(df: pd.DataFrame, df_tratado: pd.DataFrame) -> int:
    return (
        df.query("IBGE == '310670'")["quantidade"].astype(int).sum()
        - df_tratado.query("municipio_id_sus == '310670'")["quantidade"].sum()
    )


def amostra_04(df: pd.DataFrame, df_tratado: pd.DataFrame) -> int:
    return df_tratado["unidade_geografica_id"].nunique()


def amostra_05(df: pd.DataFrame, df_tratado: pd.DataFrame) -> int:
    return df["quantidade"].astype(int).sum() - df_tratado["quantidade"].sum()


def amostra_06(df: pd.DataFrame, df_tratado: pd.DataFrame) -> int:
    return df_tratado["cnes_id"].nunique() - df_tratado["cnes_id"].nunique()


def amostra_07(df: pd.DataFrame, df_tratado: pd.DataFrame) -> int:
    return df_tratado["ine_id"].nunique() - df_tratado["ine_id"].nunique()


def teste_validacao(df, df_tratado):
    assert amostra_01(df, df_tratado) > 5000
    assert amostra_02(df, df_tratado) == 0
    assert amostra_03(df, df_tratado) == 0
    assert amostra_04(df, df_tratado) >= 26
    assert amostra_05(df, df_tratado) == 0
    assert amostra_06(df, df_tratado) == 0
    assert amostra_07(df, df_tratado) == 0
