# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Define checagens da qualidade dos dados de parâmetros de cadastro."""


import pandas as pd


def verifica_qtd_municipios(df: pd.DataFrame, df_tratado: pd.DataFrame) -> int:
    """Verifica se a quantidade de municípios é superior a 5000."""
    return df["IBGE"].nunique()


def verifica_diferenca_ctg_municpios(
    df: pd.DataFrame, df_tratado: pd.DataFrame
) -> int:
    """Verifica se há diferença na contagem de municípios."""
    return df["IBGE"].nunique() - df_tratado["municipio_id_sus"].nunique()


def verifica_diferenca_mun_betim(
    df: pd.DataFrame, df_tratado: pd.DataFrame
) -> int:
    """Verifica se há diferença parametro considerando apenas Betim-MG."""
    return (
        df.query("IBGE == '310670'")["parametro"].astype(int).sum()
        - df_tratado.query("municipio_id_sus == '310670'")["parametro"].sum()
    )


def verifica_qtd_uf(df: pd.DataFrame, df_tratado: pd.DataFrame) -> int:
    """Verifica se a quantidade de unidades federativas é igual a 26."""
    return df_tratado["unidade_geografica_id"].nunique()


def verifica_diferenca_qtd_parametro(
    df: pd.DataFrame, df_tratado: pd.DataFrame
) -> int:
    """Verifica se há diferença no somatório de parâmetros."""
    return df["parametro"].astype(int).sum() - df_tratado["parametro"].sum()


def verifica_diferenca_ctg_cnes(
    df: pd.DataFrame, df_tratado: pd.DataFrame
) -> int:
    """Verifica se há diferença na contagem de estabelecimentos."""
    return df["CNES"].nunique() - df_tratado["cnes_id"].nunique()


def verifica_diferenca_ctg_ine(
    df: pd.DataFrame, df_tratado: pd.DataFrame
) -> int:
    """Verifica se há diferença na contagem de equipes."""
    return df["INE"].nunique() - df_tratado["ine_id"].nunique()


def teste_validacao(
    df: pd.DataFrame, df_tratado: pd.DataFrame, nivel_agregacao: str
):
	"""Aplica testes de qualidade nos parâmetros de cadastro.
	
	Argumentos:
		df: objeto `pandas.DataFrame` com os dados originais do SISAB, antes de
			qualquer processamento.
		df_tratado: objeto `pandas.DataFrame` que passou por preparação dos
			dados para inserção no banco de dados da ImpulsoGov.
		nivel_agregacao: Granularidade dos parâmetros de cadastro obtidos do
			SISAB. Deve ser uma opção entre `'estabelecimentos_equipes'` ou
			`'municipios'`.
	"""
    assert verifica_qtd_municipios(df, df_tratado) > 5000
    assert verifica_diferenca_ctg_municpios(df, df_tratado) == 0
    assert verifica_diferenca_mun_betim(df, df_tratado) == 0
    assert verifica_qtd_uf(df, df_tratado) >= 26
    assert verifica_diferenca_qtd_parametro(df, df_tratado) == 0
    if nivel_agregacao == "estabelecimentos_equipes":
        assert verifica_diferenca_ctg_cnes(df, df_tratado) == 0
        assert verifica_diferenca_ctg_ine(df, df_tratado) == 0
