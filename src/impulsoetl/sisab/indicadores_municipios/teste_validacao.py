# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Verifica a qualidade dos dados de indicadores de desempenho."""


import pandas as pd
from loggers import logger

def verifica_qtd_municipios (df_extraido:pd.DataFrame) -> int:
	""" Verifica se a quantidade de municípios é superior a 5000 """
	return df_extraido['IBGE'].dropna().nunique()

def verifica_diferenca_ctg_municpios (df_extraido:pd.DataFrame,df_tratado:pd.DataFrame) -> int:
	""" Verifica se há diferença na contagem de municípios """
	return df_extraido['IBGE'].dropna().nunique() - df_tratado['municipio_id_sus'].nunique()

def verifica_diferenca_qtd_numerador (df_extraido:pd.DataFrame,df_tratado:pd.DataFrame) -> int:
	""" Verifica se há diferença no somatório de numerador """
	return df_extraido['Numerador'].dropna().astype(int).sum() - df_tratado['numerador'].sum()

def verifica_diferenca_qtd_denominador_informado (df_extraido:pd.DataFrame,df_tratado:pd.DataFrame) -> int:
	""" Verifica se há diferença no somatório de denominador informado """
	return df_extraido['Denominador Identificado'].dropna().astype(int).sum() - df_tratado['denominador_informado'].sum()

def verifica_diferenca_qtd_denominador_estimado (df_extraido:pd.DataFrame,df_tratado:pd.DataFrame) -> int:
	""" Verifica se há diferença no somatório de denominador estimado """
	return df_extraido['Denominador Estimado'].dropna().astype(int).sum() - df_tratado['denominador_estimado'].sum()

def verifica_diferenca_qtd_nota (df_extraido:pd.DataFrame,df_tratado:pd.DataFrame) -> int:
	""" Verifica se há diferença no somatório de nota"""
	return df_extraido['2022 Q1 (%)'].dropna().astype(int).sum() - df_tratado['nota_porcentagem'].sum()

def verifica_validade_nota (df_tratado:pd.DataFrame):
    """ Verifica se os valores de nota está entre 0 e 100"""
    for index, row in df_tratado.iterrows():
        return row["nota_porcentagem"] >= 0 and row["nota_porcentagem"] <= 100

def verifica_nulos (df_tratado:pd.DataFrame) -> int:
	""" Verifica se 'Dataframe' possui 13 algum valor ausente em qualquer coluna"""
	return df_tratado.isna().sum().sum()

def teste_validacao(df_extraido:pd.DataFrame,df_tratado:pd.DataFrame,indicador:str):
	assert verifica_qtd_municipios(df_extraido) > 5000
	assert verifica_diferenca_ctg_municpios(df_extraido,df_tratado) == 0
	assert verifica_diferenca_qtd_numerador(df_extraido,df_tratado) == 0
	assert verifica_diferenca_qtd_denominador_informado(df_extraido,df_tratado) == 0
	assert verifica_diferenca_qtd_denominador_estimado(df_extraido,df_tratado) == 0
	assert verifica_diferenca_qtd_nota(df_extraido,df_tratado) == 0
	assert verifica_validade_nota(df_tratado) is True
	assert verifica_nulos(df_tratado) == 0
	logger.info(" Validação dos dados realizada...")
    