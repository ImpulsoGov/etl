# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Verifica a qualidade dos dados de validação por produção."""


import pandas as pd
from impulsoetl.loggers import logger

def verifica_diferenca_ctg_municpios (
	df_extraido:pd.DataFrame,
	df_tratado:pd.DataFrame
	) -> int:
	""" Verifica se há diferença na contagem de municípios """
	return df_extraido['IBGE'].nunique() - df_tratado['municipio_id_sus'].nunique()


def verifica_diferenca_ctg_validacao_tipo (
	df_extraido:pd.DataFrame,
	df_tratado:pd.DataFrame
	) -> int:
	""" Verifica se há diferença na contagem única de tipo de validação """
	return df_extraido['Validação'].nunique() - df_tratado['validacao_nome'].nunique()

def verifica_diferenca_qtd_validacao_tipo (
	df_extraido:pd.DataFrame,
	df_tratado:pd.DataFrame
	) -> int:
	""" Verifica se há diferença no contagem total de fichas """
	return df_extraido['Validação'].count() - df_tratado['validacao_nome'].count()

def verifica_diferenca_qtd_validacoes_total (
	df_extraido:pd.DataFrame,
	df_tratado:pd.DataFrame
	) -> int:
	""" Verifica se há diferença no somatório de validações total """
	return df_extraido['Total'].astype(int).sum() - df_tratado['validacao_quantidade'].sum()

def verifica_nulos (
	df_tratado:pd.DataFrame
	) -> int:
	""" Verifica se 'Dataframe' possui algum valor ausente em qualquer coluna"""
	return df_tratado.isna().sum().sum()

def teste_validacao(df_extraido:pd.DataFrame,df_tratado:pd.DataFrame):
	assert verifica_diferenca_ctg_municpios(df_extraido,df_tratado) == 0
	assert verifica_diferenca_ctg_validacao_tipo(df_extraido,df_tratado) == 0
	assert verifica_diferenca_qtd_validacao_tipo(df_extraido,df_tratado) == 0
	assert verifica_diferenca_qtd_validacoes_total(df_extraido,df_tratado) == 0
	assert verifica_nulos(df_tratado) == 0
	logger.info(" Validação dos dados realizada...")
    