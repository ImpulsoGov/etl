# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT

"""Trata os dados de produção da APS extraídos a partir do SISAB."""

import warnings

warnings.filterwarnings("ignore")

import pandas as pd
import numpy as np

from datetime import date
from typing import Final
from frozendict import frozendict
from prefect import task

from impulsoetl.loggers import logger, habilitar_suporte_loguru


COLUNAS_RENOMEAR: Final[dict[str, str]] = {
    "Conduta":"conduta",
    "Problema/Condição Avaliada":"problema_condicao_avaliada",
    "quantidade_aprovada":"quantidade_registrada"
}

COLUNAS_EXCLUIR = [
    'uf_sigla', 
    'municipio_id_sus', 
    'municipio_nome', 
    'periodo_data_inicio', 
]

COLUNAS_TIPOS: Final[frozendict] = frozendict(
    {
    "conduta":"str",
    "problema_condicao_avaliada":"str",
    "quantidade_registrada":"int",
    "periodo_id":"str",
    "unidade_geografica_id":"str",
    "tipo_producao":"str"
    }
)

def renomear_colunas(df_extraido: pd.DataFrame) -> pd.DataFrame:
    df_extraido.rename(columns=COLUNAS_RENOMEAR, inplace=True)
    return df_extraido


def excluir_colunas(df_extraido: pd.DataFrame) -> pd.DataFrame:
    df_extraido.drop(columns=COLUNAS_EXCLUIR, inplace=True)
    return df_extraido

def tratar_tipos(df_extraido: pd.DataFrame) -> pd.DataFrame:
    df_extraido = df_extraido.astype(COLUNAS_TIPOS, errors="ignore").where(
        df_extraido.notna(), None
    )
    return df_extraido

"""
@task(
    name= "Trata os Dados de Produção da APS extraídos por problema/condição avaliada e conduta",
    description=(
        "Realiza o tratamento dos dados do relatório de produção da Atenção Primária à Saúde,"
        + "obtidos partir do portal público do Sistema de Informação em Saúde para a Atenção Básica do SUS."
    ),
    tags=["aps", "sisab", "producao", "tratamento"],
    retries=2,
    retry_delay_seconds=120,
)"""
def tratamento_dados(
    df_extraido: pd.DataFrame, municipio_id_sus:str, periodo_id: str, unidade_geografica_id: str
) -> pd.DataFrame:
    
    habilitar_suporte_loguru()
    logger.info("Iniciando o tratamento dos dados...")

    df_extraido = df_extraido.loc[df_extraido['municipio_id_sus']==municipio_id_sus].reset_index()
    df_extraido = excluir_colunas(df_extraido)
    df_extraido = renomear_colunas(df_extraido)
    df_extraido['periodo_id'] = periodo_id
    df_extraido['unidade_geografica_id'] = unidade_geografica_id
    df_extraido['tipo_producao']='Atendimento Individual'
    tratar_tipos(df_extraido)    

    logger.info("Dados tratados com sucesso ...")

    return df_extraido