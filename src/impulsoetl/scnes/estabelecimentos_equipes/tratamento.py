import warnings
warnings.filterwarnings("ignore")
import requests
import pandas as pd
import json
import numpy as np
from frozendict import frozendict
from typing import Final
from datetime import date
from sqlalchemy.orm import Session

from impulsoetl.bd import Sessao

from impulsoetl.scnes.extracao_lista_cnes import extrair_lista_cnes
from impulsoetl.scnes.estabelecimentos_equipes.extracao import extrair_equipes
from impulsoetl.loggers import logger

COLUNAS_EXCLUIR = [
    'coArea', 
    'coMunicipio',
    'dsArea', 
    'quilombola', 
    'assentada', 
    'geral', 
    'escola', 
    'pronasci',
    'indigena', 
    'ribeirinha', 
    'complem', 
    'seqEquipe',
    'tpEquipe'
]

COLUNAS_RENOMEAR: Final[dict[str, str]] = {
    'dsEquipe':'equipe_tipo',
    'coEquipe':'equipe_id_ine', 
    'nomeEquipe':'equipe_nome', 
    'dtAtivacao':'periodo_ativacao', 
    'dtDesativacao':'periodo_desativacao'
}

COLUNAS_TIPOS: Final[frozendict] = frozendict ({
    'municipio_id_sus':'str', 
    'estabelecimento_cnes_id':'str', 
    'equipe_id_ine':'str', 
    'equipe_tipo':'str', 
    'equipe_tipo':'str',
    'equipe_nome':'str', 
    'periodo_ativacao':'str', 
    'periodo_desativacao':'str'
}
)

COLUNAS_DATA = ['periodo_ativacao', 'periodo_desativacao']

def renomear_colunas(df_extraido: pd.DataFrame) -> pd.DataFrame:
    df_extraido.rename(columns=COLUNAS_RENOMEAR, inplace=True)
    return df_extraido


def excluir_colunas(df_extraido: pd.DataFrame) -> pd.DataFrame:
    df_extraido.drop(columns=COLUNAS_EXCLUIR, inplace=True)
    return df_extraido

def tratar_tipos(df_extraido: pd.DataFrame) -> pd.DataFrame:
    for coluna in COLUNAS_DATA:
        df_extraido[coluna] = pd.to_datetime(
            df_extraido[coluna], infer_datetime_format=True
        )

    df_extraido = df_extraido.astype(COLUNAS_TIPOS, errors="ignore").where(
        df_extraido.notna(), None
    )

    return df_extraido

def tratar_tipos(df_extraido: pd.DataFrame) -> pd.DataFrame:
    for coluna in COLUNAS_DATA:
        df_extraido[coluna] = pd.to_datetime(
            df_extraido[coluna], infer_datetime_format=True
        )

    df_extraido = df_extraido.astype(COLUNAS_TIPOS, errors="ignore").where(
        df_extraido.notna(), None
    )

    return df_extraido

def ordenar_colunas(df_extraido: pd.DataFrame, COLUNAS_TIPOS:dict):
    ordem_colunas = list(COLUNAS_TIPOS.keys())
    df_extraido = df_extraido[ordem_colunas]

    return df_extraido


def tratamento_dados(
    df_extraido: pd.DataFrame, periodo_id: str, unidade_geografica_id: str
) -> pd.DataFrame:

    logger.info("Iniciando o tratamento dos dados ...")

    df_extraido = excluir_colunas(df_extraido)
    df_extraido = renomear_colunas(df_extraido)
    df_extraido = ordenar_colunas(df_extraido, COLUNAS_TIPOS)
    df_extraido = tratar_tipos(df_extraido)
    df_extraido["periodo_id"] = periodo_id
    df_extraido["unidade_geografica_id"] = unidade_geografica_id
    df_extraido = df_extraido.reset_index(drop=True)

    logger.info("Dados transformados ...")

    return df_extraido


#codigo_municipio = '120025'
#periodo_id = '2023'
#unidade_geografica_id = 'brasil00000'
#lista_codigos = extrair_lista_cnes(codigo_municipio)
#df_extraido = extrair_equipes(codigo_municipio, lista_codigos)
#df_tratado = tratamento_dados(df_extraido, periodo_id, unidade_geografica_id)

#print(df_tratado)