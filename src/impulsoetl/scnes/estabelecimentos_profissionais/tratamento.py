import warnings
warnings.filterwarnings("ignore")
import requests
import pandas as pd
import json
import numpy as np
from frozendict import frozendict
from typing import Final
from datetime import date

from impulsoetl.scnes.extracao_lista_cnes import extrair_lista_cnes
from impulsoetl.scnes.estabelecimentos_equipes.extracao import extrair_equipes
from impulsoetl.scnes.estabelecimentos_profissionais.extracao import extrair_profissionais
#from impulsoetl.loggers import logger


COLUNAS_EXCLUIR = [
    'tpSusNaoSus',
    'artigo2', 
    'artigo3', 
    'artigo5',
    'coArea_INE',
    'cbo_INE',
    'dsCbo_INE',
    'chOutros_INE',
    'chAmb_INE', 
    'chHosp_INE',
    'noProfissional_INE',
    'stEquipeMinima_INE',
    'diferenciada_INE',
    'complementar_INE',
    'dtEntrada',
    'cnsMaster'
]

COLUNAS_RENOMEAR: Final[dict[str, str]] = {
    'INE':'equipe_id_ine',
    'nome':'profissional_nome',
    'cns':'profissional_cns', 
    'cbo':'profissional_cbo',
    'dsCbo':'profissional_ocupacao', 
    'vinculacao':'profissional_vinculacao', 
    'vinculo':'profissional_vinculo_tipo',
    'subVinculo':'profissional_vinculo_subptipo', 
    'chHosp':'carga_horaria_hospitalar',
    'chAmb':'carga_horaria_ambulatorial', 
    'chOutros':'carga_horaria_outras', 
    'dtEntrada_INE':'periodo_data_entrada', 
    'dtDesligamento_INE':'periodo_data_desligamento',
}

COLUNAS_TIPOS: Final[frozendict] = frozendict(
    {
    'municipio_id_sus':'str',
    'estabelecimento_cnes_id':'str',
    'equipe_id_ine':'str',
    'profissional_nome':'str',
    'profissional_cns':'str', 
    'profissional_cbo':'str',
    'profissional_ocupacao':'str', 
    'profissional_vinculacao':'str', 
    'profissional_vinculo_tipo':'str',
    'profissional_vinculo_subptipo':'str', 
    'carga_horaria_hospitalar':'int',
    'carga_horaria_ambulatorial':'int', 
    'carga_horaria_outras':'int', 
    'periodo_data_entrada':'int', 
    'periodo_data_desligamento':'int',
    }
)

COLUNAS_DATA = ['periodo_data_entrada','periodo_data_desligamento']


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

    df_extraido = extrair_profissionais(codigo_municipio, lista_codigos)
    df_extraido = excluir_colunas(df_extraido)
    df_extraido = renomear_colunas(df_extraido)
    df_extraido = ordenar_colunas(df_extraido, COLUNAS_TIPOS)
    df_extraido = tratar_tipos(df_extraido)
    df_extraido["periodo_id"] = periodo_id
    df_extraido["unidade_geografica_id"] = unidade_geografica_id
    df_extraido = df_extraido.reset_index(drop=True)

    return df_extraido


codigo_municipio = '120025'
periodo_id = '2023'
unidade_geografica_id = 'brasil00000'
lista_codigos = extrair_lista_cnes(codigo_municipio)
df_extraido = extrair_profissionais(codigo_municipio, lista_codigos)
df_tratado = tratamento_dados(df_extraido, periodo_id, unidade_geografica_id)


#print(df_tratado)

