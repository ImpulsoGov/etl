import warnings
warnings.filterwarnings("ignore")
import pandas as pd
import numpy as np
from frozendict import frozendict
from typing import Final
from datetime import date

from impulsoetl.scnes.extracao_lista_cnes import extrair_lista_cnes
from impulsoetl.scnes.estabelecimentos_equipes.extracao import extrair_equipes
from impulsoetl.scnes.estabelecimentos_profissionais.extracao import extrair_profissionais
from impulsoetl.scnes.estabelecimentos_profissionais.tratamento import tratamento_dados

from impulsoetl.loggers import logger


def verifica_diferenca_qtd_registros(
    df_extraido: pd.DataFrame,
    df_tratado: pd.DataFrame,
) -> bool:
    """Verifica se há diferença na contagem de registros"""
    return (
        df_extraido["estabelecimento_cnes_id"].count()
        - df_tratado["estabelecimento_cnes_id"].count()
        == 0
    )

def verifica_profissionais_duplicados (
    df_extraido: pd.DataFrame,
) ->bool:
    """Verifica se há numero de cns duplicados"""
    return (
        df_tratado['profissional_cns'].is_unique
    )



def verificar_informacoes_estabelecimentos_identicados(
    df_extraido: pd.DataFrame,
    df_tratado: pd.DataFrame,
) -> None:
    
    logger.info("Iniciando a verificação dos dados ... ")
    assert verifica_diferenca_qtd_registros(df_extraido, df_tratado)
    assert verifica_profissionais_duplicados(df_tratado)
    logger.info("Dados verificados corretamente")





#codigo_municipio = '120025'
#periodo_id = '2023'
#unidade_geografica_id = 'brasil00000'
#lista_codigos = extrair_lista_cnes(codigo_municipio)
#df_extraido = extrair_profissionais(codigo_municipio, lista_codigos)
#df_tratado = tratamento_dados(df_extraido, periodo_id, unidade_geografica_id)
#df_verificado = verificar_informacoes_estabelecimentos_identicados(df_extraido,df_tratado)
