"""Verifica a qualidade dos dados dos estabelecimentos identificados pós processamento"""
import warnings
warnings.filterwarnings("ignore")

import pandas as pd
import numpy as np

from impulsoetl.cnes.extracao_lista_cnes import extrair_lista_cnes
from impulsoetl.cnes.estabelecimentos_identificados.extracao import extrair_informacoes_estabelecimentos
from impulsoetl.cnes.estabelecimentos_identificados.tratamento import tratamento_dados

from impulsoetl.loggers import logger

#Verificar quantidade de colunas

coMun = '120001'

def verifica_diferenca_qtd_colunas(
    df_extraido:pd.DataFrame,
    df_tratado: pd.DataFrame,
)-> bool:
    """Verifica se há diferença na contagem de colunas"""
    colunas_df_extraido = df_extraido.shape[1]
    colunas_df_tratado = df_tratado.shape[1]
    return (
        colunas_df_extraido - colunas_df_tratado 
    ) == 0

def verifica_diferenca_qtd_registros(
    df_extraido:pd.DataFrame,
    df_tratado: pd.DataFrame,
)-> bool:
    """Verifica se há diferença na contagem de colunas"""
    return (
        df_extraido['estabelecimento_cnes_id'].count() - df_tratado['estabelecimento_cnes_id'].count() == 0
    )

def verificar_informacoes_estabelecimentos_identicados(
    df_extraido: pd.DataFrame,
    df_tratado: pd.DataFrame,
) -> None:
    logger.info("Iniciando a verificação dos dados ... ")
    assert verifica_diferenca_qtd_colunas(df_extraido,df_tratado)
    assert verifica_diferenca_qtd_registros(df_extraido,df_tratado)
    logger.info("Dados verificados corretamente")


#with Sessao() as sessao:
    #lista_cnes = extrair_lista_cnes(coMun)
    #df_extraido= extrair_informacoes_estabelecimentos(coMun,lista_cnes)
    #df_tratado = tratamento_dados(df_extraido, sessao)
    #print(" df_extraido: " + str(df_extraido['municipio_id_sus'].count()))
    #print(" df_tratado: " + str(df_tratado['municipio_id_sus'].count()))
    #verificar_informacoes_estabelecimentos_identicados(df_extraido, df_tratado)
    
