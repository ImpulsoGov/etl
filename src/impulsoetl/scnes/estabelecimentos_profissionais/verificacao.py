import warnings
warnings.filterwarnings("ignore")
import pandas as pd
import numpy as np

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

def verificar_dados(
    df_extraido: pd.DataFrame,
    df_tratado: pd.DataFrame,
) -> None:
    
    logger.info("Iniciando a verificação dos dados ... ")
    assert verifica_diferenca_qtd_registros(df_extraido, df_tratado)
    logger.info("Dados verificados corretamente")

