import pandas as pd
import numpy as np
import datetime
from datetime import date

from impulsoetl.loggers import logger


def verifica_existencia_nulos(
    df: pd.DataFrame,
) -> bool:
    """Verifica se há existencia de valores nulos no DataFrame"""
    return (
        df.isnull().values.any()
    )

def verifica_existencia_valores_negativos_quantidade(
    df: pd.DataFrame,
) -> bool:
    """Verifica se há existencia de valores negativos no dataframe"""
    return (
        not (df['quantidade'] < 0).any().any()
        )

def verificar_informacoes_relatorio_producao(
    df_tratado: pd.DataFrame,
) -> None:
    logger.info("Iniciando a verificação dos dados ... ")
    assert verifica_existencia_nulos(df_tratado)
    assert verifica_existencia_valores_negativos_quantidade(df_tratado)

    logger.info("Dados verificados corretamente")
 