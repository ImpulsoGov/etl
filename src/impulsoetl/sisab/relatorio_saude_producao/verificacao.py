import pandas as pd
import numpy as np
import datetime
from datetime import date

from prefect import task
from impulsoetl.loggers import logger, habilitar_suporte_loguru


def verifica_existencia_nulos(
    df: pd.DataFrame,
) -> bool:
    """Verifica se há existencia de valores nulos no DataFrame"""
    return (
        not df.isnull().values.any()
    )

def verifica_existencia_valores_negativos_quantidade(
    df: pd.DataFrame,
) -> bool:
    """Verifica se há existencia de valores negativos no dataframe"""
    return (
        not (df['quantidade'] < 0).any().any()
        )

@task(
    name="Validar Relatório de Produção de Saúde ",
    description=(
        "Realizaça a verificalççao dos dados do relatório de Produção de Saúde extraído a partir da página do SISAB."
    ),
    tags=["sisab", "produção", "verificação"],
    retries=0,
    retry_delay_seconds=None,
)
def verificar_informacoes_relatorio_producao(
    df_tratado: pd.DataFrame,
) -> None:
    """
    Valida os dados extraídos do Relatório de Produção pós tratamento.
     Argumentos:
        df_extraido: df_extraido: [`DataFrame`][] contendo os dados extraídos no na página do CNES
            (conforme retornado pela função [`extrair_relatório()`][]).
        df_tratado: [`DataFrame`][] contendo os dados tratados
            (conforme retornado pela função [`tratamento_dados()`][]).
     Exceções:
        Levanta um erro da classe [`AssertionError`][] quando uma das condições testadas não é
        considerada válida.
    [`AssertionError`]: https://docs.python.org/3/library/exceptions.html#AssertionError
    """

    logger.info("Iniciando a verificação dos dados ... ")
    assert verifica_existencia_nulos(df_tratado)
    assert verifica_existencia_valores_negativos_quantidade(df_tratado)

    logger.info("Dados verificados corretamente")
 