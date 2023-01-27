import warnings

warnings.filterwarnings("ignore")
import numpy as np
import pandas as pd
from prefect import task

from impulsoetl.loggers import habilitar_suporte_loguru, logger
from impulsoetl.scnes.estabelecimentos_equipes.extracao import extrair_equipes
from impulsoetl.scnes.estabelecimentos_profissionais.extracao import (
    extrair_profissionais,
)
from impulsoetl.scnes.estabelecimentos_profissionais.tratamento import (
    tratamento_dados,
)
from impulsoetl.scnes.extracao_lista_cnes import extrair_lista_cnes


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


@task(
    name="Validar dados dos Profissionais de Saúde",
    description=(
        "Realiza a validacao dos dados dos profissionais de saúde "
        + "pós tratamento dos dados extraídos a partir da página do CNES"
    ),
    tags=["cnes", "profissionais", "validacao"],
    retries=0,
    retry_delay_seconds=None,
)
def verificar_dados(
    df_extraido: pd.DataFrame,
    df_tratado: pd.DataFrame,
) -> None:
    """
    Valida os dados extraídos para os profissionais de saúde pós tratamento.
     Argumentos:
        df_extraido: df_extraido: [`DataFrame`][] contendo os dados extraídos no na página do CNES
            (conforme retornado pela função [`extrair_informacoes_estabelecimentos()`][]).
        df_tratado: [`DataFrame`][] contendo os dados tratados
            (conforme retornado pela função [`tratamento_dados()`][]).
     Exceções:
        Levanta um erro da classe [`AssertionError`][] quando uma das condições testadas não é
        considerada válida.
    [`AssertionError`]: https://docs.python.org/3/library/exceptions.html#AssertionError
    """

    habilitar_suporte_loguru()
    logger.info("Iniciando a verificação dos dados ... ")
    assert verifica_diferenca_qtd_registros(df_extraido, df_tratado)
    logger.info("Dados verificados corretamente")
