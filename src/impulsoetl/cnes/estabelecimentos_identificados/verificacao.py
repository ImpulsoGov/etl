# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT

"""Verifica a qualidade dos dados dos estabelecimentos identificados pós processamento"""

import warnings

warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd
from prefect import task

from impulsoetl.cnes.estabelecimentos_identificados.extracao import (
    extrair_informacoes_estabelecimentos,
)
from impulsoetl.cnes.estabelecimentos_identificados.tratamento import (
    tratamento_dados,
)
from impulsoetl.cnes.extracao_lista_cnes import extrair_lista_cnes
from impulsoetl.loggers import habilitar_suporte_loguru, logger


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
    name="Validar dados dos Estabelecimentos Identificados",
    description=(
        "Realiza a validacao dos dados dos estabelecimentos de saúde "
        + "pós tratamento dos dados extraídos a partir da página do CNES"
    ),
    tags=["cnes", "estabelecimentos", "validacao"],
    retries=0,
    retry_delay_seconds=None,
)
def verificar_informacoes_estabelecimentos_identicados(
    df_extraido: pd.DataFrame,
    df_tratado: pd.DataFrame,
) -> None:
    """
    Valida os dados extraídos para o estabelecimentos de saúde pós tratamento.

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
