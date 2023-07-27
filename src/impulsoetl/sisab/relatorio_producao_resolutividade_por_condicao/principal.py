# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT

"""Junta etapas do fluxo de ETL dos dados de produção da APS a partir do SISAB."""

import warnings

warnings.filterwarnings("ignore")
import time
import traceback
from datetime import date

import pandas as pd
from prefect import flow
from sqlalchemy.orm import Session

from impulsoetl import __VERSION__
from impulsoetl.loggers import habilitar_suporte_loguru, logger
from impulsoetl.sisab.excecoes import SisabExcecao
from impulsoetl.sisab.relatorio_producao_resolutividade_por_condicao.carregamento import (
    carregar_dados,
)
from impulsoetl.sisab.relatorio_producao_resolutividade_por_condicao.extracao import (
    extrair_relatorio,
)
from impulsoetl.sisab.relatorio_producao_resolutividade_por_condicao.tratamento import (
    tratamento_dados,
)


@flow(
    name="Obter Relatório de Resolutividade da APS por Condição Avaliada",
    description=(
        "Extrai, transforma e carrega os dados de produção da Atenção Primária à Saúde "
        + "por problema/condição avaliada, a partir do Sistema de Informação em Saúde da Atenção "
        + "Básica do SUS."
    ),
    retries=0,
    retry_delay_seconds=None,
    timeout_seconds=14400,
    version=__VERSION__,
    validate_parameters=False,
)
def obter_relatorio_resolutividade_por_condicao(
    sessao: Session,
    tabela_destino: str,
    periodo_id: str,
    unidade_geografica_id: str,
    unidade_geografica_id_sus: str,
    periodo_competencia: date,
    teste: bool,
) -> None:

    tempo_inicio_etl = time.time()

    operacao_id = "0644acff-4642-75e1-b559-6193f928cb16"

    try:

        df_extraido = extrair_relatorio(
            periodo_competencia=periodo_competencia,
        )

        df_tratado = tratamento_dados(
            df_extraido=df_extraido,
            periodo_id=periodo_id,
            municipio_id_sus=unidade_geografica_id_sus,
            unidade_geografica_id=unidade_geografica_id,
        )

        carregar_dados(
            sessao=sessao,
            df_tratado=df_tratado,
            tabela_destino=tabela_destino,
            periodo_id=periodo_id,
            unidade_geografica_id=unidade_geografica_id,
        )

    except (KeyError, pd.errors.ParserError):
        traceback_str = traceback.format_exc()
        enviar_erro = SisabExcecao("Competência indisponível no SISAB")
        enviar_erro.insere_erro_database(
            sessao=sessao,
            traceback_str=traceback_str,
            operacao_id=operacao_id,
            periodo_id=periodo_id,
        )

        logger.error("Data da competência do relatório não está disponível")
        return 0

    except Exception as mensagem_erro:
        traceback_str = traceback.format_exc()
        enviar_erro = SisabExcecao(mensagem_erro)
        enviar_erro.insere_erro_database(
            sessao=sessao,
            traceback_str=traceback_str,
            operacao_id=operacao_id,
            periodo_id=periodo_id,
        )

        logger.error(mensagem_erro)
        return 0

    tempo_final_etl = time.time() - tempo_inicio_etl

    logger.info(
        "Terminou ETL para "
        + "da comepetência`{periodo_competencia}` "
        + "em {tempo_final_etl}.",
        periodo_competencia=periodo_competencia,
        tempo_final_etl=tempo_final_etl,
    )

    return df_tratado
