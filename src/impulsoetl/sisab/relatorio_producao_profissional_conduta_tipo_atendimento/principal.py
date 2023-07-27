import warnings

warnings.filterwarnings("ignore")
import time
import traceback
from datetime import date

import pandas as pd
from prefect import flow
from sqlalchemy.orm import Session

from impulsoetl import __VERSION__
from impulsoetl.bd import Sessao
from impulsoetl.loggers import habilitar_suporte_loguru, logger
from impulsoetl.sisab.excecoes import SisabExcecao
from impulsoetl.sisab.relatorio_producao_profissional_conduta_tipo_atendimento.extracao import (
    extrair_relatorio,
)
from impulsoetl.sisab.relatorio_producao_profissional_conduta_tipo_atendimento.tratamento import (
    tratamento_dados,
)
from impulsoetl.utilitarios.bd import carregar_dataframe


@flow(
    name="Obter Relatório de Produção por Profissional, Contuta e Tipo de Atendimento (Painel AGP)",
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
def relatorio_profissional_conduta_atendimento(
    sessao: Session,
    tabela_destino: str,
    periodo_competencia: date,
    periodo_id: str,
    unidade_geografica_id: str,
) -> None:

    tempo_inicio_etl = time.time()

    operacao_id = "064540b9-78b9-766c-8130-cdc0f1ed5828"

    logger.info(
        "Extraindo relatório da competencia {}, ...".format(
            periodo_competencia
        )
    )

    try:

        df_extraido = extrair_relatorio(
            periodo_competencia=periodo_competencia
        )

        df_tratado = tratamento_dados(
            df_extraido=df_extraido,
            periodo_id=periodo_id,
            unidade_geografica_id=unidade_geografica_id,
        )

        carregar_dataframe(
            sessao=sessao, df=df_tratado, tabela_destino=tabela_destino
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
