# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Junta etapas do fluxo de ETL de indicadores de desempenho dos municípios."""

from datetime import date
from typing import Final

from prefect import flow
from sqlalchemy.orm import Session
import pandas as pd
import traceback

from impulsoetl import __VERSION__
from impulsoetl.loggers import habilitar_suporte_loguru, logger
from impulsoetl.sisab.excecoes import SisabExcecao
from impulsoetl.utilitarios.bd import carregar_dataframe
from impulsoetl.sisab.indicadores_municipios.extracao import (
    extrair_indicadores,
)
from impulsoetl.sisab.indicadores_municipios.tratamento import (
    transformar_indicadores,
)

INDICADORES_CODIGOS: Final[dict[str, str]] = {
    "Pré-Natal (6 consultas)": "1",
    "Pré-Natal (Sífilis e HIV)": "2",
    "Gestantes Saúde Bucal": "3",
    "Cobertura Citopatológico": "4",
    "Cobertura Polio e Penta": "5",
    "Hipertensão (PA Aferida)": "6",
    "Diabetes (Hemoglobina Glicada)": "7",
}


@flow(
    name="Obter Indicadores do Previne Brasil",
    description=(
        "Extrai, transforma e carrega os dados dos relatórios de indicadores "
        + "do Previne Brasil a partir do portal público do Sistema de "
        + "Informação em Saúde para a Atenção Básica do SUS."
    ),
    retries=0,
    retry_delay_seconds=None,
    version=__VERSION__,
    validate_parameters=False,
)
def obter_indicadores_desempenho(
    sessao: Session,
    visao_equipe: str,
    periodo_data: date,
    periodo_id: str,
    periodo_codigo: str,
    operacao_id: str,
    tabela_destino: str,
) -> None:
    habilitar_suporte_loguru()
    for indicador in INDICADORES_CODIGOS:
        try: 
            df_extraido = extrair_indicadores(
                sessao=sessao,
                visao_equipe=visao_equipe,
                periodo_data=periodo_data,
                indicador=indicador,
                periodo_id=periodo_id,
                operacao_id=operacao_id,
            )
            df_tratado = transformar_indicadores(
                sessao=sessao,
                df_extraido=df_extraido,
                periodo_data=periodo_data,
                indicador=indicador,
                periodo_id=periodo_id,
                periodo_codigo=periodo_codigo,
                operacao_id=operacao_id,
            )
            logger.info("Iniciando carga dos dados no banco...")
            print(df_tratado.head(5))
            carregar_dataframe(
                sessao=sessao, df=df_tratado, tabela_destino=tabela_destino
            )
            logger.info("Carga dos dados no banco realizada...")

        except pd.errors.ParserError:
            traceback_str = traceback.format_exc()
            enviar_erro = SisabExcecao("Competência indisponível no SISAB")
            enviar_erro.insere_erro_database(sessao=sessao,traceback_str=traceback_str,operacao_id=operacao_id,periodo_id=periodo_id)

            logger.error("Data da competência do relatório não está disponível")
            break
    
        except Exception as mensagem_erro:
            traceback_str = traceback.format_exc()
            enviar_erro = SisabExcecao(mensagem_erro)
            enviar_erro.insere_erro_database(sessao=sessao,traceback_str=traceback_str,operacao_id=operacao_id,periodo_id=periodo_id)

            logger.error(mensagem_erro)
            break