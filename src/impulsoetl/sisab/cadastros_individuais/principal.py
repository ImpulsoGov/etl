# SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


from datetime import date
import time

from prefect import flow
from sqlalchemy.orm import Session

from impulsoetl import __VERSION__
from impulsoetl.loggers import habilitar_suporte_loguru, logger
from impulsoetl.sisab.cadastros_individuais.extracao import (
    extrair_cadastros_individuais,
)
from impulsoetl.sisab.cadastros_individuais.tratamento import tratar_dados
from impulsoetl.utilitarios.bd import carregar_dataframe


@flow(
    name="Obter Cadastros Individuais",
    description=(
        "Extrai, transforma e carrega os dados de cadastros individuais "
        + "do portal público do Sistema de Informação em Saúde para a Atenção "
        + "Básica do SUS."
    ),
    retries=0,
    retry_delay_seconds=None,
    version=__VERSION__,
    validate_parameters=False,
)
def obter_cadastros_individuais(
    sessao: Session,
    visao_equipe: str,
    periodo_data: date,
    periodo_id: str,
    periodo_codigo: str,
    tabela_destino: str,
    com_ponderacao: list[bool] = [False, True],
) -> None:
    """Extrai, transforma e carrega dados de cadastros de equipes pelo SISAB.

    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov.
        visao_equipe: Indica a situação da equipe considerada para a contagem
            dos cadastros.
        periodo: Referente ao mês/ano de disponibilização do relatório.
        com_ponderacao: Lista de booleanos indicando quais tipos de população
            devem ser filtradas no cadastro - onde `True` indica apenas as
            populações com critério de ponderação e `False` indica todos os
            cadastros. Por padrão, o valor é `[True, False]`, indicando que
            ambas as possibilidades são extraídas do SISAB e carregadas para a
            mesma tabela de destino.
        teste: Indica se as modificações devem ser de fato escritas no banco de
            dados (`False`, padrão). Caso seja `True`, as modificações são
            adicionadas à uma transação, e podem ser revertidas com uma chamada
            posterior ao método [`Session.rollback()`][] da sessão gerada com o
            SQLAlchemy.
    """
    habilitar_suporte_loguru()

    tempo_inicio_etl = time.time()
    for status_ponderacao in com_ponderacao:
        logger.info("Iniciando extração dos dados...")
        df_extraido = extrair_cadastros_individuais(
            visao_equipe=visao_equipe,
            com_ponderacao=status_ponderacao,
            competencia=periodo_data,
        )
        logger.info("Extração dos dados realizada...")

        logger.info("Iniciando tratamento dos dados...")
        df_tratado = tratar_dados(
            sessao=sessao,
            df_extraido=df_extraido,
            com_ponderacao=status_ponderacao,
            periodo_id=periodo_id,
            periodo_codigo=periodo_codigo,
        )
        logger.info("Tratamento dos dados realizada...")

        logger.info("Iniciando carga dos dados no banco...")
        carregar_dataframe(
            sessao=sessao, df=df_tratado, tabela_destino=tabela_destino
        )
        logger.info("Carga dos dados no banco realizada...")

    tempo_final_etl = time.time() - tempo_inicio_etl
    logger.info(
        "Terminou ETL para `{visao_equipe}` "
        + "da comepetência`{periodo_codigo}` "
        + "em {tempo_final_etl}.",
        tabela_nome=tabela_destino,
        periodo_codigo=periodo_codigo,
        tempo_final_etl=tempo_final_etl,
    )
