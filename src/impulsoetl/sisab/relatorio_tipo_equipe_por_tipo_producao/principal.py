# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT

"""Junta etapas do fluxo de ETL dos dados de produção da APS a partir do SISAB."""

import warnings

warnings.filterwarnings("ignore")
from datetime import date

from prefect import flow
from sqlalchemy.orm import Session

from impulsoetl import __VERSION__

from impulsoetl.sisab.relatorio_tipo_equipe_por_tipo_producao.extracao import extrair_relatorio
from impulsoetl.sisab.relatorio_tipo_equipe_por_tipo_producao.tratamento import tratamento_dados
from impulsoetl.sisab.relatorio_tipo_equipe_por_tipo_producao.carregamento import carregar_dados

from impulsoetl.utilitarios.bd import carregar_dataframe



@flow(
    name="Obter Relatório de Produção por Tipo de Equipe da APS",
    description=(
        "Extrai, transforma e carrega os dados de produção da Atenção Primária à Saúde,"
        + "por tipo da produção realizada e por tipo de equipe a partir do portal público"
        + "do Sistema de Informação em Saúde para a Atenção Básica do SUS."
    ),
    retries=0,
    retry_delay_seconds=None,
    timeout_seconds=14400,
    version=__VERSION__,
    validate_parameters=False,
)
def obter_relatorio_tipo_equipe_por_producao(
    sessao: Session,
    tabela_destino: str,
    periodo_id: str,
    unidade_geografica_id: str,
    unidade_geografica_id_sus: str,
    periodo_competencia: date,
) -> None:

    df_extraido = extrair_relatorio(
        periodo_competencia=periodo_competencia,
    )

    df_tratado = tratamento_dados(
        df_extraido=df_extraido,
        periodo_id=periodo_id,
        municipio_id_sus = unidade_geografica_id_sus,
        unidade_geografica_id=unidade_geografica_id
    )

    carregar_dados(
        sessao=sessao, 
        df_tratado=df_tratado, 
        tabela_destino=tabela_destino, 
        periodo_id = periodo_id, 
        unidade_geografica_id = unidade_geografica_id
    )

    return df_tratado