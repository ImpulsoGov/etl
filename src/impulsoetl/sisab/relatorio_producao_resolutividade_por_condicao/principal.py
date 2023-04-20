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

from impulsoetl.sisab.relatorio_producao_resolutividade_por_condicao.extracao import extrair_relatorio
from impulsoetl.sisab.relatorio_producao_resolutividade_por_condicao.tratamento import tratamento_dados
from impulsoetl.utilitarios.bd import carregar_dataframe


@flow(
    name="Obter Relatório de Resolutividade da APS por Condição Avaliada",
    description=(
        "Extrai, transforma e carrega os dados de produção da Atenção Primária à Saúde "
        +"por problema/condição avaliada, a partir do Sistema de Informação em Saúde da Atenção "
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

    df_extraido = extrair_relatorio(
        periodo_competencia=periodo_competencia,
    )

    df_tratado = tratamento_dados(
        df_extraido=df_extraido,
        periodo_id=periodo_id,
        municipio_id_sus = unidade_geografica_id_sus,
        unidade_geografica_id=unidade_geografica_id
    )

    carregar_dataframe(
        sessao=sessao, df=df_tratado, tabela_destino=tabela_destino, teste = teste
    )

    return df_tratado