# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT

"""Extrai dados de produção da APS a partir do SISAB."""

import warnings

warnings.filterwarnings("ignore")

import pandas as pd

from datetime import date
from prefect import task

from impulsoetl.sisab.utilitarios_sisab_relatorio_producao import extrair_producao_por_municipio
from impulsoetl.sisab.utilitarios_sisab_relatorio_producao import transformar_producao_por_municipio


from impulsoetl.loggers import logger
"""
@task(
    name= "Extrair Dados de Produção da APS por Tipo de Condição Avaliada e Desfecho",
    description=(
        "Extrai os dados do relatório de produção da Atenção Primária à Saúde,"
        + "por problema/condição avaliada e desfecho a partir do portal público"
        + "do Sistema de Informação em Saúde para a Atenção Básica do SUS."
    ),
    tags=["aps", "sisab", "producao", "extracao"],
    retries=2,
    retry_delay_seconds=120,
)"""
def extrair_relatorio(
    periodo_competencia: date)-> pd.DataFrame():
    """
    Extrai relatório de produção por problema/condição avaliada e conduta a partir da página do SISAB

     Argumentos:
        periodo_data_inicio: Data da competência 
     Retorna:
        Objeto [`pandas.DataFrame`] com os dados extraídos.
    """
    df_consolidado = pd.DataFrame()
    
    try:
        df_parcial = extrair_producao_por_municipio(
            tipo_producao="Atendimento Individual",
            competencias=[periodo_competencia],
            selecoes_adicionais={
                "Problema/Condição Avaliada": "Selecionar Todos", 
                "Conduta":"Selecionar Todos",
            },
            ).pipe(transformar_producao_por_municipio)
        
        df_consolidado = df_consolidado.append(df_parcial)

    except Exception as e:
        logger.error(e)
        pass

    return df_consolidado

