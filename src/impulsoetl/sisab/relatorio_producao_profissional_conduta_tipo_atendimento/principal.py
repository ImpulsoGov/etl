import warnings
warnings.filterwarnings("ignore")

import pandas as pd
from datetime import date

from sqlalchemy.orm import Session
from prefect import flow 

from impulsoetl import __VERSION__
from impulsoetl.bd import Sessao

from impulsoetl.loggers import logger, habilitar_suporte_loguru
from impulsoetl.sisab.relatorio_producao_profissional_conduta_tipo_atendimento.extracao import extrair_relatorio
from impulsoetl.sisab.relatorio_producao_profissional_conduta_tipo_atendimento.tratamento import tratamento_dados
from impulsoetl.utilitarios.bd import carregar_dataframe


@flow(
    name="Obter Relatório de Produção por Profissional, Contuta e Tipo de Atendimento (Painel AGP)",
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
def relatorio_profissional_conduta_atendimento(
    sessao: Session,
    tabela_destino: str,
    periodo_competencia: date,
    periodo_id: str,
    unidade_geografica_id: str
)-> None:

    logger.info("Extraindo relatório da competencia {}, ...".format(periodo_competencia))

    df_extraido = extrair_relatorio(
        periodo_competencia = periodo_competencia
    )
    
    df_tratado = tratamento_dados(
        df_extraido=df_extraido,
        periodo_id=periodo_id,
        unidade_geografica_id=unidade_geografica_id,
    )

    carregar_dataframe(
        sessao=sessao, df=df_tratado, tabela_destino=tabela_destino
    )
