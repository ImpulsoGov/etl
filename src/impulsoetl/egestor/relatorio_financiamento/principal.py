# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""ETL do relatório de financiamento do eGestor por município."""

from datetime import date
from io import BytesIO
from typing import Final

import pandas as pd
from prefect import flow
from sqlalchemy.orm import Session
import traceback

from impulsoetl import __VERSION__
from impulsoetl.sisab.excecoes import SisabExcecao
from impulsoetl.utilitarios.bd import carregar_dataframe
from impulsoetl.egestor.relatorio_financiamento.extracao import extrair
from impulsoetl.egestor.relatorio_financiamento.tratamento import (
    tratamento_dados,
)
from impulsoetl.loggers import habilitar_suporte_loguru, logger

ABAS_NOMES: Final[dict[str, str]] = {
    "dados_publicos.egestor_financiamento_acoes_estrategicas_academia_saude": "Academia da Saúde",
    "dados_publicos.egestor_financiamento_acoes_estrategicas_outros": "Ações Estratégicas - Outros",
    "dados_publicos.egestor_financiamento_acoes_estrategicas_ribeirinha": "Ações Est. - Ribeirinha",
    "dados_publicos.egestor_financiamento_acoes_estrategicas_consultorio_rua": "Ações Est. - eCR",
    "dados_publicos.egestor_financiamento_acoes_estrategicas_residencia_profissiona": "Ações Est. - Residência",
    "dados_publicos.egestor_financiamento_acoes_estrategicas_saude_bucal": "Ações Est. - SB",
    "dados_publicos.egestor_financiamento_acoes_estrategicas_saude_hora": "Ações Est. - SNH",
    "dados_publicos.egestor_financiamento_acoes_estrategicas_ubs_fluvial": "Ações Est. - UBSF",
    "dados_publicos.egestor_financiamento_acoes_estrategicas_acs": "ACS",
    "dados_publicos.egestor_financiamento_acoes_estrategicas_informatiza_aps": "Informatização",
    "dados_publicos.egestor_financiamento_desempenho_isf": "Desempenho ISF",
    "dados_publicos.egestor_financiamento_capitacao_ponderada": "Capitação Ponderada",
}


@flow(
    name="Obter Relatórios de Financiamento",
    description=(
        "Extrai, transforma e carrega os dados dos relatórios de "
        + "financiamento do eGestor Atenção Básica."
    ),
    retries=0,
    retry_delay_seconds=None,
    version=__VERSION__,
    validate_parameters=False,
)
def obter_relatorio_financiamento(
    sessao: Session,
    periodo_id: str,
    tabela_destino: str,
    periodo_mes: date,
    operacao_id: str,
) -> None:
    """
    Extrai, transforma e carrega os dados do relatório de financiamento APS do egestor.
    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite acessar a base de dados da ImpulsoGov.
        periodo_id: Código de identificação do período .
        tabela_destino: Nome da tabela de destino a ser carregada com os dados extraidos e tratados.
        periodo_mes: Data do mês em referência.
    """
    habilitar_suporte_loguru()
    try: 
        arquivo = extrair(periodo_mes=periodo_mes)
        df_extraido = pd.read_excel(
            BytesIO(arquivo),
            sheet_name=ABAS_NOMES[tabela_destino],
            header=3,
            dtype="object",
        )

        df_tratado = tratamento_dados(
            sessao=sessao,
            df_extraido=df_extraido,
            aba=ABAS_NOMES[tabela_destino],
            periodo_data_inicio=periodo_mes,
            periodo_id=periodo_id,
        )

        carregar_dataframe(
        sessao=sessao, df=df_tratado, tabela_destino=tabela_destino
        )

            
    except pd.errors.ParserError:
        traceback_str = traceback.format_exc()
        enviar_erro = SisabExcecao("Competência indisponível no Egestor")
        enviar_erro.insere_erro_database(sessao=sessao,traceback_str=traceback_str,operacao_id=operacao_id,periodo_id=periodo_id)

        logger.error("Data da competência do relatório não está disponível")
        raise Exception("Interrompendo o bloco de código")
    
    except Exception as mensagem_erro:
        traceback_str = traceback.format_exc()
        enviar_erro = SisabExcecao(mensagem_erro)
        enviar_erro.insere_erro_database(sessao=sessao,traceback_str=traceback_str,operacao_id=operacao_id,periodo_id=periodo_id)

        logger.error(mensagem_erro)
        raise Exception("Interrompendo o bloco de código")
