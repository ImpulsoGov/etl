# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Junta etapas do fluxo de ETL do relatório de financiamento do Egestor por aplicação dos municípios."""

from __future__ import annotations

from datetime import date
from sqlite3 import Date
from typing import Final
import pandas as pd
from requests import session
from sqlalchemy.orm import Session

import sys
sys.path.append(r'C:\Users\maira\Impulso\etl\src\impulsoetl')

from egestor.relatorio_financiamento.extracao import extracao
from egestor.relatorio_financiamento.tratamento import tratamento_dados 
from egestor.relatorio_financiamento.verificacao import verificar_relatorio_egestor 
from egestor.relatorio_financiamento.carregamento import carregar_dados 
from bd import Sessao
from loggers import logger


ABAS_NOMES : Final[dict[str, str]] = {
    "dados_publicos.egestor_financiamento_acoes_estrategicas_academia_saude":"Academia da Saúde",
    "dados_publicos.egestor_financiamento_acoes_estrategicas_outros":"Ações Estratégicas - Outros",
    "dados_publicos.egestor_financiamento_acoes_estrategicas_ribeirinha":"Ações Est. - Ribeirinha",
    "dados_publicos.egestor_financiamento_acoes_estrategicas_consultorio_rua":"Ações Est. - eCR",
    "dados_publicos.egestor_financiamento_acoes_estrategicas_residencia_profissiona":"Ações Est. - Residência",
    "dados_publicos.egestor_financiamento_acoes_estrategicas_saude_bucal":"Ações Est. - SB",#
    "dados_publicos.egestor_financiamento_acoes_estrategicas_saude_hora":"Ações Est. - SNH",
    "dados_publicos.egestor_financiamento_acoes_estrategicas_ubs_fluvial":"Ações Est. - UBSF",
    "dados_publicos.egestor_financiamento_acoes_estrategicas_acs":"ACS",
    "dados_publicos.egestor_financiamento_acoes_estrategicas_informatiza_aps":"Informatização",
    "dados_publicos.egestor_financiamento_desempenho_isf":"Desempenho ISF",
    }

def obter_relatorio_financiamento(
        sessao: Session,
        periodo_competencia: str,
        periodo_id: str,
        tabela_destino: str,
        periodo_mes:date
    ) -> None:

        # substituir por função/método de extração dos dados
        arquivo_excel = extracao(periodo_mes=periodo_mes)
        df_extraido = pd.read_excel(arquivo_excel, sheet_name=ABAS_NOMES[tabela_destino] , header=3, dtype="object") 
        ####

        df_tratado=tratamento_dados(
            sessao=sessao,
            df_extraido=df_extraido,
            aba=ABAS_NOMES[tabela_destino], # testar com abas
            periodo_competencia=periodo_competencia,
            periodo_id=periodo_id
            )
            
        # adicionar mais checagem 
        verificar_relatorio_egestor(
           df_extraido=df_extraido,
            df_tratado=df_tratado,
        )
        ###

        carregar_dados(
            sessao=sessao,
            df_tratado=df_tratado,
            tabela_destino=tabela_destino
        )
        print(df_tratado.info())