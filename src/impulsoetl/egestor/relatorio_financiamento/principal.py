# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Junta etapas do fluxo de ETL do relatório de financiamento do Egestor por aplicação dos municípios."""

from __future__ import annotations

from datetime import date
from typing import Final
import pandas as pd
from sqlalchemy.orm import Session

from impulsoetl.egestor.relatorio_financiamento.tratamento import tratamento_dados 
from impulsoetl.egestor.relatorio_financiamento.verificacao import verificar_relatorio_egestor 
from impulsoetl.egestor.relatorio_financiamento.carregamento import carregar_dados 
from impulsoetl.bd import Sessao
from impulsoetl.loggers import logger


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
        periodo_competencia: date,
        periodo_id: str,
        tabela_destino: str,
    ) -> None:

        # substituir por função/método de extração dos dados
        arquivo_excel = "/Users/Walter Matheus/Impulso/etl_impulso/etl/2022-09_pagamento_aps.xls"
        df_extraido = pd.read_excel(arquivo_excel, sheet_name=ABAS_NOMES[tabela_destino] , header=3, dtype="object")
        ####

        df_tratado=tratamento_dados(
            sessao=sessao,
            df_extraido=df_extraido,
            aba=ABAS_NOMES[tabela_destino],
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


# captura de agendamento (teste)
periodo_id = '33a78f01-26b6-4b22-a3bd-43fc5a802775'
periodo_competencia = date(year=2022, month=9, day=1)
tabela_destino = 'dados_publicos.egestor_financiamento_acoes_estrategicas_academia_saude'
teste=True

if __name__ == "__main__":
    with Sessao() as sessao:
        obter_relatorio_financiamento(
            sessao=sessao,
            periodo_competencia=periodo_competencia,
            periodo_id=periodo_id,
            tabela_destino=tabela_destino
            )
        if not teste:
            sessao.commit()
