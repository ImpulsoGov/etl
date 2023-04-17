import warnings

warnings.filterwarnings("ignore")
from datetime import date
from typing import Final

import numpy as np
import pandas as pd
import math
from frozendict import frozendict
from prefect import task

from prefect import flow

from impulsoetl import __VERSION__
from impulsoetl.bd import tabelas, Sessao

from impulsoetl.loggers import habilitar_suporte_loguru, logger
from impulsoetl.scnes.estabelecimentos_equipes.extracao import extrair_equipes
from impulsoetl.scnes.extracao_lista_cnes import extrair_lista_cnes
from impulsoetl.scnes.estabelecimentos_profissionais_totais.extracao import extrair_profissionais_totais
from impulsoetl.scnes.estabelecimentos_profissionais_totais.tratamento import tratamento_dados
from impulsoetl.scnes.estabelecimentos_profissionais_totais.principal import obter_profissionais_cnes_totais

"""
codigo_municipio = '351710' 
lista_codigos = extrair_lista_cnes(codigo_municipio)
periodo_data_inicio = date(2023,1,1)
periodo_id = 'aaaa'
unidade_geografica_id = 'xxxx'

df_extraido = extrair_profissionais_totais(codigo_municipio, lista_codigos, periodo_data_inicio)
df_tratado = tratamento_dados(df_extraido, periodo_id, unidade_geografica_id)
print(df_tratado.columns)
"""




agendamentos = tabelas["configuracoes.capturas_agendamentos"]
capturas_historico = tabelas["configuracoes.capturas_historico"]
"""
@flow(
    name="Rodar Agendamentos dos Profissionais SCNES",
    description=(
        "Lê as capturas agendadas para ficha de profissionais dos estabelecimentos de saúde por município"
    ),
    retries=0,
    retry_delay_seconds=None,
    timeout_seconds=14400,
    version=__VERSION__,
    validate_parameters=False,
)"""
def cnes_profissionais_totais(
    teste: bool = False,
) -> None:

    operacao_id = "0643da4f-8562-7520-ba7a-606062b1e1e1"

    with Sessao() as sessao:
        agendamentos_cnes = (
            sessao.query(agendamentos)
            .filter(agendamentos.c.operacao_id == operacao_id)
            .all()
        )

        for agendamento in agendamentos_cnes:
            periodo_id = agendamento.periodo_id
            unidade_geografica_id = agendamento.unidade_geografica_id
            tabela_destino = agendamento.tabela_destino
            codigo_sus_municipio = agendamento.unidade_geografica_id_sus
            periodo_data_inicio = agendamento.periodo_data_inicio

            obter_profissionais_cnes_totais(
                sessao=sessao,
                tabela_destino=tabela_destino,
                codigo_municipio=codigo_sus_municipio,
                periodo_id=periodo_id,
                unidade_geografica_id=unidade_geografica_id,
                periodo_data_inicio=periodo_data_inicio,
            )

            if teste:
                sessao.rollback()
                break

            logger.info("Registrando captura bem-sucedida...")

            requisicao_inserir_historico = capturas_historico.insert(
                {
                    "operacao_id": operacao_id,
                    "periodo_id": agendamento.periodo_id,
                    "unidade_geografica_id": agendamento.unidade_geografica_id,
                }
            )
            conector = sessao.connection()
            conector.execute(requisicao_inserir_historico)
            sessao.commit()
            logger.info("OK.")

if __name__ == '__main__':
    cnes_profissionais_totais()
