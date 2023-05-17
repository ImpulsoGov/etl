import warnings

warnings.filterwarnings("ignore")
from datetime import date

import pandas as pd
import numpy as np

from typing import Final
from frozendict import frozendict


from sqlalchemy.orm import Session

from impulsoetl import __VERSION__
from impulsoetl.bd import Sessao, tabelas

from impulsoetl.loggers import logger, habilitar_suporte_loguru
from impulsoetl.sisab.relatorio_producao_profissional_conduta_tipo_atendimento.principal import relatorio_profissional_conduta_atendimento


agendamentos = tabelas["configuracoes.capturas_agendamentos"]
capturas_historico = tabelas["configuracoes.capturas_historico"]

def obter_relatorio_profissional_conduta_atendimento (
    teste:bool = True
) -> None:

    operacao_id = "064540b9-78b9-766c-8130-cdc0f1ed5828" 
    agendamentos = tabelas["configuracoes.capturas_agendamentos"]
    with Sessao() as sessao:
        agendamentos_relatorio_producao_saude = (
            sessao.query(agendamentos)
            .filter(agendamentos.c.operacao_id == operacao_id)
            .all()
        )
        sessao.commit()

        logger.info("Leitura dos Agendamentos ok!")
        for agendamento in agendamentos_relatorio_producao_saude:
            relatorio_profissional_conduta_atendimento(
                sessao=sessao,
                tabela_destino=agendamento.tabela_destino,
                periodo_competencia=agendamento.periodo_data_inicio,
                periodo_id=agendamento.periodo_id,
                unidade_geografica_id=agendamento.unidade_geografica_id
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

obter_relatorio_profissional_conduta_atendimento()
