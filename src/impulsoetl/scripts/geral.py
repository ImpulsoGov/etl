#!/usr/bin/env python3
#
# SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Scripts para a obtenção de dados de uso geral entre produtos da Impulso."""


from prefect import flow
from sqlalchemy.orm import Session

from impulsoetl import __VERSION__
from impulsoetl.bd import tabelas
from impulsoetl.brasilapi.cep import obter_cep
from impulsoetl.loggers import habilitar_suporte_loguru, logger
from impulsoetl.scnes.habilitacoes import obter_habilitacoes
from impulsoetl.scnes.vinculos import obter_vinculos
from impulsoetl.sim.do import obter_do

agendamentos = tabelas["configuracoes.capturas_agendamentos"]
capturas_historico = tabelas["configuracoes.capturas_historico"]


@flow(
    name="Tratar Agendamentos de Habilitações do SCNES",
    description=(
        "Lê as capturas agendadas para os arquivos de disseminação de "
        + "habilitações dos estabelecimentos de saúde do Sistema do Cadastro "
        + "Nacional de Estabelecimentos de Saúde."
    ),
    retries=0,
    retry_delay_seconds=None,
    version=__VERSION__,
    validate_parameters=False,
)
def habilitacoes_disseminacao(
    sessao: Session,
    teste: bool = False,
) -> None:
    habilitar_suporte_loguru()
    logger.info(
        "Capturando habilitações dos estabelecimentos do SCNES.",
    )
    operacao_id = "06307c18-d268-748c-8cd2-75cd262126c4"
    agendamentos_habilitacoes = (
        sessao.query(agendamentos)
        .filter(agendamentos.c.operacao_id == operacao_id)
        .all()
    )
    for agendamento in agendamentos_habilitacoes:
        obter_habilitacoes(
            sessao=sessao,
            uf_sigla=agendamento.uf_sigla,
            periodo_data_inicio=agendamento.periodo_data_inicio,
            tabela_destino=agendamento.tabela_destino,
            teste=teste,
        )

        logger.info("Registrando captura bem-sucedida...")
        # NOTE: necessário registrar a operação de captura em nível de UF,
        # mesmo que o gatilho na tabela de destino no banco de dados já
        # registre a captura em nível dos municípios automaticamente quando há
        # a inserção de uma nova linha
        requisicao_inserir_historico = capturas_historico.insert(
            {
                "operacao_id": operacao_id,
                "periodo_id": agendamento.periodo_id,
                "unidade_geografica_id": agendamento.unidade_geografica_id,
            }
        )
        conector = sessao.connection()
        conector.execute(requisicao_inserir_historico)
        if teste:
            sessao.rollback()
            break
        sessao.commit()
        logger.info("OK.")


@flow(
    name="Tratar Agendamentos de Vínculos do SCNES",
    description=(
        "Lê as capturas agendadas para os arquivos de disseminação de "
        + "vínculos dos profissionais de saúde do Sistema do Cadastro "
        + "Nacional de Estabelecimentos de Saúde."
    ),
    retries=0,
    retry_delay_seconds=None,
    version=__VERSION__,
    validate_parameters=False,
)
def vinculos_disseminacao(
    sessao: Session,
    teste: bool = False,
) -> None:
    habilitar_suporte_loguru()
    logger.info(
        "Capturando vínculos profissionais do SCNES.",
    )
    operacao_id = "f8d49ce7-7e11-44ff-9308-885d1b181f6d"
    agendamentos_vinculos = (
        sessao.query(agendamentos)
        .filter(agendamentos.c.operacao_id == operacao_id)
        .all()
    )
    for agendamento in agendamentos_vinculos:
        obter_vinculos(
            sessao=sessao,
            uf_sigla=agendamento.uf_sigla,
            periodo_data_inicio=agendamento.periodo_data_inicio,
            tabela_destino=agendamento.tabela_destino,
            teste=teste,
        )

        logger.info("Registrando captura bem-sucedida...")
        # NOTE: necessário registrar a operação de captura em nível de UF,
        # mesmo que o gatilho na tabela de destino no banco de dados já
        # registre a captura em nível dos municípios automaticamente quando há
        # a inserção de uma nova linha
        requisicao_inserir_historico = capturas_historico.insert(
            {
                "operacao_id": operacao_id,
                "periodo_id": agendamento.periodo_id,
                "unidade_geografica_id": agendamento.unidade_geografica_id,
            }
        )
        conector = sessao.connection()
        conector.execute(requisicao_inserir_historico)
        if teste:
            sessao.rollback()
            break
        sessao.commit()
        logger.info("OK.")


@flow(
    name="Tratar Agendamentos de Declarações de Óbito do SIM",
    description=(
        "Lê as capturas agendadas para os arquivos de disseminação de "
        + "declarações de óbito do Sistema de Informações da Mortalidade do "
        + "SUS."
    ),
    retries=0,
    retry_delay_seconds=None,
    version=__VERSION__,
    validate_parameters=False,
)
def obitos_disseminacao(
    sessao: Session,
    teste: bool = False,
) -> None:
    habilitar_suporte_loguru()
    logger.info("Capturando Declarações de Óbito do SIM.")
    operacao_ids = [
        "063091e1-9bf4-782c-95bb-a564713aeaa0",
    ]
    agendamentos_do = (
        sessao.query(agendamentos)
        .filter(agendamentos.c.operacao_id.in_(operacao_ids))
        .all()
    )
    for agendamento in agendamentos_do:
        obter_do(
            sessao=sessao,
            uf_sigla=agendamento.uf_sigla,
            periodo_id=agendamento.periodo_id,
            periodo_data_inicio=agendamento.periodo_data_inicio,
            tabela_destino=agendamento.tabela_destino,
            teste=teste,
            **agendamento.parametros,
        )
        if teste:
            break

        logger.info("Registrando captura bem-sucedida...")
        # NOTE: necessário registrar a operação de captura em nível de UF,
        # mesmo que o gatilho na tabela de destino no banco de dados já
        # registre a captura em nível dos municípios automaticamente quando há
        # a inserção de uma nova linha
        requisicao_inserir_historico = capturas_historico.insert(
            {
                "operacao_id": agendamento.operacao_id,
                "periodo_id": agendamento.periodo_id,
                "unidade_geografica_id": agendamento.unidade_geografica_id,
            }
        )
        conector = sessao.connection()
        conector.execute(requisicao_inserir_historico)
        sessao.commit()
        logger.info("OK.")


@flow(
    name="Tratar Agendamentos de CEPs",
    description=(
        "Lê as capturas agendadas para os Códigos de Endereçamento Postal dos "
        + "Correios."
    ),
    retries=0,
    retry_delay_seconds=None,
    version=__VERSION__,
    validate_parameters=False,
)
def ceps(sessao: Session, teste: bool = False) -> None:
    habilitar_suporte_loguru()
    logger.info(
        "Capturando informações de Códigos de Endereçamento Postal.",
    )
    logger.info("Checando CEPs pendentes...")

    tabela_ceps_pendentes = tabelas["configuracoes.ceps_pendentes"]

    ceps_pendentes_query = sessao.query(
        tabela_ceps_pendentes.c.id_cep,
    )
    if teste:
        ceps_pendentes_query = ceps_pendentes_query.limit(10)
    ceps_pendentes = ceps_pendentes_query.all()

    obter_cep(sessao=sessao, ceps_pendentes=ceps_pendentes, teste=teste)
