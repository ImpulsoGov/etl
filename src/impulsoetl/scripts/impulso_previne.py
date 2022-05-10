#!/usr/bin/env python3
#
# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Scripts para o produto Impulso Previne."""


import pandas as pd
from requests import head
from sqlalchemy.orm import Session

from impulsoetl.bd import Sessao, tabelas
from impulsoetl.loggers import logger

from impulsoetl.sisab.cadastros_individuais import obter_cadastros_individuais
from impulsoetl.sisab.parametros_cadastro import obter_parametros
from impulsoetl.sisab.relatorio_validacao import (
    obter_validacao_municipios_producao,
)


agendamentos = tabelas["configuracoes.capturas_agendamentos"]
capturas_historico = tabelas["configuracoes.capturas_historico"]


@logger.catch
def cadastros_municipios_equipe_validas(
    sessao: Session,
    teste: bool = False,
) -> None:

    logger.info(
        "Capturando Cadastros de equipes válidas por município.",
    )
    # este já é o ID definitivo da operação!
    operacao_id = "da6bf13a-2acd-44c1-a3e2-21ab071fc8a3"
    visao_equipe = "equipes-validas"
    agendamentos_cadastros = (
        sessao.query(agendamentos)
        .filter(agendamentos.c.operacao_id == operacao_id)
        .all()
    )

    for agendamento in agendamentos_cadastros:
        periodo = agendamento.periodo_data_inicio
        obter_cadastros_individuais(
            sessao=sessao,
            visao_equipe=visao_equipe,
            periodo=periodo,
            teste=teste,
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
                "operacao_id": operacao_id,
                "periodo_id": agendamento.periodo_id,
                "unidade_geografica_id": agendamento.unidade_geografica_id,
            }
        )
        conector = sessao.connection()
        conector.execute(requisicao_inserir_historico)
        sessao.commit()
        logger.info("OK.")


@logger.catch
def cadastros_municipios_equipe_homologada(
    sessao: Session,
    teste: bool = False,
) -> None:

    logger.info(
        "Capturando Cadastros de equipes válidas por município.",
    )

    operacao_id = "c668a75e-9eeb-4176-874b-98d7553222f2"
    visao_equipe = "equipes-homologadas"
    agendamentos_cadastros = (
        sessao.query(agendamentos)
        .filter(agendamentos.c.operacao_id == operacao_id)
        .all()
    )

    for agendamento in agendamentos_cadastros:
        periodo = agendamento.periodo_data_inicio
        obter_cadastros_individuais(
            sessao=sessao,
            visao_equipe=visao_equipe,
            periodo=periodo,
            teste=teste,
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
                "operacao_id": operacao_id,
                "periodo_id": agendamento.periodo_id,
                "unidade_geografica_id": agendamento.unidade_geografica_id,
            }
        )
        conector = sessao.connection()
        conector.execute(requisicao_inserir_historico)
        sessao.commit()
        logger.info("OK.")


@logger.catch
def cadastros_municipios_equipe_todas(
    sessao: Session,
    teste: bool = False,
) -> None:

    logger.info(
        "Capturando Cadastros de equipes válidas por município.",
    )

    operacao_id = "180ae562-2e34-4ae7-bff4-31ded6f0b418"
    visao_equipe = "todas-equipes"
    agendamentos_cadastros = (
        sessao.query(agendamentos)
        .filter(agendamentos.c.operacao_id == operacao_id)
        .all()
    )

    for agendamento in agendamentos_cadastros:
        periodo = agendamento.periodo_data_inicio
        obter_cadastros_individuais(
            sessao=sessao,
            visao_equipe=visao_equipe,
            periodo=periodo,
            teste=teste,
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
                "operacao_id": operacao_id,
                "periodo_id": agendamento.periodo_id,
                "unidade_geografica_id": agendamento.unidade_geografica_id,
            }
        )
        conector = sessao.connection()
        conector.execute(requisicao_inserir_historico)
        sessao.commit()
        logger.info("OK.")


@logger.catch
def parametros_municipios_equipes_homologada(
    sessao: Session,
    teste: bool = False,
) -> None:

    logger.info(
        "Capturando parâmetros de cadastros por município.",
    )

    operacao_id = "8f593199-fcef-4023-b79a-0ed7f9050cd2"
    visao_equipe = 'equipes-homologadas'
    nivel_agregacao = 'municipios'

    agendamentos_cadastros = (
        sessao.query(agendamentos)
        .filter(agendamentos.c.operacao_id == operacao_id)
        .all()
    )

    for agendamento in agendamentos_cadastros:
        periodo = agendamento.periodo_data_inicio
        obter_parametros(
            sessao=sessao,
            visao_equipe=visao_equipe,
            periodo=periodo,
            nivel_agregacao=nivel_agregacao,
            teste=teste
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
                "operacao_id": operacao_id,
                "periodo_id": agendamento.periodo_id,
                "unidade_geografica_id": agendamento.unidade_geografica_id,
            }
        )
        conector = sessao.connection()
        conector.execute(requisicao_inserir_historico)
        sessao.commit()
        logger.info("OK.")


@logger.catch
def parametros_cne_ine_equipes_homologada(
    sessao: Session,
    teste: bool = False,
) -> None:

    logger.info(
        "Capturando parâmetros de cadastros por estabelecimento e equipe.",
    )

    operacao_id = "dcb03493-8ad2-4f48-bd3b-4022fc33c2c2"
    visao_equipe = 'equipes-homologadas'
    nivel_agregacao = 'estabelecimentos_equipes'

    agendamentos_cadastros = (
        sessao.query(agendamentos)
        .filter(agendamentos.c.operacao_id == operacao_id)
        .all()
    )

    for agendamento in agendamentos_cadastros:
        periodo = agendamento.periodo_data_inicio
        obter_parametros(
            sessao=sessao,
            visao_equipe=visao_equipe,
            periodo=periodo,
            nivel_agregacao=nivel_agregacao,
            teste=teste
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
                "operacao_id": operacao_id,
                "periodo_id": agendamento.periodo_id,
                "unidade_geografica_id": agendamento.unidade_geografica_id,
            }
        )
        conector = sessao.connection()
        conector.execute(requisicao_inserir_historico)
        sessao.commit()
        logger.info("OK.")


@logger.catch
def parametros_cnes_ine_equipes_validas(
    sessao: Session,
    teste: bool = False,
) -> None:

    logger.info(
        "Capturando parâmetros de cadastros por estabelecimento e equipe.",
    )

    operacao_id = "3a61f9ca-c32f-4844-b6ac-a115bd8e4b5a"
    visao_equipe = 'equipes-validas'
    nivel_agregacao = 'estabelecimentos_equipes'

    agendamentos_cadastros = (
        sessao.query(agendamentos)
        .filter(agendamentos.c.operacao_id == operacao_id)
        .all()
    )

    for agendamento in agendamentos_cadastros:
        periodo = agendamento.periodo_data_inicio
        obter_parametros(
            sessao=sessao,
            visao_equipe=visao_equipe,
            periodo=periodo,
            nivel_agregacao=nivel_agregacao,
            teste=teste
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
                "operacao_id": operacao_id,
                "periodo_id": agendamento.periodo_id,
                "unidade_geografica_id": agendamento.unidade_geografica_id,
            }
        )
        conector = sessao.connection()
        conector.execute(requisicao_inserir_historico)
        sessao.commit()
        logger.info("OK.")


@logger.catch
def validacao_municipios_por_producao(
    sessao: Session,
    teste: bool = False,
) -> None:

    # este já é o ID definitivo da operação!
    operacao_id = "c84c1917-4f57-4592-a974-50a81b3ed6d5"

    # Ler agendamentos e rodar ETL para cada agendamento pendente
    # ...
    agendamentos = tabelas["configuracoes.capturas_agendamentos"]
    agendamentos_relatorio_validacao = (
        sessao.query(agendamentos)
        .filter(agendamentos.c.operacao_id == operacao_id)
        .all()
    )
    sessao.commit()

    logger.info("Leitura dos Agendamentos ok!")

    envio_prazo_lista = [True, False]

    for agendamento in agendamentos_relatorio_validacao:
        for tipo in envio_prazo_lista:
            envio_prazo = tipo
            obter_validacao_municipios_producao(
                sessao=sessao,
                periodo_competencia=agendamento.periodo_data_inicio,
                envio_prazo=envio_prazo,
                tabela_destino=agendamento.tabela_destino,
                periodo_codigo=agendamento.periodo_codigo,
            )

        if teste:  # evitar rodar muitas iterações
            break

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
        sessao.commit()
        logger.info("OK.")


def principal(sessao: Session, teste: bool = False) -> None:
    """Executa todos os scripts de captura de dados do Impulso Previne.

    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov.
        teste: Indica se as modificações devem ser de fato escritas no banco de
            dados (`False`, padrão). Caso seja `True`, as modificações são
            adicionadas à uma transação, e podem ser revertidas com uma chamada
            posterior ao método [`Session.rollback()`][] da sessão gerada com o
            SQLAlchemy.

    [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
    """

    cadastros_municipios_equipe_validas(sessao=sessao, teste=teste)
    cadastros_municipios_equipe_homologada(sessao=sessao, teste=teste)
    cadastros_municipios_equipe_todas(sessao=sessao, teste=teste)
    parametros_municipios_equipes_validas(sessao=sessao, teste=teste)
    parametros_municipios_equipes_homologada(sessao=sessao, teste=teste)
    parametros_cnes_ine_equipes_validas(sessao=sessao, teste=teste)
    parametros_cne_ine_equipes_homologada(sessao=sessao, teste=teste)
    validacao_municipios_por_producao(sessao=sessao, teste=teste)

    # outros scripts do Impulso Previne aqui...


if __name__ == "__main__":
    with Sessao() as sessao:
        principal(sessao=sessao)
