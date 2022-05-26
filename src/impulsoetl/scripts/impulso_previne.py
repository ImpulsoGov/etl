#!/usr/bin/env python3
#
# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT

"""Scripts para o produto Impulso Previne."""


from sqlalchemy.orm import Session

from impulsoetl.bd import Sessao, tabelas
from impulsoetl.loggers import logger
from impulsoetl.sisab.relatorio_validacao_ficha_aplicacao_producao.funcoes import (
    obter_validacao_ficha_aplicacao_producao,
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
def validacao_municipios_por_producao(
    sessao: Session,
    teste: bool = False,
) -> None:

    # este já é o ID definitivo da operação!
    operacao_id = "c84c1917-4f57-4592-a974-50a81b3ed6d5"

    # Ler agendamentos e rodar ETL para cada agendamento pendente
    # ...


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
    validacao_municipios_por_producao(sessao=sessao, teste=teste)
    # outros scripts do Impulso Previne aqui...


def validacao_producao_ficha_por_aplicacao(
    sessao: Session,
    teste: bool = False,
) -> None:

    # este já é o ID definitivo da operação!
    operacao_id = "c577c9fd-6a8e-43e3-9d65-042ad2268cf0"

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

    fichas = {
        "Cadastro individual": "&j_idt87=2",
        "Atendimento individual": "&j_idt87=4",
        "Procedimentos": "&j_idt87=7",
        "Visita Domiciliar": "&j_idt87=8",
    }

    aplicacoes = {
        "CDS-offline": "&j_idt92=0",
        "CDS-online": "&j_idt92=1",
        "PEC": "&j_idt92=2",
        "Sistema proprio": "&j_idt92=3",
        "Android ACS": "&j_idt92=4",
    }

    for agendamento in agendamentos_relatorio_validacao:
        for ficha_tipo, ficha_codigo in fichas.items():
            for aplicacao_tipo, aplicacao_codigo in aplicacoes.items():
                if (
                    (
                        ficha_tipo == "Cadastro individual"
                        and aplicacao_tipo == "PEC"
                    )  # não precisa de \
                    or (
                        ficha_tipo == "Visita Domiciliar"
                        and aplicacao_tipo == "PEC"
                    )
                    or (
                        ficha_tipo == "Atendimento individual"
                        and aplicacao_tipo == "Android ACS"
                    )
                    or (
                        ficha_tipo == "Procedimentos"
                        and aplicacao_tipo == "Android ACS"
                    )
                ):
                    continue
                for tipo in envio_prazo_lista:
                    envio_prazo = tipo
                    obter_validacao_ficha_aplicacao_producao(
                        sessao=sessao,
                        periodo_competencia=agendamento.periodo_data_inicio,
                        ficha_tipo=ficha_tipo,
                        aplicacao_tipo=aplicacao_tipo,
                        ficha_codigo=ficha_codigo,
                        aplicacao_codigo=aplicacao_codigo,
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


if __name__ == "__main__":
    with Sessao() as sessao:
        principal(sessao=sessao)
