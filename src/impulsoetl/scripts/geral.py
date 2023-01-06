#!/usr/bin/env python3
#
# SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Scripts para a obtenção de dados de uso geral entre produtos da Impulso."""


from sqlalchemy.orm import Session

from impulsoetl.bd import Sessao, tabelas
from impulsoetl.brasilapi.cep import obter_cep
from impulsoetl.loggers import logger
from impulsoetl.scnes.habilitacoes import obter_habilitacoes
from impulsoetl.scnes.vinculos import obter_vinculos
from impulsoetl.sim.do import obter_do

from impulsoetl.cnes.estabelecimentos_identificados.principal import obter_informacoes_estabelecimentos_identificados

agendamentos = tabelas["configuracoes.capturas_agendamentos"]
capturas_historico = tabelas["configuracoes.capturas_historico"]


@logger.catch
def habilitacoes_disseminacao(
    sessao: Session,
    teste: bool = False,
) -> None:
    logger.info(
        "Capturando vínculos profissionais do SCNES.",
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


@logger.catch
def vinculos_disseminacao(
    sessao: Session,
    teste: bool = False,
) -> None:
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


@logger.catch
def obitos_disseminacao(
    sessao: Session,
    teste: bool = False,
) -> None:
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


@logger.catch
def ceps(sessao: Session, teste: bool = False) -> None:
    logger.info(
        "Capturando informações de Códigos de Endereçamento Postal.",
    )
    logger.info("Checando CEPs pendentes...")

    tabela_ceps_pendentes = tabelas["configuracoes.ceps_pendentes"]
    ceps_pendentes = sessao.query(
        tabela_ceps_pendentes.c.id_cep,
    ).all()
    obter_cep(sessao=sessao, ceps_pendentes=ceps_pendentes, teste=teste)


def cnes_estabelecimentos_identificados(
    sessao : Session,
    teste: bool = True,
)-> None:

    operacao_id  = "063b5cf8-34d1-744d-8f96-353d4f199171"

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

        df_extraido = obter_informacoes_estabelecimentos_identificados(
            sessao=sessao,
            tabela_destino=tabela_destino,
            codigo_municipio=codigo_sus_municipio
        )
        df_extraido['periodo_id']=periodo_id
        df_extraido['unidade_geografica_id']=unidade_geografica_id

        if teste: 
            sessao.rollback()
            break

        logger.info("Registrando captura bem-sucedida...")

        #requisicao_inserir_historico = capturas_historico.insert(
         #   {
           #     "operacao_id": operacao_id,
           #     "periodo_id": agendamento.periodo_id,
           #     "unidade_geografica_id": agendamento.unidade_geografica_id,
         #   }
      #  )
        #conector = sessao.connection()
        #conector.execute(requisicao_inserir_historico)
        sessao.commit()
        logger.info("OK.")


def principal(sessao: Session, teste: bool = False) -> None:
    """Executa todos os scripts de captura de dados de uso geral.

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

    vinculos_disseminacao(sessao=sessao, teste=teste)
    cnes_estabelecimentos_identificados(sessao=sessao, teste=teste)
    # ceps(sessao=sessao, teste=teste)
    # outros scripts de uso geral aqui...


if __name__ == "__main__":
    with Sessao() as sessao:
        principal(sessao=sessao)
