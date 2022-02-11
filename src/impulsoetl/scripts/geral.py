#!/usr/bin/env python3
#
# SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Scripts para a obtenção de dados de uso geral entre produtos da Impulso."""


from sqlalchemy.orm import Session

from impulsoetl.bd import Sessao, tabelas
from impulsoetl.brasilapi.cep import obter_cep
from impulsoetl.cnes.vinculos import obter_vinculos
from impulsoetl.loggers import logger

agendamentos = tabelas["configuracoes.capturas_agendamentos"]
capturas_historico = tabelas["configuracoes.capturas_historico"]


@logger.catch
def vinculos_disseminacao(
    sessao: Session,
    teste: bool = False,
) -> None:
    logger.info(
        "Capturando vínculos profissionais do CNES.",
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
            ano=agendamento.periodo_data_inicio.year,
            mes=agendamento.periodo_data_inicio.month,
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

    # vinculos_disseminacao(sessao=sessao, teste=teste)
    ceps(sessao=sessao, teste=teste)
    # outros scripts de uso geral aqui...


if __name__ == "__main__":
    with Sessao() as sessao:
        principal(sessao=sessao)
