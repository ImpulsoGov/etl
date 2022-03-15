#!/usr/bin/env python3
#
# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Scripts para o produto Impulso Previne."""


from sqlalchemy.orm import Session

from impulsoetl.bd import Sessao, tabelas
from impulsoetl.loggers import logger
# from impulsoetl.sisab.cadastros import obter_cadastros_municipios_equipe_validas
# from impulsoetl.sisab.validacao import obter_validacao_municipios_por_producao


agendamentos = tabelas["configuracoes.capturas_agendamentos"]


@logger.catch
def cadastros_municipios_equipe_validas(
    sessao: Session,
    teste: bool = False,
) -> None:

    # este já é o ID definitivo da operação!
    operacao_id = ("da6bf13a-2acd-44c1-a3e2-21ab071fc8a3")

    # Ler agendamentos e rodar ETL para cada agendamento pendente
    # ...


@logger.catch
def validacao_municipios_por_producao(
    sessao: Session,
    teste: bool = False,
) -> None:

    # este já é o ID definitivo da operação!
    operacao_id = ("c84c1917-4f57-4592-a974-50a81b3ed6d5")

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
    validacao_municipios_por_producao(sessao=sessao, teste=teste)
    # outros scripts do Impulso Previne aqui...


if __name__ == "__main__":
    with Sessao() as sessao:
        principal(sessao=sessao)
