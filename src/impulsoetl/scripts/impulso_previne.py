#!/usr/bin/env python3
#
# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT
# flake8: noqa
# type: ignore


"""Scripts para o produto Impulso Previne."""


from requests import head
from sqlalchemy.orm import Session

from impulsoetl.bd import Sessao, tabelas
from impulsoetl.loggers import logger
#from impulsoetl.sisab.cadastros import obter_cadastros_municipios_equipe_validas
from impulsoetl.sisab.relatorio_validacao.funcoes import obter_validacao_municipios_producao
from impulsoetl.sisab.relatorio_validacao.funcoes import obter_lista_periodo


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
    engine = sessao.get_bind()
    agendamentos = tabelas["configuracoes.capturas_agendamentos"]
    periodos = pd.read_sql_query(
            f"""select distinct periodo_data_inicio from {agendamentos} where operacao_id = '{operacao_id}';""",
            engine   )

    periodos['periodo_data_inicio'] = pd.to_datetime(periodos['periodo_data_inicio'])

    periodos["periodo_data_inicio"] = periodos["periodo_data_inicio"].apply(lambda x: (x).strftime('%Y%m'))

    periodos_lista = periodos['periodo_data_inicio'].tolist()

    logger.info("Leitura dos Agendamentos ok!")
    
    envio_prazo_on = '&envioPrazo=on' #Check box envio requisições no prazo marcado
    
    envio_prazo_lista=[envio_prazo_on,'']

    for periodo in periodos_lista:
        periodo_competencia = periodo
        for tipo in envio_prazo_lista:
            envio_prazo = tipo
            obter_validacao_municipios_producao(sessao=sessao,periodo_competencia=periodo_competencia,envio_prazo=envio_prazo)

    #if teste:  # evitar rodar muitas iterações
    #    break

    #sessao.commit()
    return 0

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
