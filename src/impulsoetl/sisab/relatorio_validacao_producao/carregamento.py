# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Carrega dados de validação por ficha por aplicação no banco de dados da Impulso."""


import pandas as pd
from prefect import task
from sqlalchemy import delete
from sqlalchemy.orm import Query, Session

from impulsoetl.bd import tabelas
from impulsoetl.loggers import habilitar_suporte_loguru, logger
from impulsoetl.utilitarios.bd import carregar_dataframe


def obter_lista_registros_inseridos(
    sessao: Session,
    tabela_destino: str,
) -> Query:
    """Obtém lista de registro da períodos que já constam na tabela.
    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
        acessar a base de dados da ImpulsoGov.
        tabela_destino: Tabela que irá acondicionar os dados.
    Retorna:
        Lista de períodos que já constam na tabela destino filtrados por ficha
        e aplicação.
    [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
    """

    tabela = tabelas[tabela_destino]
    registros = sessao.query(tabela.c.periodo_id, tabela.c.no_prazo).distinct(
        tabela.c.periodo_id, tabela.c.no_prazo
    )

    logger.info("Leitura dos períodos inseridos no banco Impulso OK!")
    return registros


@task(
    name="Carrega Relatórios de Validação da Produção",
    description=(
        "Carrega os dados dos relatórios de validação da produção extraídos "
        + "extraídos e transformados a partir do portal público do Sistema "
        + "de Informação em Saúde para a Atenção Básica do SUS."
    ),
    tags=["aps", "sisab", "validacao_producao", "carregamento"],
    retries=0,
    retry_delay_seconds=None,
)
def carregar_dados(
    sessao: Session,
    df_tratado: pd.DataFrame,
    tabela_destino: str,
    no_prazo: bool,
    periodo_id: str,
) -> int:
    """Carrega os dados de um arquivo validação do portal SISAB no BD da Impulso.

    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov.
        df_tratado: objeto [`pandas.DataFrame`][] contendo os
            dados a serem carregados na tabela de destino, já no formato
            utilizado pelo banco de dados da ImpulsoGov.

    Retorna:
        Código de saída do processo de carregamento. Se o carregamento
        for bem sucedido, o código de saída será `0`.

    [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
    [`pandas.D"""
    habilitar_suporte_loguru()
    logger.info("Excluíndo registros se houver atualização retroativa...")
    tabela_relatorio_validacao = tabelas[tabela_destino]
    registros_inseridos = obter_lista_registros_inseridos(
        sessao, tabela_destino
    )

    if any(
        [registro.periodo_id == periodo_id for registro in registros_inseridos]
    ):
        limpar = (
            delete(tabela_relatorio_validacao)
            .where(tabela_relatorio_validacao.c.periodo_id == periodo_id)
            .where(tabela_relatorio_validacao.c.no_prazo == no_prazo)
        )
        logger.debug(limpar)
        sessao.execute(limpar)

    logger.info("Carregando dados em tabela...")
    carregar_dataframe(
        sessao=sessao, df=df_tratado, tabela_destino=tabela_destino
    )

    logger.info(
        "Carregamento concluído para a tabela `{tabela_nome}`: "
        + "adicionadas {linhas_adicionadas} novas linhas.",
        tabela_nome=tabela_destino,
        linhas_adicionadas=len(df_tratado),
    )

    return 0
