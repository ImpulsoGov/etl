# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Carrega dados de validação por ficha por aplicação no banco de dados da Impulso."""


from __future__ import annotations

import pandas as pd
from sqlalchemy.orm import Session,Query
from sqlalchemy import delete

from impulsoetl.loggers import logger
from impulsoetl.utilitarios.bd import carregar_dataframe
from impulsoetl.bd import tabelas

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
    registros = sessao.query(
        tabela.c.periodo_id, tabela.c.ficha, tabela.c.aplicacao, tabela.c.no_prazo
    ).distinct(tabela.c.periodo_id, tabela.c.ficha, tabela.c.aplicacao,tabela.c.no_prazo)

    logger.info("Leitura dos períodos inseridos no banco Impulso OK!")
    return registros

def carregar_dados(
    sessao: Session, 
    df_tratado: pd.DataFrame,
    tabela_destino:str,
    periodo_id:str,
    no_prazo:bool,
    ficha_tipo: str,
    aplicacao_tipo: str,
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
    [`pandas.D """

    logger.info("Excluíndo registros se houver atualização retroativa...")
    tabela_relatorio_validacao = tabelas[tabela_destino]
    registros_inseridos = obter_lista_registros_inseridos(
        sessao, tabela_destino
    )

    if any(
        [
            registro.aplicacao == aplicacao_tipo
            and registro.ficha == ficha_tipo
            and registro.periodo_id == periodo_id
            and registro.no_prazo == no_prazo
            for registro in registros_inseridos
        ]
    ):
        limpar = (
            delete(tabela_relatorio_validacao)
            .where(
                tabela_relatorio_validacao.c.periodo_id == periodo_id
            )
            .where(tabela_relatorio_validacao.c.ficha == ficha_tipo)
            .where(tabela_relatorio_validacao.c.aplicacao == aplicacao_tipo)
            .where(tabela_relatorio_validacao.c.no_prazo == no_prazo)
        )
        logger.debug(limpar)
        sessao.execute(limpar)

    logger.info("Carregando dados em tabela...")
    carregar_dataframe(sessao=sessao, df=df_tratado, tabela_destino=tabela_destino)

    

    logger.info(
        "Carregamento concluído para a tabela `{tabela_nome}`: "
        + "adicionadas {linhas_adicionadas} novas linhas.",
        tabela_nome=tabela_destino,
        linhas_adicionadas=len(df_tratado),
    )

    return 0
