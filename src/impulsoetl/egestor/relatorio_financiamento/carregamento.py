# SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT

"""Carrega dados do relatório de financiamento do Egestor no banco de dados da Impulso."""

import pandas as pd
from prefect import task
from sqlalchemy.orm import Session

from impulsoetl.loggers import habilitar_suporte_loguru, logger
from impulsoetl.utilitarios.bd import carregar_dataframe


@task(
    name="Carregar Relatórios de Financiamento",
    description=(
        "Carrega os dados dos relatórios de financiamento da Atenção "
        + "Primária à Saúde extraídos e transformados a partir do eGestor "
        + "Atenção Básica com destino ao banco de dados da Impulso Gov."
    ),
    tags=["aps", "egestor", "financiamento", "carregamento"],
    retries=0,
    retry_delay_seconds=None,
)
def carregar_dados(
    sessao: Session, df_tratado: pd.DataFrame, tabela_destino: str
) -> int:
    """Carrega os dados de uma aba do relatório de financiamento do egstor para o BD da Impulso.

    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov.
        df_tratado: objeto [`pandas.DataFrame`][] contendo os
            dados a serem carregados na tabela de destino, já no formato
            utilizado pelo banco de dados da ImpulsoGov.
        tabela_destino: Nome da tabela de destino a ser carregada com os dados extraidos e tratados.

    Retorna:
        Código de saída do processo de carregamento. Se o carregamento
        for bem sucedido, o código de saída será `0`.

    [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
    [`pandas.D
    """
    habilitar_suporte_loguru()
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
