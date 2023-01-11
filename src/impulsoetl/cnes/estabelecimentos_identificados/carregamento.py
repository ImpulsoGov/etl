# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Carrega dados dos estabelecimentos de saúde no banco de dados da Impulso."""

import warnings
warnings.filterwarnings("ignore")
import pandas as pd

from sqlalchemy.orm import Session
from prefect import task

from impulsoetl.loggers import logger
from impulsoetl.utilitarios.bd import carregar_dataframe

@task(
    name="Carregar dados dos Estabelecimentos Identificados",
    description=(
        "Realiza o carregamento dos dados dos estabelecimentos de saúde"
        +"extraídos e transformados a partir da página do CNES " 
        +"com destino ao banco de dados da Impulso Gov"
    ),
    tags=["cnes", "estabelecimentos", "carregamento"],
    retries=0,
    retry_delay_seconds=None,
)
def carregar_dados(
    sessao: Session, 
    df_tratado: pd.DataFrame, 
    tabela_destino: str
) -> int:

    """
    Carrega os dados dos estabelecimentos de saúde no banco de dados da Impulso

     Argumentos:
        sesssao: objeto [`sqlalchemy.orm.session.Session`][] que permite acessar a base de 
                 dados da ImpulsoGov.
        df_tratado: [`DataFrame`][] contendo os dados a serem carregados na tabela destino,
                    já no formato utilizado no banco de dados da impulso.
        tabela_destino: Tabela destino onde serão carregados os dados extraídos e tratados.

     Retorna:
            Código de saída do processo de carregamento. Se o carregamento for bem sucedido, 
            o código de saída será `0`.
    """
    
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