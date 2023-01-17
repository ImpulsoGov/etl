# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Carrega dados do CNES no banco de dados da Impulso."""

import warnings

warnings.filterwarnings("ignore")
import pandas as pd
#from prefect import task
from sqlalchemy.orm import Session

from impulsoetl.loggers import logger, habilitar_suporte_loguru
from impulsoetl.utilitarios.bd import carregar_dataframe

def carregar_dados(
    sessao: Session, df_tratado: pd.DataFrame, tabela_destino: str
) -> int:

    """
    Carrega os dados dos ETLs do SCNES no banco de dados da Impulso
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
    #habilitar_suporte_loguru()

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