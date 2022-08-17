# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Carrega dados de validação por ficha por aplicação no banco de dados da Impulso."""


from __future__ import annotations

import json

import pandas as pd
from sqlalchemy.orm import Session
from impulsoetl.loggers import logger
from impulsoetl.bd import tabelas

sisab_validacao_municipios_por_producao_ficha_por_aplicacao = tabelas[
    "dados_publicos._sisab_validacao_municipios_por_producao_ficha_por_aplicacao"
]

def carregar_dados(
    sessao: Session, df_tratado: pd.DataFrame
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

    registros = json.loads(
        df_tratado.to_json(
            orient="records",
            date_format="iso",
        )
    )

    requisicao_insercao = sisab_validacao_municipios_por_producao_ficha_por_aplicacao.insert().values(registros)

    sessao.execute(requisicao_insercao)

    logger.info(
        "Carregamento concluído para a tabela `{tabela_nome}`: "
        + "adicionadas {linhas_adicionadas} novas linhas.",
        tabela_nome="dados_publicos._sisab_validacao_municipios_por_producao_ficha_por_aplicacao",
        linhas_adicionadas=len(df_tratado),
    )

    return 0
