# SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


from __future__ import annotations

import json

import pandas as pd
from sqlalchemy.orm import Session

from impulsoetl.sisab.cadastros_individuais.modelos import (
    cadastros_equipe_homologadas,
    cadastros_equipe_validas,
    cadastros_todas_equipes,
)
from impulsoetl.loggers import logger


def carregar_cadastros(
    sessao: Session, cadastros_transformada: pd.DataFrame, visao_equipe: str
) -> int:

    registros = json.loads(
        cadastros_transformada.to_json(
            orient="records",
            date_format="iso",
        )
    )

    if visao_equipe == 'equipes-validas':
        requisicao_insercao = cadastros_equipe_validas.insert().values(registros)
        sufixo_tabela = 'equipe_validas'
    elif visao_equipe == 'equipes-homologadas':
        requisicao_insercao = cadastros_equipe_homologadas.insert().values(registros)
        sufixo_tabela = 'equipe_homologadas'
    else:
        requisicao_insercao = cadastros_todas_equipes.insert().values(registros)
        sufixo_tabela = 'equipe_todas'

    conector = sessao.connection()
    conector.execute(requisicao_insercao)

    logger.info(
        "Carregamento concluído para a tabela `{tabela_nome}`: "
        + "adicionadas {linhas_adicionadas} novas linhas.",
        tabela_nome=f"dados_publicos._sisab_cadastros_municipios_{sufixo_tabela}",
        linhas_adicionadas=len(cadastros_transformada),
    )

    return 0
