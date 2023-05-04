# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Carrega dados de indicadores de desempenho no banco de dados da Impulso."""


import json

import pandas as pd
from prefect import task
from sqlalchemy.orm import Session

from impulsoetl.loggers import habilitar_suporte_loguru, logger
from impulsoetl.sisab.indicadores_municipios.modelos import (
    indicadores_equipe_homologadas,
    indicadores_equipe_validas,
    indicadores_todas_equipes,
)


@task(
    name="Carrega Indicadores do Previne Brasil",
    description=(
        "Carrega os dados dos relatórios de indicadores do Previne Brasil "
        + "extraídos e transformados a partir do portal público do Sistema de "
        + "Informação em Saúde para a Atenção Básica do SUS."
    ),
    tags=["aps", "sisab", "indicadores_municipios", "carregamento"],
    retries=0,
    retry_delay_seconds=None,
)
def carregar_indicadores(
    sessao: Session,
    indicadores_transformada: pd.DataFrame,
    visao_equipe: str,
) -> int:
    habilitar_suporte_loguru()
    registros = json.loads(
        indicadores_transformada.to_json(
            orient="records",
            date_format="iso",
        )
    )

    if visao_equipe == "equipes-validas":
        requisicao_insercao = indicadores_equipe_validas.insert().values(
            registros
        )
        sufixo_tabela = "equipe_validas"
    elif visao_equipe == "equipes-homologadas":
        requisicao_insercao = indicadores_equipe_homologadas.insert().values(
            registros
        )
        sufixo_tabela = "equipe_homologadas"
    else:
        requisicao_insercao = indicadores_todas_equipes.insert().values(
            registros
        )
        sufixo_tabela = "equipe_todas"

    sessao.execute(requisicao_insercao)

    logger.info(
        "Carregamento concluído para a tabela `{tabela_nome}`: "
        + "adicionadas {linhas_adicionadas} novas linhas.",
        tabela_nome="dados_publicos.sisab_indicadores_municipios_{}".format(
            sufixo_tabela
        ),
        linhas_adicionadas=len(indicadores_transformada),
    )

    return 0
