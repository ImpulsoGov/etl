
from __future__ import annotations
import json
from sqlalchemy.orm import Session
import pandas as pd
from impulsoetl.loggers import logger
from impulsoetl.bd import logger
from impulsoetl.sisab.indicadores_municipios.modelos import (
    indicadores_equipe_validas,
    indicadores_equipe_homologadas, 
    indicadores_todas_equipes
)

def carregar_indicadores(sessao: Session,indicadores_transformada:pd.DataFrame,visao_equipe:str) -> int:

    registros = json.loads(
        indicadores_transformada.to_json(
            orient="records",
            date_format="iso",
        )
    )

    if visao_equipe == 'equipes-validas':
        requisicao_insercao = indicadores_equipe_validas.insert().values(registros)
        sufixo_tabela = 'equipe_validas'
    elif visao_equipe == 'equipes-homologadas':
        requisicao_insercao = indicadores_equipe_homologadas.insert().values(registros)
        sufixo_tabela = 'equipe_homologadas'
    else:
        requisicao_insercao = indicadores_todas_equipes.insert().values(registros)
        sufixo_tabela = 'equipe_todas'

    conector = sessao.connection()
    conector.execute(requisicao_insercao)


    logger.info(
        "Carregamento concluído para a tabela `{tabela_nome}`: "
        + "adicionadas {linhas_adicionadas} novas linhas.",
        tabela_nome=f"dados_publicos.sisab_indicadores_municipios_{sufixo_tabela}",
        linhas_adicionadas=len(indicadores_transformada),
    )
    
    return 0

    