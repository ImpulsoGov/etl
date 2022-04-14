
from __future__ import annotations
import json
from sqlalchemy.orm import Session
import pandas as pd
from impulsoetl.loggers import logger
from impulsoetl.bd import logger

from modelos import parametros_municipios_equipe_validas, parametros_municipios_equipe_homologadas

def carregar_parametros_municipios(sessao: Session,parametros_municipios_transformada:pd.DataFrame,visao_equipe:str) -> int:

    registros = json.loads(
        parametros_municipios_transformada.to_json(
            orient="records",
            date_format="iso",
        )
    )

    if visao_equipe == 'equipes-validas':
        requisicao_insercao = parametros_municipios_equipe_validas.insert().values(registros)
        sulfixo_tabela = 'equipe_validas'
    if visao_equipe == 'equipes-homologadas':
        requisicao_insercao = parametros_municipios_equipe_homologadas.insert().values(registros)
        sulfixo_tabela = 'equipe_homologadas'
    conector = sessao.connection()
    conector.execute(requisicao_insercao)


    logger.info(
        "Carregamento concluído para a tabela `{tabela_nome}`: "
        + "adicionadas {linhas_adicionadas} novas linhas.",
        tabela_nome=f"dados_publicos._sisab_cadastros_municipios_{sulfixo_tabela}",
        linhas_adicionadas=len(parametros_municipios_transformada),
    )
    
    return 0