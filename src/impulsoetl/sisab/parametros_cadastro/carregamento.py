
from __future__ import annotations
import json
from sqlalchemy.orm import Session
import pandas as pd
from impulsoetl.loggers import logger
from impulsoetl.bd import logger
from impulsoetl.sisab.parametros_cadastro.modelos import (
parametros_municipios_equipe_validas, 
parametros_municipios_equipe_homologadas,
parametros_equipes_equipe_validas,
parametros_equipes_equipe_homologadas
)

def carregar_parametros(sessao: Session,parametros_transformada:pd.DataFrame,visao_equipe:str,nivel_agregacao:str) -> int:

    registros = json.loads(
        parametros_transformada.to_json(
            orient="records",
            date_format="iso",
        )
    )

    if nivel_agregacao == 'municipios':
        if visao_equipe == 'equipes-validas':
            requisicao_insercao = parametros_municipios_equipe_validas.insert().values(registros)
            sufixo_tabela = 'parametro_municipios_equipe_validas'
        if visao_equipe == 'equipes-homologadas':
            requisicao_insercao = parametros_municipios_equipe_homologadas.insert().values(registros)
            sufixo_tabela = 'parametro_municipios_equipe_homologadas'
    else:
        if visao_equipe == 'equipes-validas':
            requisicao_insercao = parametros_equipes_equipe_validas.insert().values(registros)
            sufixo_tabela = 'parametro_cnes_ine_equipe_validas'
        if visao_equipe == 'equipes-homologadas':
            requisicao_insercao = parametros_equipes_equipe_homologadas.insert().values(registros)
            sufixo_tabela = 'parametro_cnes_ine_equipe_homologadas'

    conector = sessao.connection()
    conector.execute(requisicao_insercao)


    logger.info(
        "Carregamento concluído para a tabela `{tabela_nome}`: "
        + "adicionadas {linhas_adicionadas} novas linhas.",
        tabela_nome=f"dados_publicos._sisab_cadastros_{sufixo_tabela}",
        linhas_adicionadas=len(parametros_transformada),
    )
    
    return 0