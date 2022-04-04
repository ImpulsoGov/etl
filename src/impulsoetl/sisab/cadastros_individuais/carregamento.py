
from __future__ import annotations
import json
from sqlalchemy.orm import Session
#from impulsoetl.loggers import logger
import sys
sys.path.append("/Users/walt/PycharmProjects/Impulso/ETL/etl/src/impulsoetl")
from bd import logger
from modelos import cadastros_equipe_homologadas ,cadastros_todas_equipes, cadastros_equipe_validas
from log import logger

def carregar_cadastros(cadastros_transformada,visao_equipe,sessao: Session) -> int:
    try:

        """Carrega os dados de um arquivo de disseminação da RAAS no BD da Impulso.
        Argumentos:
            sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
                acessar a base de dados da ImpulsoGov.
            raas_ps: [`DataFrame`][] contendo os dados a serem carregados
                na tabela de destino, já no formato utilizado pelo banco de dados
                da ImpulsoGov (conforme retornado pela função
                [`transformar_raas_ps()`][]).
        Retorna:
            Código de saída do processo de carregamento. Se o carregamento
            for bem sucedido, o código de saída será `0`.
        [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
        [`DataFrame`]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
        [`transformar_raas_ps()`]: impulsoetl.siasus.raas_ps.transformar_raas_ps
        """

        registros = json.loads(
            cadastros_transformada.to_json(
                orient="records",
                date_format="iso",
            )
        )

        if visao_equipe == 'equipes-validas':
            requisicao_insercao = cadastros_equipe_validas.insert().values(registros)
            sulfixo_tabela = 'equipe_validas'
        elif visao_equipe == 'equipes-homologadas':
            requisicao_insercao = cadastros_equipe_homologadas.insert().values(registros)
            sulfixo_tabela = 'equipe_homologadas'
        else:
            requisicao_insercao = cadastros_todas_equipes.insert().values(registros)
            sulfixo_tabela = 'equipe_todas'

        conector = sessao.connection()
        conector.execute(requisicao_insercao)
        
        return 0

    except:
          logger.error(sys.exc_info())
          return False