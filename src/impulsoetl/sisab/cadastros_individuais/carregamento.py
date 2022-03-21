
from __future__ import annotations

import json
import pandas as pd
from sqlalchemy.orm import Session
from modelos import cadastros as tabela_destino
#import sys
#sys.path.append("/Users/walt/PycharmProjects/Impulso/ETL/etl/src/impulsoetl")
#from loggers import logger
from impulsoetl.loggers import logger

def carregar_cadastros(
    sessao: Session, cadastros_transformada
    ) -> int:
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

    requisicao_insercao = tabela_destino.insert().values(registros)

    conector = sessao.connection()
    conector.execute(requisicao_insercao)

    logger.info(
        "Carregamento concluído para a tabela `{tabela_nome}`: "
        + "adicionadas {linhas_adicionadas} novas linhas.",
        tabela_nome="dados_publicos.siasus_raas_psicossocial_disseminacao",
        linhas_adicionadas=len(cadastros_transformada),
    )

    return 0