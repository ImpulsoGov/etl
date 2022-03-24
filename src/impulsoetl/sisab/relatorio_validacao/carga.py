
#%%
from sqlalchemy import create_engine
import psycopg2
import pandas as pd
from sqlalchemy.orm import sessionmaker




#%%
# conexão com banco de dados                 #postgres://usuario:senha@host:porta(5432)/database
engine = create_engine('postgresql+psycopg2://silas:Sessions-Angrily9-Sandlot-Acuteness-Around-Scoundrel-Baboon-Poison@35.239.239.250:5432/postgres')
conn = engine.raw_connection()
cur = conn.cursor()
#teste de conexão - (tentar mais uma vez após 30 segundos caso dê erro)
try:
    conn 
    print("Success!!")
except Exception as e:
	print("connect fail : "+str(e))

#%%
Session = sessionmaker(bind=engine)
session = Session()




#%%
#engine = create_engine('postgresql+psycopg2://scott:tiger@localhost/mydatabase')

#%%
#Parâmetro if_exists='replace': Se a tabela existir será reescrita. Por padrão é fail. Alterada em razão do exercício
#Parâmetro index=False: Não inclui o índice do dataframe como uma coluna na tabela


#df.to_sql('cnes_equipes_1', engine, if_exists='replace', index=False)

#%%
pd.read_sql_query("""select * from dadospublicos.populacao limit 1;""", engine)
# %%


#%%
import json
import uuid
from typing import Final

import numpy as np
import pandas as pd
from sqlalchemy.orm import Session

from impulsoetl.comum.datas import periodo_por_data
from impulsoetl.comum.geografias import id_sus_para_id_impulso
from impulsoetl.loggers import logger
from impulsoetl.siasus.modelos import raas_ps as tabela_destino


#%%
def carregar_raas_ps(
    sessao: Session, raas_ps_transformada: pd.DataFrame
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
        raas_ps_transformada.to_json(
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
        linhas_adicionadas=len(raas_ps_transformada),
    )

    return 0

#%%
def carregar_aih_rd(
    sessao: Session,
    aih_rd_transformada: pd.DataFrame,
    passo: int = 1000,
) -> int:
    """Carrega um arquivo de disseminação de procedimentos ambulatoriais no BD.

    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov.
        aih_rd_transformada: [`DataFrame`][] contendo os dados a serem
            carregados na tabela de destino, já no formato utilizado pelo banco
            de dados da ImpulsoGov (conforme retornado pela função
            [`transformar_aih_rd()`][]).
        passo: Indica quantos registros devem ser enviados para a base de dados
            de cada vez.

    Retorna:
        Código de saída do processo de carregamento. Se o carregamento
        for bem sucedido, o código de saída será `0`.

    [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
    [`DataFrame`]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    [`transformar_aih_rd()`]: impulsoetl.sihsus.aih_rd.transformar_aih_rd
    """

    tabela_nome = tabela_destino.key
    num_registros = len(aih_rd_transformada)
    logger.info(
        "Carregando {num_registros} registros de procedimentos ambulatoriais "
        "para a tabela `{tabela_nome}`...",
        num_registros=num_registros,
        tabela_nome=tabela_nome,
    )

    logger.info("Processando dados para JSON e de volta para um dicionário...")
    registros = json.loads(
        aih_rd_transformada.to_json(
            orient="records",
            date_format="iso",
        )
    )

    conector = sessao.connection()

    # Iterar por fatias do total de registro. Isso é necessário porque
    # executar todas as inserções em uma única operação acarretaria um consumo
    # proibitivo de memória
    contador = 0
    while contador < num_registros:
        logger.info(
            "Enviando registros para a tabela de destino "
            "({contador} de {num_registros})...",
            contador=contador,
            num_registros=num_registros,
        )
        subconjunto_registros = registros[
            contador : min(num_registros, contador + passo)
        ]
        requisicao_insercao = tabela_destino.insert().values(
            subconjunto_registros,
        )
        try:
            conector.execute(requisicao_insercao)
        except Exception as err:
            mensagem_erro = str(err)
            if len(mensagem_erro) > 500:
                mensagem_erro = mensagem_erro[:500]
            logger.error(mensagem_erro)
            breakpoint()
            sessao.rollback()
            return 1

        contador += passo

    logger.info(
        "Carregamento concluído para a tabela `{tabela_nome}`: "
        + "adicionadas {linhas_adicionadas} novas linhas.",
        tabela_nome=tabela_nome,
        linhas_adicionadas=num_registros,
    )

    return 0