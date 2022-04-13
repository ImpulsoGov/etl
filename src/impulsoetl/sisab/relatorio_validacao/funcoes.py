# flake8: noqa
# type: ignore
import requests
from suporte_extracao import head
from io import StringIO
import pandas as pd
import os
import json
import dotenv
from datetime import datetime
import uuid
# ----------- importações para consulta banco
from sqlalchemy import create_engine
import psycopg2
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker
from sqlalchemy import delete
#----------- importações para carga
from impulsoetl.loggers import logger
from impulsoetl.bd import tabelas
# importacao para transformacao
from impulsoetl.comum.geografias import id_sus_para_id_impulso
from frozenlist import FrozenList
from impulsoetl.bd import Sessao


def obter_lista_periodo(operacao_id,sessao=Sessao):
    """Obtém lista de períodos da tabela agendamento

    Args:
        operacao_id (_type_): ID da operação do ETL

    Returns:
        periodos_lista: períodos que precisam ser atualizados em formato de lista
    """    


    agendamentos = tabelas["configuracoes.capturas_agendamentos"]
    periodos = pd.read_sql_query(
            f"""select distinct periodo_data_inicio from {agendamentos} where operacao_id = '{operacao_id}';""",
            engine    )

    periodos['periodo_data_inicio'] = pd.to_datetime(periodos['periodo_data_inicio'])

    periodos["periodo_data_inicio"] = periodos["periodo_data_inicio"].apply(lambda x: (x).strftime('%Y%m'))

    periodos_lista = periodos['periodo_data_inicio'].tolist()
    return periodos_lista


def obter_lista_periodos_inseridos(sessao=Sessao):
    """Obtém lista de períodos da períodos que já constam na tabela

        Returns:
        periodos_lista: períodos que já constam na tabela destino
    """    


    tabela_alvo="dados_publicos._sisab_validacao_municipios_por_producao"
    periodos = pd.read_sql_query(
            f"""select distinct periodo_codigo from {tabela_alvo};""",
            engine    )
    periodos = periodos['periodo_codigo'].tolist()
    return periodos

def competencia_para_periodo_codigo(periodo_competencia):
    """Essa função converte o período de competência de determinado relatorio no código do periodo padrão da impulso
    EX: 202203 para 2022.M3
    Args:
        periodo_competencia (str): período de competência de determinado relatório

    Returns:
        periodo código
    """


    ano = periodo_competencia[0:4]
    mes = ".M" + periodo_competencia[4:6]
    if mes[2] == '0':
        mes = ".M" + periodo_competencia[5:6]
        periodo_codigo = ano + mes
    else:
        periodo_codigo = ano + mes
    return periodo_codigo

def obter_data_criacao(tabela, periodo_codigo,sessao=Sessao):
    """Obtém a data de criação do registro que já consta na tabela baseado no período
        
        Args:
            tabela (str): tabela alvo da busca
            periodo_codigo (str): Período de referência da data
        
        Returns:
        data_criacao: data em formato datetime  
    """


    data_criacao = pd.read_sql_query(
                f"select distinct criacao_data from {tabela} where periodo_codigo  = '{periodo_codigo}';",
                engine)
    if data_criacao.empty == True:
        data_criacao = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    else:
        data_criacao = data_criacao.iloc[0]["criacao_data"].strftime("%Y-%m-%d %H:%M:%S")
    return data_criacao


def requisicao_validacao_sisab_producao(periodo_competencia,envio_prazo):
    """Obtém os dados da API
    
    Args:
        tabela (str): tabela alvo da busca
        periodo_codigo (str): Período de referência da data
    
    Returns:
    data_criacao: data em formato datetime  
"""


    url = "https://sisab.saude.gov.br/paginas/acessoRestrito/relatorio/federal/envio/RelValidacao.xhtml"
    periodo_tipo='producao'
    hd = head(url)
    vs = hd[1] #viewstate
    payload='j_idt44=j_idt44&unidGeo=brasil&periodo='+periodo_tipo+'&j_idt70='+periodo_competencia+'&colunas=regiao&colunas=uf&colunas=ibge&colunas=municipio&colunas=cnes&colunas=ine'+envio_prazo+'&javax.faces.ViewState='+vs+'&j_idt102=j_idt102'
    headers = hd[0]
    resposta = requests.request("POST", url, headers=headers, data=payload)    
    

def tratamento_validacao_producao(resposta,data_criacao,envio_prazo,periodo_codigo,sessao=Sessao):
    """Tratamento dos dados obtidos 

    Args:
    resposta (requests.models.Response): Resposta da requisição efetuada no sisab


    Returns:
    df: dataframe com os dados enriquecidos e tratados em formato pandas dataframe  
    """


    df = pd.read_csv (StringIO(resposta.text),sep=';',encoding = 'ISO-8859-1', skiprows=range(0,4), skipfooter=4,  engine='python') #ORIGINAL DIRETO DA EXTRAÇÃO 

    df['INE'] = df['INE'].fillna('0')

    df['INE'] = df['INE'].astype('int')

    df.drop(["Região", "Uf", "Municipio", "Unnamed: 8"], axis=1, inplace=True)

    df.columns = [
        "municipio_id_sus",
        "cnes_id",
        "ine_id",
        "validacao_nome",
        "validacao_quantidade",
    ]

    # ------------- novas colunas em lugares específicos
    df.insert(0, "id", value="")

    df.insert(2, "periodo_id", value="")

    # -------------novas colunas para padrão tabela requerida

    df = df.assign(
        criacao_data=data_criacao,
        atualizacao_data=pd.Timestamp.now(),
        no_prazo=1 if (envio_prazo == envio_prazo_on) else 0,
        periodo_codigo=periodo_codigo,
    )

    query = pd.read_sql_query(
        f"select id  from listas_de_codigos.periodos where codigo  = '{periodo_codigo}';",
        engine,
    )

    query = query.iloc[0]["id"]

    df = df.assign(periodo_id=query)

    idsus = df.iloc[0][
        "municipio_id_sus"
    ]  # idsus para preencher coluna id_unidade_geo

    id_unidade_geo = id_sus_para_id_impulso(sessao, idsus)

    df = df.assign(unidade_geografica_id=id_unidade_geo)

    df['id'] = df.apply(lambda row:uuid.uuid4(), axis=1)

    df[["id","municipio_id_sus", "periodo_id","cnes_id",\
        "ine_id","validacao_nome","periodo_codigo","unidade_geografica_id"]] = df[["id","municipio_id_sus", "periodo_id","cnes_id","ine_id",\
        "validacao_nome","periodo_codigo","unidade_geografica_id"]].astype("string")


    df["validacao_quantidade"] = df["validacao_quantidade"].astype("int")

    df["no_prazo"] = df["no_prazo"].astype("bool")

    df['atualizacao_data'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    df_validacao_tratado = df 
    return df_validacao_tratado


def carregar_validacao_producao(df_validacao_tratado,sessao=Sessao):
    """Carrega os dados de um arquivo validação do portal SISAB no BD da Impulso.

    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov.
        relatorio_validacao_df: [`DataFrame`][] contendo os dados a serem carregados
            na tabela de destino, já no formato utilizado pelo banco de dados
            da ImpulsoGov.

    Retorna:
        Código de saída do processo de carregamento. Se o carregamento
        for bem sucedido, o código de saída será `0`.
  """


    relatorio_validacao_df = df_validacao_tratado

    registros = json.loads(
        relatorio_validacao_df.to_json(
            orient="records",
            date_format="iso",
        )
    )


    tabela_relatorio_validacao = tabelas[
        "dados_publicos._sisab_validacao_municipios_por_producao"
    ]  # tabela teste

    conector = sessao.connection()

    if periodo_codigo in periodos_inseridos:
        
        limpar = delete(tabela_relatorio_validacao).where(tabela_relatorio_validacao.c.periodo_codigo == periodo_codigo)
        print(limpar)
        conector.execute(limpar)
        sessao.commit()



    requisicao_insercao = tabela_relatorio_validacao.insert().values(registros)
    print(requisicao_insercao)

    conector.execute(requisicao_insercao)
    sessao.commit()


