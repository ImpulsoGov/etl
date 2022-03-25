#%%
import requests
from sup import head
from io import StringIO
import pandas as pd
import os
import json
import dotenv

dotenv.load_dotenv(dotenv.find_dotenv())
oge = os.getenv

#----------- importações para consulta banco
from sqlalchemy import create_engine
import psycopg2
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker

#----------- importações para carga
from impulsoetl.loggers import logger
from impulsoetl.bd import tabelas

#importacao para transformacao

from impulsoetl.comum.geografias import id_sus_para_id_impulso


#%%
# conexão com banco de dados                 #postgres://usuario:senha@host:porta(5432)/database

engine = create_engine(f"postgresql+psycopg2://{oge('IMPULSOETL_BD_USUARIO')}:{oge('IMPULSOETL_BD_SENHA')}@{oge('IMPULSOETL_BD_HOST')}:{oge('IMPULSOETL_BD_PORTA')}/{oge('IMPULSOETL_BD_NOME')}")
conn = engine.raw_connection()
cur = conn.cursor()

Session = sessionmaker(bind=engine)
sessao = Session()


#teste de conexão - (tentar mais uma vez após 30 segundos caso dê erro)
try:
    conn 
    print("Conexão com o banco Impulso OK!")
except Exception as e:
	print("connect fail : "+str(e))

#%%-------------------------------------------Extração-----------------------------------


'''

    Faz a extração do relatório de validação de acordo com alguns parâmetros 

'''
# #%% Checando disponibilidade da API
# try:
#     url = "https://sisab.saude.gov.br/paginas/acessoRestrito/relatorio/federal/envio/RelValidacao.xhtml"
#     retorno = requests.get(url) # checagem da URl
#     print('Api Online 'retorno)
# except Exception as e:
#     print('Erro na requisição: 'e)

#%% Parâmetros sisab website
#Filtros dos períodos no sisab website
periodo_tipo='producao' #produção ou envio


periodo_competencia='202203'  #AAAAMM periodo averiguado

'''
    Pelo padrão utilizado no banco de dados da impulso e dos dados obtidos mensalmente, fazer a conversão de AAAAMM para AAAAMX onde X é o mês de competência 

'''

ano = periodo_competencia[0:4]

mes = '.M'+periodo_competencia[5:6]

periodo_codigo = ano+mes

envio_prazo_on = '&envioPrazo=on' #Check box envio requisições no prazo marcado

envio_prazo=[] #preencher envio_prazo_on ou deixar vazio ou usar '' em caso de querer os 2 de uma vez 

aplicacao=[] #tipo de aplicação Filtro 4 

#%% Busca dados na API
# try:
#     hd = head(url)
#     vs = hd[1] #viewstate
#     payload='j_idt44=j_idt44&unidGeo=brasil&periodo='+periodo_tipo+'&j_idt70='+periodo_competencia+'&colunas=regiao&colunas=uf&colunas=ibge&colunas=municipio&colunas=cnes&colunas=ine&javax.faces.ViewState='+vs+'&j_idt102=j_idt102'
#     headers = hd[0]
#     response = requests.request("POST", url, headers=headers, data=payload)
#     print("Dados obtidos")
    
# except Exception as e:
#     print(e)    
#     print("leitura falhou")




#%%--------------------------------------------TRATAMENTO
'''
    TRATAMENTO DE DADOS
'''
try:

    #df = pd.read_csv (StringIO(response.text),sep=';',encoding = 'ISO-8859-1', skiprows=range(0,4), skipfooter=4) ORIGINAL DIRETO DA EXTRAÇÃO 

    # leitura do arquivo pulando o cabeçalho e últimas linhas
    df = pd.read_csv ('/home/silas/Documentos/Impulso/etlValidacaolocal/rel_Validacao032022.csv',sep=';',encoding = 'ISO-8859-1',engine='python', skiprows=range(0,4), skipfooter=4)

    print('Dados carregados!!!')

except Exception as e:
    print(e+' Falha no carregamento')

#%%
'''
    Dados excluídos por falta de necessidade: Região, Uf, Município, coluna NaN
    Coluna IBGE e demais colunas mudarão de nome 
'''
print('Dados em tratamento!')

try:

    df.drop(['Região', 'Uf','Municipio','Unnamed: 8'], axis=1, inplace=True)



    df.columns = ['municipio_id_sus', 'cnes_id', 'id_ine', 'validacao_nome', 'validacao_quantidade']


    #------------- novas colunas em lugares específicos
    df.insert(0,"id", value= '')


    df.insert(2,"periodo_id", value='')


    #-------------novas colunas para padrão tabela requerida
    df = df.assign(criacao_data = pd.Timestamp.now(),
                atualizacao_data = pd.Timestamp.now(), 
                no_prazo = 1 if(envio_prazo == envio_prazo_on) else 0,
                periodo_codigo = periodo_codigo,
                unidade_geografica_id = '')


    df['no_prazo'] = df['no_prazo'].astype('bool')


    query = pd.read_sql_query(f"select id  from listas_de_codigos.periodos where codigo  = '{periodo_codigo}';", engine)

    query = (query.iloc[0]['id'])

    df = df.assign (periodo_id = query)

    idsus = (df.iloc[0]['municipio_id_sus']) #idsus para preencher coluna id_unidade_geo 

    id_unidade_geo = id_sus_para_id_impulso(sessao,idsus)

    print('Dados Tratados!\nDataframe final Pronto!')

except Exception as e:
    print(e+' Erro de tratamento!')

#%%
print(df.head())

#%% 

# Análise dos dados
# df.head(10)
# 
# df.tail(10)
# 
# print(df)
#
# df.isnull().sum()

#códigos para checagem de tipo e conteúdo do request
#print(type(response))
#src = response.content
#src


#------------------------------------------------------------------------------------------
# def carregar_relatorio_validacao(
#     sessao: Session, relatorio_validacao_df: pd.DataFrame
# ) -> int:
#     """Carrega os dados de um arquivo de disseminação da RAAS no BD da Impulso.

#     Argumentos:
#         sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
#             acessar a base de dados da ImpulsoGov.
#         relatorio_validacao_df: [`DataFrame`][] contendo os dados a serem carregados
#             na tabela de destino, já no formato utilizado pelo banco de dados
#             da ImpulsoGov.

#     Retorna:
#         Código de saída do processo de carregamento. Se o carregamento
#         for bem sucedido, o código de saída será `0`.

#     """

#     registros = json.loads(
#         relatorio_validacao_df.to_json(
#             orient="records",
#             date_format="iso",
#         )
#     )


#     tabela_relatorio_validacao = tabelas["dados_publicos.sisab_validacao_municipios_por_producao"]

#     requisicao_insercao = tabela_relatorio_validacao.insert().values(registros)

#     conector = sessao.connection()
#     conector.execute(requisicao_insercao)

#     logger.info(
#             "Carregamento concluído para a tabela `{tabela_relatorio_validacao}`: "
#             + "adicionadas {linhas_adicionadas} novas linhas.",
#             tabela_nome="dados_publicos.sisab_validacao_municipios_por_producao",
#             linhas_adicionadas=len(relatorio_validacao_df),
#         )

#     return 0

