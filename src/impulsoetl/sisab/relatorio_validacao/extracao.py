#%%
import requests
from sup import head
from io import StringIO
import pandas as pd

'''

    Faz a extração do relatório de validação de acordo com alguns parâmetros 

'''
# #%%
# try:
#     url = "https://sisab.saude.gov.br/paginas/acessoRestrito/relatorio/federal/envio/RelValidacao.xhtml"
#     retorno = requests.get(url) # checagem da URl
#     print(retorno)
# except Exception as e:
#     print(e)

#%%
#período tipo sisab website
periodo_tipo='producao' #produção ou envio


#perido competência sisab website

periodo_competencia='202203'  #AAAAMM periodo averiguado

'''
    Pelo padrão utilizado no banco de dados da impulso e dos dados obtidos mensalmente, fazer a conversão de AAAAMM para AAAAMX onde X é o mês de competência 
'''
ano = periodo_competencia[0:4]
#print(ano)

mes = 'M'+periodo_competencia[5:6]
#print(mes)


aplicacao=[]## o que é essa aplicação? #tipo de aplicação Filtro 4 


envio_prazo_on = '&envioPrazo=on' #envio prazo ON 
envio_prazo=[] #preencher envio_prazo_on ou deixar vazio ou usar '' em caso de querer os 2 de uma vez 

#%%
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

#%% códigos para checagem de tipo e conteúdo do request
#print(type(response))
#src = response.content
#src



#%%TRATAMENTO
'''
    TRATAMENTO DE DADOS
'''


#df = pd.read_csv (StringIO(response.text),sep=';',encoding = 'ISO-8859-1', skiprows=range(0,4), skipfooter=4) ORIGINAL DIRETO DA EXTRAÇÃO 

# leitura do arquivo pulando o cabeçalho e últimas linhas
df = pd.read_csv ('rel_Validacao032022.csv',sep=';',encoding = 'ISO-8859-1', skiprows=range(0,4), skipfooter=4)


#%%

# Análise dos dados
# df.head(10)
# 
# df.tail(10)
# 
# print(df)
#
# df.isnull().sum()
# 
# df.info()

#%%
'''
    Dados excluídos por falta de necessidade: Região, Uf, Município, coluna NaN
    Coluna IBGE e demais colunas mudarão de nome 
'''

df.drop(['Região', 'Uf','Municipio','Unnamed: 8'], axis=1, inplace=True)



df.columns = ['municipio_id_sus', 'cnes_id', 'id_ine', 'validacao_nome', 'validacao_quantidade']


#%% novas colunas em lugares específicos
df.insert(0,"id", value= '')


df.insert(2,"periodo_id", value='')



#%% novas colunas para padrão tabela requerida
df = df.assign(criacao_data = pd.Timestamp.now(),
            atualizacao_data = pd.Timestamp.now(), 
            no_prazo = 1 if(envio_prazo == envio_prazo_on) else 0,
            periodo_codigo = ano+mes)


#%%
df['no_prazo'] = df['no_prazo'].astype('bool')

#%%
#df.head()

#%%
df.info()
#%%

