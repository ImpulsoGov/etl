# #%%
# import requests
# from sup import head,save_csv

# #%%
# url = "https://sisab.saude.gov.br/paginas/acessoRestrito/relatorio/federal/envio/RelValidacao.xhtml"
# periodo_tipo=['producao','envio']
# periodo_competencia='202201'
# aplicacao=['0','1','2','3','4','5']## o que é essa aplicação? 
# envio_prazo=['&envioPrazo=on','']

# def get_data(periodo_competencia):
#     try:
#         for periodo in periodo_tipo:
#             for sistema in aplicacao:
#                 for envio in envio_prazo:
#                     hd = head(url)
#                     vs = hd[1]
#                   payload='j_idt44=j_idt44&unidGeo=brasil&periodo='+periodo+'&j_idt70='+periodo_competencia+'&colunas=municipio&colunas=cnes&colunas=tp_unidade&colunas=ine&colunas=tp_equipe&j_idt77=2%25&j_idt77=3%25&j_idt77=4%25&j_idt77=1%25&j_idt77=N%2FA&j_idt87=2&j_idt92='+sistema+envio+'&javax.faces.ViewState='+vs+'&j_idt102=j_idt102'
#                     headers = hd[0]
#                     response = requests.request("POST", url, headers=headers, data=payload)
#                     save_csv(response.text,'validacao','CDS Offline','producao',periodo_competencia)
#     except Exception as e:
#       print(e)    
#       print("leitura falhou")


# get_data(periodo_competencia)


#%%
import requests
from sup import head
from io import StringIO
import pandas as pd

'''

    Faz a extração do relatório de validação de acordo com alguns parâmetros 

'''
#%%
try:
    url = "https://sisab.saude.gov.br/paginas/acessoRestrito/relatorio/federal/envio/RelValidacao.xhtml"
    retorno = requests.get(url) # checagem da URl
    print(retorno)
except Exception as e:
    print(e)

#%%
#período tipo sisab website
periodo_tipo='producao' #produção ou envio


#perido competência sisab website
periodo_competencia='202203'  #AAAAMM periodo averiguado

aplicacao=[]## o que é essa aplicação? #tipo de aplicação Filtro 4 

envio_prazo=[] #'&envioPrazo=on',''


try:
    hd = head(url)
    vs = hd[1] #viewstate
    payload='j_idt44=j_idt44&unidGeo=brasil&periodo='+periodo_tipo+'&j_idt70='+periodo_competencia+'&colunas=regiao&colunas=uf&colunas=ibge&colunas=municipio&colunas=cnes&colunas=ine&javax.faces.ViewState='+vs+'&j_idt102=j_idt102'
    headers = hd[0]
    response = requests.request("POST", url, headers=headers, data=payload)
    print("Dados obtidos")
    
except Exception as e:
    print(e)    
    print("leitura falhou")

#%% códigos para checagem de tipo e conteúdo do request
#print(type(response))

#src = response.content
#src


#%%
df = pd.read_csv (StringIO(response.text),sep=';',encoding = 'ISO-8859-1', skiprows=range(0,4), skipfooter=4)

#%%
df
