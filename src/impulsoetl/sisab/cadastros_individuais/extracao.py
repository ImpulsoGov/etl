
import requests
import urllib
from sisab.parametros_requisicao import head
import pandas as pd
from io import StringIO
from impulsoetl.tipos import DatetimeLike
 


def extrair_cadastros_individuais(visao_equipe:str,com_ponderacao:bool,competencia:DatetimeLike)->str:
    competencia = competencia.replace('-','')
    competencia= competencia[0:6]
    url = "https://sisab.saude.gov.br/paginas/acessoRestrito/relatorio/federal/indicadores/indicadorCadastro.xhtml"
    hd = head(url)
    vs = hd[1]
    ponderacao = '&beneficiarios=on' if com_ponderacao==True else ''
    visao_equipe = urllib.parse.quote(visao_equipe)
    headers = hd[0]
    payload='j_idt44=j_idt44&selectLinha=cnes_ine&opacao-capitacao='+visao_equipe+ponderacao+'&competencia='+competencia+'&javax.faces.ViewState='+vs+'&j_idt83=j_idt83'
    response = requests.request("POST", url, headers=headers, data=payload)
    return response.text

def _extrair_cadastros_individuais(rp:str,visao_equipe:str, com_ponderacao:bool)->pd.DataFrame:
 
    df = pd.read_csv(StringIO(rp), delimiter='\t', header=None, engine= 'python')
    
    if com_ponderacao == False:
      if visao_equipe == 'todas-equipes':
        dados = df.iloc[8:-4]
        df = pd.DataFrame(data=dados)
        df=df[0].str.split(';', expand=True)
        df.columns=['Uf','IBGE','Municipio','CNES','Nome UBS','INE','Sigla','quantidade','Parametro']
      else:
        dados = df.iloc[9:-4]
        df = pd.DataFrame(data=dados)
        df=df[0].str.split(';', expand=True)
        df.columns=['Uf','IBGE','Municipio','CNES','Nome UBS','INE','Sigla','quantidade','Parametro','Coluna']  
    else:
      if visao_equipe == 'todas-equipes':
        dados = df.iloc[9:-4]
        df = pd.DataFrame(data=dados)
        df=df[0].str.split(';', expand=True)
        df.columns=['Uf','IBGE','Municipio','CNES','Nome UBS','INE','Sigla','quantidade','Coluna']
      else:
        dados = df.iloc[10:-4]
        df = pd.DataFrame(data=dados)
        df=df[0].str.split(';', expand=True)
        df.columns=['Uf','IBGE','Municipio','CNES','Nome UBS','INE','Sigla','quantidade','Coluna']
    return df

