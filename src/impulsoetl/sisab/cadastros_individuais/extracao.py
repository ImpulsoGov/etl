
import requests
import urllib
from parametros_requisicao import head
import pandas as pd
from io import StringIO
from tratamento import tratamentoDados
from sqlalchemy.orm import Session
from carregamento import carregar_cadastros
#import sys
#sys.path.append("/Users/walt/PycharmProjects/Impulso/ETL/etl/src/impulsoetl")
#from bd import Sessao
from impulsoetl.bd import Sessao



#('todas-equipes',''),
#('equipes-homologadas','|HM|')

visao_equipe=[
    ('equipes-validas','|HM|NC|AQ|')
] 

ponderacao = [True, False]


def extracaoDados(visao_equipe,pond,competencia):
    url = "https://sisab.saude.gov.br/paginas/acessoRestrito/relatorio/federal/indicadores/indicadorCadastro.xhtml"
    hd = head(url)
    vs = hd[1]
    ponderacao = '&beneficiarios=on' if pond==True else ''
    visao_equipe = urllib.parse.quote(visao_equipe)
    headers = hd[0]
    joined_string = "&competencia=".join(competencia)
    payload='j_idt44=j_idt44&selectLinha=cnes_ine&opacao-capitacao='+visao_equipe+ponderacao+'&competencia='+joined_string+'&javax.faces.ViewState='+vs+'&j_idt83=j_idt83'
    response = requests.request("POST", url, headers=headers, data=payload)
    return response.text

def criacaoDataFrame(rp,tipo_equipe, ponderacao,sessao: Session):
    try:
      df = pd.read_csv(StringIO(rp), delimiter='\t', header=None)
      if ponderacao == False:
        if tipo_equipe == 'todas-equipes':
          dados = df.iloc[8:-4]
          df = pd.DataFrame(data=dados)
          df=df[0].str.split(';', expand=True)
          df.columns=['Uf','IBGE','Municipio','CNES','Nome UBS','INE','Sigla','JAN/2022','Parametro']
        else:
          dados = df.iloc[9:-4]
          df = pd.DataFrame(data=dados)
          df=df[0].str.split(';', expand=True)
          df.columns=['Uf','IBGE','Municipio','CNES','Nome UBS','INE','Sigla','JAN/2022','Parametro','Coluna']  
      else:
        if tipo_equipe == 'todas-equipes':
          dados = df.iloc[9:-4]
          df = pd.DataFrame(data=dados)
          df=df[0].str.split(';', expand=True)
          df.columns=['Uf','IBGE','Municipio','CNES','Nome UBS','INE','Sigla','JAN/2022','Coluna']
        else:
          dados = df.iloc[10:-4]
          df = pd.DataFrame(data=dados)
          df=df[0].str.split(';', expand=True)
          df.columns=['Uf','IBGE','Municipio','CNES','Nome UBS','INE','Sigla','JAN/2022','Coluna']
      return df

    except Exception as e:
      print(e)

def main(periodo_list,sessao: Session):
    try:
        for i in range(len(visao_equipe)):
            for k in range(len(ponderacao)):
                df = criacaoDataFrame(extracaoDados(visao_equipe[i][1],ponderacao[k], periodo_list), visao_equipe[i][0], ponderacao[k],sessao=sessao) 
                df_tratado = tratamentoDados(df,visao_equipe[i][0], ponderacao[k],sessao=sessao)
                df_carregado = carregar_cadastros(cadastros_transformada=df_tratado,sessao=sessao)
                return df_carregado            
    except Exception as e:
      print(e)

if __name__ == "__main__":
    with Sessao() as sessao:
      periodos_list = ['202201']
      main(periodos_list,sessao=sessao)