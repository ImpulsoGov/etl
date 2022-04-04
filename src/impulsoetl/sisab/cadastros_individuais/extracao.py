
import requests
import urllib
from parametros_requisicao import head
import pandas as pd
from io import StringIO
from tratamento import tratamento_dados
from sqlalchemy.orm import Session
from carregamento import carregar_cadastros
import sys
sys.path.append("/Users/walt/PycharmProjects/Impulso/ETL/etl/src/impulsoetl")
from bd import Sessao
#from impulsoetl.bd import Sessao
from log import logger



ponderacao = [True,False]
periodos_dict = {
            '202201':'2022.M1'}

def extrair_cadastros_individuais(visao_equipe:str,pond:bool,competencia:str):
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

def _extrair_cadastros_individuais(rp,visao_equipe:str, ponderacao:bool):
   
    df = pd.read_csv(StringIO(rp), delimiter='\t', header=None)
    
    if ponderacao == False:
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

def obter_cadastros_individuais(sessao: Session,visao_equipe:str,periodo:str,teste: bool = True) -> None:
  
    for i in range(len(visao_equipe)):
      for k in range(len(ponderacao)):
        try:
            df = _extrair_cadastros_individuais(extrair_cadastros_individuais(visao_equipe[i][1],ponderacao[k], periodo), visao_equipe[i][0], ponderacao[k]) 
            df_tratado = tratamento_dados(df, ponderacao[k],periodo,sessao=sessao)
            carregar_cadastros(df_tratado,visao_equipe[i][0],sessao=sessao)
            if not teste:
              sessao.commit()
        except:
          logger.error(sys.exc_info())
          return False

visao_equipe=[('todas-equipes','')] 
periodo = '202201'
periodos_list = []
periodos_list.append(periodo)
teste = True


if __name__ == "__main__":
    with Sessao() as sessao:
      obter_cadastros_individuais(visao_equipe,periodo=periodos_list,sessao=sessao,teste=teste)
