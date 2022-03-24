
import requests
import urllib
from parametros_requisicao import head
import pandas as pd
from io import StringIO
from tratamento import tratamentoDados
from sqlalchemy.orm import Session
from carregamento import carregar_cadastros
import sys
from impulsoetl.bd import Sessao


ponderacao = [True, False]
periodos_dict = {
            '202201':'2022.M1'}

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

def criacaoDataFrame(rp,visao_equipe, ponderacao,periodo):
    try:
      df = pd.read_csv(StringIO(rp), delimiter='\t', header=None)
      
      for item in periodo:
        if ponderacao == False:
          if visao_equipe == 'todas-equipes':
            dados = df.iloc[8:-4]
            df = pd.DataFrame(data=dados)
            df=df[0].str.split(';', expand=True)
            df.columns=['Uf','IBGE','Municipio','CNES','Nome UBS','INE','Sigla',periodos_dict[item],'Parametro']
          else:
            dados = df.iloc[9:-4]
            df = pd.DataFrame(data=dados)
            df=df[0].str.split(';', expand=True)
            df.columns=['Uf','IBGE','Municipio','CNES','Nome UBS','INE','Sigla',periodos_dict[item],'Parametro','Coluna']  
        else:
          if visao_equipe == 'todas-equipes':
            dados = df.iloc[9:-4]
            df = pd.DataFrame(data=dados)
            df=df[0].str.split(';', expand=True)
            df.columns=['Uf','IBGE','Municipio','CNES','Nome UBS','INE','Sigla',periodos_dict[item],'Coluna']
          else:
            dados = df.iloc[10:-4]
            df = pd.DataFrame(data=dados)
            df=df[0].str.split(';', expand=True)
            df.columns=['Uf','IBGE','Municipio','CNES','Nome UBS','INE','Sigla',periodos_dict[item],'Coluna']
        return df

    except Exception as e:
      print(e)

def obter_cadastros_municipios(visao_equipe,periodo,sessao: Session,teste: bool = False):
    try:
        for k in range(len(ponderacao)):
              df = criacaoDataFrame(extracaoDados(visao_equipe[0][1],ponderacao[k], periodo), visao_equipe[0][0], ponderacao[k],periodo) 
              df_tratado = tratamentoDados(df,visao_equipe[0][0], ponderacao[k],periodo,sessao=sessao)
              carregar_cadastros(cadastros_transformada=df_tratado,sessao=sessao)
              if not teste:
                sessao.commit()
              
    except Exception as e:
      print(e)

visao_equipe=[('todas-equipes','')] 
periodo = '202201'
periodos_list = []
periodos_list.append(periodo)
teste = False

