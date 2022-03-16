
from email import header
import requests
import urllib
from parametros_requisicao import head
import pandas as pd
from io import StringIO
#from tratamento import tratamentoDados


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

def criacaoDataFrame(rp,rel,ind,eq,quad, pond):
    try:
      df = pd.read_csv(StringIO(rp), sep='\t', header=None)

      if pond == False:
        if eq == 'todas-equipes':
          df.columns = df.loc[7]
          df = df.iloc[8:-4]
        else:
          df.columns = df.loc[8]
          df = df.iloc[9:-4]
        path = ind+"-"+eq+"-"+quad+".csv"       
        df.to_csv(path, index=False, encoding='utf-8')
      

      else: # então True
        if eq == 'todas-equipes':
          df.columns = df.loc[8]
          df = df.iloc[9:-4]
        else:
          df.columns = df.loc[9]
          df = df.iloc[10:-4]
        path = ind+"-"+eq+"-"+quad+".csv"       
        df.to_csv(path, index=False, encoding='utf-8')

      #tratamentoDados()

      
    except Exception as e:
      print(e)

def main(periodo_list):
    try:
        for i in range(len(visao_equipe)):
            for k in range(len(ponderacao)):
                name = 'ponderacao-' + str(ponderacao[k])
                joined_string = ",".join(periodo_list)
                criacaoDataFrame(extracaoDados(visao_equipe[i][1],ponderacao[k], periodo_list),'cadastro', name, visao_equipe[i][0], joined_string, ponderacao[k])
                
                
    except Exception as e:
      print(e)

# periodos_list = ['201804', '201808', '201812', '201904', '201908', '201912', '202004', '202008', '202012','202101', '202102', '202103', '202104', '202105', '202106','202107', '202108', '202109', '202110', '202111']
periodos_list = ['202201']
main(periodos_list)