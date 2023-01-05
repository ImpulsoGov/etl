"""Extrai a lista dos códigos dos estabelecimentos do município a partir da página do CNES"""
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import requests
import pandas as pd
import json




def extrair_lista_cnes(codigo_municipio: str) -> list: 
       
    url = "http://cnes.datasus.gov.br/services/estabelecimentos"+"?municipio="+codigo_municipio
    payload={}
    headers = {
      'Accept': 'application/json, text/plain, */*',
      'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
      'Connection': 'keep-alive',
      'Referer': 'http://cnes.datasus.gov.br/pages/estabelecimentos/',
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36'
    }
    
    response = requests.request("GET", url, headers=headers, data=payload)
    res = response.text
    lista = json.loads(res)

    colunas = ['id', 'cnes', 'noFantasia','noEmpresarial','uf','noMunicipio','gestao','natJuridica','atendeSus']
    df_consolidado = pd.DataFrame(columns=colunas)
    

    for i in lista:
        parsed = {k:[v] for k,v in i.items()}  
        df = pd.DataFrame(parsed)
        df_consolidado = df_consolidado.append(df)
        lista_cnes = df_consolidado['cnes'].value_counts().index.tolist()
    
    return lista_cnes

#coMun = '120001'
#data = extrair_lista_cnes(coMun)
#print(data)