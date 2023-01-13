import warnings
warnings.filterwarnings("ignore")
import requests
import pandas as pd
import json

import sys
sys.path.append(r'C:\Users\maira\Impulso\etl\src\impulsoetl')
from scnes.extracao_lista_cnes import extrair_lista_cnes
from scnes.estabelecimentos_equipes.extracao import extrair_equipes
#from impulsoetl.loggers import logger


def extrair_profissionais_por_equipe(codigo_municipio,lista_codigos):

    df_extraido = pd.DataFrame()

    lista_codigos = extrair_lista_cnes(codigo_municipio)
    equipes = extrair_equipes(codigo_municipio, lista_codigos)

    for cnes in lista_codigos:
        equipes_cnes = equipes.loc[equipes['estabelecimento_cnes_id']==cnes]
        codigos = dict(zip(equipes_cnes['coEquipe'],equipes['coArea']))
        for coEquipe in codigos:
            coArea = codigos[coEquipe]
            
            url = "http://cnes.datasus.gov.br/services/estabelecimentos-equipes/profissionais/"+codigo_municipio+cnes+"?coMun="+codigo_municipio+"&coArea="+coArea+"&coEquipe="+coEquipe
    
            payload={}
            headers = {
                'Accept': 'application/json, text/plain, */*',
                'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'Connection': 'keep-alive',
            'Referer': 'http://cnes.datasus.gov.br/pages/estabelecimentos/ficha/equipes/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36'
            }
    
            response = requests.request("GET", url, headers=headers, data=payload)
            res = response.text

            parsed = json.loads(res)
            df = pd.DataFrame(parsed)
            df['INE'] = coEquipe
            df['coArea'] = coArea
            df['estabelecimento_cnes_id'] = cnes

            df_extraido = df_extraido.append(df)


    df_extraido =df_extraido[['cns','estabelecimento_cnes_id','coArea','INE']]
    return df_extraido    

def extrair_profissionais_geral (codigo_municipio, lista_codigos):
    df_extraido = pd.DataFrame()

    for cnes in lista_codigos:
        try:
            url = "http://cnes.datasus.gov.br/services/estabelecimentos-profissionais/"+codigo_municipio+cnes
    
            payload={}
            headers = {
                'Accept': 'application/json, text/plain, */*',
                'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'Connection': 'keep-alive',
            'Referer': 'http://cnes.datasus.gov.br/pages/estabelecimentos/ficha/equipes/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36'
            }
    
            response = requests.request("GET", url, headers=headers, data=payload)
            res = response.text

            parsed = json.loads(res)
            df = pd.DataFrame(parsed)
            df['municipio_id_sus'] = codigo_municipio
            df['estabelecimento_cnes_id'] = cnes

            df_extraido = df_extraido.append(df)

        
        except:
            pass
    
    #print(df.columns)
    df_extraido = df_extraido[['nome', 'cns','municipio_id_sus','estabelecimento_cnes_id']]

    return df_extraido


codigo_municipio = '120001'
lista_codigos = extrair_lista_cnes(codigo_municipio)
profissionais_ine = extrair_profissionais_por_equipe(codigo_municipio, lista_codigos)
profissionais_geral = extrair_profissionais_geral(codigo_municipio, lista_codigos)

print(profissionais_ine.info())
print(profissionais_geral.info())
df = pd.merge(profissionais_ine, profissionais_geral, how='outer', on=['cns','estabelecimento_cnes_id'])
print(df.info())

#print("---------------------------------------------------------profissionais por equipe---------------------------------------------------------")
print(df)