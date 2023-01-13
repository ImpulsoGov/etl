import warnings
warnings.filterwarnings("ignore")
import requests
import pandas as pd
import json

from impulsoetl.scnes.extracao_lista_cnes import extrair_lista_cnes
from impulsoetl.scnes.estabelecimentos_equipes.extracao import extrair_equipes
#from impulsoetl.loggers import logger


def extrair_profissionais_com_ine (codigo_municipio,lista_codigos):

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
    
    return df_extraido    

def extrair_profissionais (codigo_municipio, lista_codigos):
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
    
    df_ine = extrair_profissionais_com_ine(codigo_municipio,lista_codigos)
    df_ine = df_ine.add_suffix('_INE')
    df_ine = df_ine.rename(
        columns={
            'INE_INE':'INE',
            'estabelecimento_cnes_id_INE': 'estabelecimento_cnes_id',
            'cns_INE':'cns',
            })
    df = pd.merge(df_extraido, df_ine, how='outer', on=['cns','estabelecimento_cnes_id'])

    return df


codigo_municipio = '120025'
lista_codigos = extrair_lista_cnes(codigo_municipio)
data = extrair_profissionais(codigo_municipio, lista_codigos)

#print(data)