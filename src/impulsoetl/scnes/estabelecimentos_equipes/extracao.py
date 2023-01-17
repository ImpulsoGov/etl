import warnings
warnings.filterwarnings("ignore")
import requests
import pandas as pd
import json

from impulsoetl.scnes.extracao_lista_cnes import extrair_lista_cnes
from impulsoetl.loggers import logger

def extrair_equipes(codigo_municipio: str, lista_cnes: list) -> pd.DataFrame:
    
    logger.info("Iniciando extração das equipes ...")
    df_extraido = pd.DataFrame()

    for cnes in lista_cnes:

        try:

            url = "http://cnes.datasus.gov.br/services/estabelecimentos-equipes/"+codigo_municipio+cnes

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

            parsed = json.loads(res)
            df = pd.DataFrame(parsed)
            df['municipio_id_sus']=codigo_municipio
            df['estabelecimento_cnes_id']=cnes
            df_extraido = df_extraido.append(df)
            
        except Exception as e:
            print(e)
            pass
    
    logger.info("Equipes do município " +  cnes + "extraídas com sucesso ...")


    return df_extraido


#codigo_municipio = '120025'
#lista_codigos = extrair_lista_cnes(codigo_municipio)
#data = extrair_equipes(codigo_municipio, lista_codigos)
#print(data)


