import warnings
warnings.filterwarnings("ignore")
import requests
import pandas as pd
import json

from impulsoetl.cnes.extracao_lista_cnes import extrair_lista_cnes
from impulsoetl.loggers import logger


def extrair_informacoes_estabelecimentos(codigo_municipio: str, lista_cnes: list) -> pd.DataFrame:
    
    df_extraido = pd.DataFrame()
    
    logger.info("Iniciando a extração da lista dos códigos CNES do município...")
    
    for cnes in lista_cnes:
        url = "http://cnes.datasus.gov.br/services/estabelecimentos/"+codigo_municipio+cnes
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
        df_parcial = pd.DataFrame([parsed])

        df_extraido = df_extraido.append(df_parcial)

    logger.info("Extração concluída")

    return df_extraido


#coMun = '120001'
#lista_codigos = extrair_lista_cnes(coMun)
#data = extrair_informacoes_estabelecimentos(coMun, lista_codigos)
#print(data)