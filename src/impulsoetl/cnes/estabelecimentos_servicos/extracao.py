import warnings
warnings.filterwarnings("ignore")
import requests
import pandas as pd
import json

import sys
sys.path.append(r'C:\Users\maira\Impulso\etl\src\impulsoetl')
from cnes.extracao_lista_cnes import extrair_lista_cnes
#from impulsoetl.loggers import logger


def extrair_servicos_estabelecimentos(codigo_municipio: str, lista_cnes: list) -> pd.DataFrame:
    
    df_atividades = pd.DataFrame()

    for l in lista_cnes:

        try:

            url = "http://cnes.datasus.gov.br/services/estabelecimentos/servicos-classificacao/"+coMun+l
            #url = "http://cnes.datasus.gov.br/services/estabelecimentos/servicos-especializados/"+coMun+l

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
            df['municipio_id_sus']=coMun
            df['estabelecimento_cnes_id']=l
            df_atividades = df_atividades.append(df)
        
        except:
            pass

    return df_atividades

    #logger.info("Extração concluída")

    return df_extraido


coMun = '120001'
lista_codigos = extrair_lista_cnes(coMun)
data = extrair_servicos_estabelecimentos(coMun, lista_codigos)
print(data)