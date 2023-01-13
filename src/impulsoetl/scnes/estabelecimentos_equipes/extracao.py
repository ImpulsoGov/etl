import warnings
warnings.filterwarnings("ignore")
import requests
import pandas as pd
import json

import sys
sys.path.append(r'C:\Users\maira\Impulso\etl\src\impulsoetl')
from scnes.extracao_lista_cnes import extrair_lista_cnes
#from impulsoetl.loggers import logger

codigo_municipio = '120001'
lista_codigos = extrair_lista_cnes(codigo_municipio)

def extrair_equipes(codigo_municipio: str, lista_cnes: list) -> pd.DataFrame:
    
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
        
        except:
            pass

    return df_extraido

    #logger.info("Extração concluída")


#equipes = extrair_equipes(codigo_municipio, lista_codigos)
##equipes = equipes[['municipio_id_sus', 'estabelecimento_cnes_id','coEquipe','coArea']]
#data = equipamentos[['municipio_id_sus','estabelecimento_cnes_id','dsTpEquip','qtExiste','qtUso','tpSus','dsEquipamento']]
#teste = data.loc[data['estabelecimento_cnes_id']=='5701929']
#print("---------------------------------------------------------EQUIPES---------------------------------------------------------")
#print(equipes)

#colunas = ['municipio_id_sus', 'estabelecimento_cnes_id', 'tpEquipe', 'dsEquipe','coEquipe', 'nomeEquipe', 'seqEquipe', 'coArea', 'coMunicipio','dsArea', 'quilombola', 'assentada', 'geral', 'escola', 'pronasci','indigena', 'ribeirinha', 'complem', 'dtAtivacao', 'dtDesativacao']

