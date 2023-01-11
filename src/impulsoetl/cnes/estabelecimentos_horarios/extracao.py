import requests
import pandas as pd
import json

from impulsoetl.loggers import logger
from impulsoetl.cnes.extracao_lista_cnes import extrair_lista_cnes

def extrair_horario_atendimento_estabelecimentos(
    codigo_municipio:str, 
    lista_cnes:list
) -> pd.DataFrame:
    """
    Extrai os horários de funcionamento dos estabelecimentos de saúde ATIVOS presentes no município
    
    Argumentos:
        coMun: Id sus do município
        lista_cnes: Lista com os códigos CNES dos estabelecimentos presentes no município
    
    Retorna:
        Objeto [`pandas.DataFrame`] com os dados extraídos.
    """

    df_horario = pd.DataFrame()
    
    logger.info(
        "Iniciando a extração das informações dos horários dos estabelecimentos do município: "
        + codigo_municipio
    )
    for cnes in lista_cnes:
        try:
            url = (
                "http://cnes.datasus.gov.br/services/estabelecimentos/atendimento/"
                +codigo_municipio
                +cnes
            )
            payload={}
            headers = {
                'Accept': 'application/json, text/plain, */*',
                'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
                'Connection': 'keep-alive',
                'Referer': 'http://cnes.datasus.gov.br/pages/estabelecimentos/',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36'
            }
    
            response = requests.request(
                "GET", url, headers=headers, data=payload
            )
            res = response.text
    
            parsed = json.loads(res)
            df = pd.DataFrame(parsed)
            df['municipio_id_sus']=codigo_municipio
            df['estabelecimento_cnes_id']=cnes
            df_horario= df_horario.append(df)
        
        except:
            pass


    return df_horario



#codigo_municipio = '120001'
#lista_codigos = extrair_lista_cnes(codigo_municipio)

#data = extrair_horario_atendimento_estabelecimentos(codigo_municipio,lista_codigos)
#print(data)

