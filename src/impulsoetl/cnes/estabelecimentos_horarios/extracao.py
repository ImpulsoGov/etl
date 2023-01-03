import requests
import pandas as pd
import json

import sys

sys.path.append (r'C:\Users\maira\Impulso\etl\src\impulsoetl')
from cnes.extracao_lista_cnes import extrair_lista_cnes

def extrair_horario_atendimento_estabelecimentos(coMun:str, lista_cnes:list) -> pd.DataFrame:
    """
    Extrai os horários de funcionamento dos estabelecimentos de saúde ATIVOS presentes no município
    
    Argumentos:
        coMun: Id sus do município
        lista_cnes: Lista com os códigos CNES dos estabelecimentos presentes no município
    
    Retorna:
        Dataframe contendo os dias e horários de funcionamento dos estabelecimentos de saúde
    """

    colunas_horario = ['diaSemana','hrInicioAtendimento','municipio_id_sus','estabelecimento_cnes_id']
    df_horario = pd.DataFrame(columns=colunas_horario)

    for cnes in lista_cnes:
        url = "http://cnes.datasus.gov.br/services/estabelecimentos/atendimento/"+coMun+cnes
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
        df['estabelecimento_cnes_id']=cnes
        df_horario= df_horario.append(df)


    return df_horario



#coMun = '120001'
#lista_codigos = extrair_lista_cnes(coMun)

#data = extrair_horario_atendimento_estabelecimentos(coMun,lista_codigos)
#print(data)

