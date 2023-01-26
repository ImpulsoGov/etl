import warnings

warnings.filterwarnings("ignore")
import json
from datetime import date
from prefect import task

import pandas as pd
import requests

from impulsoetl.loggers import logger, habilitar_suporte_loguru
from impulsoetl.scnes.extracao_lista_cnes import extrair_lista_cnes

@task(
    name="Extrair Informações das Equipes",
    description=(
        "Extrai os dados das equipes dos estabelecimentos de saúde"
        + "a partir da página do CNES."
    ),
    tags=["cnes", "equipes", "extracao"],
    retries=2,
    retry_delay_seconds=120,
)
def extrair_equipes(codigo_municipio: str, lista_cnes: list, periodo_data_inicio:date) -> pd.DataFrame:
    """
    Extrai informaçãoes das equipes de saúde dos estabelecimentos a partir da página do CNES
     Argumentos:
        codigo_municipio: Id sus do municipio.
        lista_cnes: Lista contento os códigos CNES dos estabelecimentos presentes no município
                    (conforme retornado pela função [`extrair_lista_cnes()`][]).
        periodo_data_inicipio: Data da competência atual
     Retorna:
        Objeto [`pandas.DataFrame`] com os dados extraídos.
    """
    habilitar_suporte_loguru()
    logger.info("Iniciando extração das equipes ...")
    df_extraido = pd.DataFrame()

    for cnes in lista_cnes:

        try:

            url = ("https://cnes.datasus.gov.br/services/estabelecimentos-equipes/{}{}?competencia={:%Y%m}".format(codigo_municipio,cnes,periodo_data_inicio))

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
            
        except json.JSONDecodeError:
            logger.info("Erro ao tentar extrair equipes para o estabelecimento " + cnes)
            pass
    
    logger.info("Equipes do município " +  codigo_municipio + " extraídas com sucesso ...")


    return df_extraido



