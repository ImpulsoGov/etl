import warnings

warnings.filterwarnings("ignore")
import json
from datetime import date

import pandas as pd
import requests
from prefect import task

from impulsoetl.loggers import habilitar_suporte_loguru, logger
from impulsoetl.scnes.estabelecimentos_equipes.extracao import extrair_equipes
from impulsoetl.scnes.extracao_lista_cnes import extrair_lista_cnes


"""
@task(
    name="Extrair Informações dos Profissionais de Saúde",
    description=(
        "Extrai os dados dos profisisonais de saúde dos estabelecimentos de cada município"
        + "a partir da página do CNES."
    ),
    tags=["cnes", "profissionais", "extracao"],
    retries=2,
    retry_delay_seconds=120,
)"""
def extrair_profissionais_totais (
    codigo_municipio:str, 
    lista_codigos:list, 
    periodo_data_inicio:date
)-> pd.DataFrame:
    """
    Extrai informaçãoes dos profissionais de saúde dos estabelecimentos a partir da página do CNES
     Argumentos:
        codigo_municipio: Id sus do municipio.
        lista_cnes: Lista contento os códigos CNES dos estabelecimentos presentes no município
                    (conforme retornado pela função [`extrair_lista_cnes()`][]).
        periodo_data_inicio: Data da competência 
     Retorna:
        Objeto [`pandas.DataFrame`] com os dados extraídos.
    """
    habilitar_suporte_loguru()
    logger.info("Iniciando extração dos profissionais ...")

    df_consolidado = pd.DataFrame()

    for cnes in lista_codigos:
        try:
            url = ("http://cnes.datasus.gov.br/services/estabelecimentos-profissionais/"+codigo_municipio+cnes+"?competencia={:%Y%m}".format(periodo_data_inicio))

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

            df_consolidado = df_consolidado.append(df)

        except json.JSONDecodeError:
            logger.error("Erro ao extrair os profissionais")
            pass

    logger.info("Extração concluída ...")

    return df_consolidado