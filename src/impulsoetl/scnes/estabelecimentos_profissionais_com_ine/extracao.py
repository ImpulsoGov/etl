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

@task(
    name="Extrair dados dos profissionais de saúde com INE por estabelecimento",
    description=(
        "Realiza a extração dos dados dos profissionais vinculados à alguma equipe de saúde"
        + "a partir da página do CNES"
    ),
    tags=["cnes", "profissionais", "extracao"],
    retries=0,
    retry_delay_seconds=None,
)
def extrair_profissionais_com_ine (
    codigo_municipio:str,
    lista_codigos:list,
    periodo_data_inicio:date
)->pd.DataFrame:
    """
    Extrai informaçãoes dos profissionais de saúde que fazem parte de alguma equipe a partir da página do CNES
     Argumentos:
        codigo_municipio: Id sus do municipio.
        lista_cnes: Lista contento os códigos CNES dos estabelecimentos presentes no município
                    (conforme retornado pela função [`extrair_lista_cnes()`][]).
        periodo_data_inicio: Data da competência 
     Retorna:
        Objeto [`pandas.DataFrame`] com os dados extraídos.
    """

    # Extrai as equipes do municipio
    #lista_cnes = extrair_lista_cnes(codigo_municipio)
    equipes = extrair_equipes(codigo_municipio,lista_codigos,periodo_data_inicio).reset_index()
    linhas = equipes.shape[0]

    df_extraido = pd.DataFrame()

    # Para cada equipe seleciona a seqEquipe (número INE sem os zeros no inicio) e o coArea, e extrai os profissionais vinculados àquela equipe:
    cont = 0
    while cont <= (linhas-1):
        cnes = equipes.loc[cont,'estabelecimento_cnes_id']
        seqEquipe = equipes.loc[cont, 'seqEquipe']
        coArea = equipes.loc[cont, 'coArea']
        coEquipe = equipes.loc[cont, 'coEquipe']
        
        try:
            url = "http://cnes.datasus.gov.br/services/estabelecimentos-equipes/profissionais/"+codigo_municipio+cnes+"?coMun="+codigo_municipio+"&coArea="+coArea+"&coEquipe="+seqEquipe+"&competencia={:%Y%m}".format(periodo_data_inicio)
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
            df['municipio_id_sus'] = codigo_municipio
            df_extraido = df_extraido.append(df)
            cont += 1

        except Exception as e:
            logger.error(e)
            logger.error("Erro ao extrair os profissionais com INE")
            pass
            
    return df_extraido    

