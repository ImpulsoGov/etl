import warnings

warnings.filterwarnings("ignore")
import json
from datetime import date

import pandas as pd
import requests
#from prefect import task

from impulsoetl.loggers import habilitar_suporte_loguru, logger
from impulsoetl.scnes.estabelecimentos_equipes.extracao import extrair_equipes
from impulsoetl.scnes.extracao_lista_cnes import extrair_lista_cnes


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
    df_extraido = pd.DataFrame()

    lista_codigos = extrair_lista_cnes(codigo_municipio)
    equipes = extrair_equipes(codigo_municipio, lista_codigos, periodo_data_inicio)

    for cnes in lista_codigos:
        equipes_cnes = equipes.loc[equipes['estabelecimento_cnes_id']==cnes]
        codigos_equipe_area = dict(zip(equipes_cnes['seqEquipe'],equipes_cnes['coArea']))
        codigos_equipe_cnes =  dict(zip(equipes_cnes['seqEquipe'],equipes_cnes['coEquipe']))

        for seqEquipe in codigos_equipe_area:
            coArea = codigos_equipe_area[seqEquipe]
            for coEquipe in codigos_equipe_cnes:
                coEquipe = codigos_equipe_cnes[coEquipe]

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

                except json.JSONDecodeError:
                    logger.error("Erro ao extrair os profissionais com INE")
                    pass
            
    return df_extraido    

@task(
    name="Extrair Informações dos Profissionais de Saúde",
    description=(
        "Extrai os dados dos profisisonais de saúde dos estabelecimentos de cada município"
        + "a partir da página do CNES."
    ),
    tags=["cnes", "profissionais", "extracao"],
    retries=2,
    retry_delay_seconds=120,
)
def extrair_profissionais (
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

    df_parcial = pd.DataFrame()

    for cnes in lista_codigos:
        try:
            url = ("http://cnes.datasus.gov.br/services/estabelecimentos-profissionais/"+codigo_municipio+cnes+"?competencia={:%Y%m}".format(periodo_data_inicio))
            print(url)
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

            df_parcial = df_parcial.append(df)

            if df.empty:
                colunas = ['tpSusNaoSus', 'cbo', 'dsCbo', 'chOutros', 'chAmb', 'chHosp','vinculacao', 'vinculo', 'subVinculo', 'nome', 'cns', 'cnsMaster','artigo2', 'artigo3', 'artigo5', 'dtEntrada']
                df_sem_profissionais = pd.DataFrame(columns = colunas)
                df_sem_profissionais['municipio_id_sus'] = [codigo_municipio]
                df_sem_profissionais['estabelecimento_cnes_id'] = [cnes]

                df_parcial = df_parcial.append(df_sem_profissionais)

        except json.JSONDecodeError:
            logger.error("Erro ao extrair os profissionais")
            pass
    
    df_ine = extrair_profissionais_com_ine(codigo_municipio=codigo_municipio,lista_codigos=lista_codigos, periodo_data_inicio=periodo_data_inicio)
    df_ine = df_ine[['estabelecimento_cnes_id','INE','dtEntrada','dtDesligamento','cns','municipio_id_sus']]
    df_consolidado = pd.merge(df_parcial, df_ine, how='outer', on=['cns','estabelecimento_cnes_id','municipio_id_sus'])

    logger.info("Extração concluída ...")

    return df_consolidado

