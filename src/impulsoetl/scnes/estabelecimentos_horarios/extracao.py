import warnings

warnings.filterwarnings("ignore")
import json

import pandas as pd
import requests
import sys
import json

from datetime import date
from prefect import task

from impulsoetl.scnes.extracao_lista_cnes import extrair_lista_cnes
from impulsoetl.loggers import logger, habilitar_suporte_loguru

@task(
    name="Extrair Horários de Funcionamento dos Estabelecimentos de Saúde",
    description=(
        "Extrai os dados dos horários de funcionamento dos estabelecimentos de saúde"
        + "a partir da página do CNES."
    ),
    tags=["cnes", "equipes", "extracao"],
    retries=2,
    retry_delay_seconds=120,
)
def extrair_horarios_estabelecimentos(
    codigo_municipio: str, lista_cnes: list, periodo_data_inicio: date
) -> pd.DataFrame:
    """
    Extrai os horários de funcionamento dos estabelecimentos de saúde ATIVOS presentes no município

    Argumentos:
        coMun: Id sus do município
        lista_cnes: Lista com os códigos CNES dos estabelecimentos presentes no município
        periodo_data_inicio: Data da competência

    Retorna:
        Objeto [`pandas.DataFrame`] com os dados extraídos.
    """

    habilitar_suporte_loguru()
    logger.info(
        "Iniciando a extração dos horários dos estabelecimentos do município: "
        + codigo_municipio
    )

    df_extraido = pd.DataFrame()

    for cnes in lista_cnes:
        try:
            url = (
                "http://cnes.datasus.gov.br/services/estabelecimentos/atendimento/"
                + codigo_municipio
                + cnes
                + "?competencia={:%Y%m}".format(periodo_data_inicio)
            )
            payload = {}
            headers = {
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
                "Connection": "keep-alive",
                "Referer": "http://cnes.datasus.gov.br/pages/estabelecimentos/",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36",
            }

            response = requests.request(
                "GET", url, headers=headers, data=payload
            )
            res = response.text

            parsed = json.loads(res)
            df = pd.DataFrame(parsed)
            df["municipio_id_sus"] = codigo_municipio
            df["estabelecimento_cnes_id"] = cnes
            df_extraido = df_extraido.append(df)

            if  df.empty:
                try:
                    url = 'https://cnes.datasus.gov.br/services/estabelecimentos/'+codigo_municipio+cnes+"?competencia={:%Y%m}".format(periodo_data_inicio)
                    payload = {}
                    headers = {
                        "Accept": "application/json, text/plain, */*",
                        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
                        "Connection": "keep-alive",
                        "Referer": "http://cnes.datasus.gov.br/pages/estabelecimentos/",
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36",
                        }
                    response = requests.request("GET", url, headers=headers, data=payload)
                    res = response.text
                    parsed = json.loads(res)

                    if "tpSempreAberto"  in parsed:
                        horario_sempre_aberto = parsed.get("tpSempreAberto")     
                    if horario_sempre_aberto == "S":
                        df_parcial = pd.DataFrame()
                        df_parcial['municipio_id_sus'] = [codigo_municipio]
                        df_parcial['estabelecimento_cnes_id'] = [cnes]
                        df_parcial['diaSemana'] = 'Sempre Aberto'
                        df_parcial['hrInicioAtendimento'] = ""
                        df_parcial['hrFimAtendimento'] = ""
                        df_extraido = df_extraido.append(df_parcial)
                
                except json.JSONDecodeError:
                    pass  

        except json.JSONDecodeError:
            logger.info(
                "Não foi possível extrair os horarios para o estabelecimento: "
                + cnes
            )
            pass

    return df_extraido
