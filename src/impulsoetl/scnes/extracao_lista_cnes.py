# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT

"""Extrai a lista dos códigos dos estabelecimentos do município a partir da página do CNES"""
import warnings

warnings.simplefilter(action="ignore", category=FutureWarning)

import json

import pandas as pd
import requests

from impulsoetl.loggers import logger


def extrair_lista_cnes(codigo_municipio: str) -> list:
    """
    Extrai a lista dos códigos CNES dos estabelecimentos presentes no município.
     Argumentos:
      codigo_municipio: Id sus do municipio.
    Retorna:
      Lista contendo os códigos CNES dos estabelecimentos de saúde.
    """

    logger.info(
        "Iniciando a extração da lista dos códigos CNES do município:"
        + codigo_municipio
    )

    df_extraido = pd.DataFrame()

    try:
        url = (
            "http://cnes.datasus.gov.br/services/estabelecimentos"
            + "?municipio="
            + codigo_municipio
        )
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
        df_parcial = pd.DataFrame(parsed)
        df_extraido = df_extraido.append(df_parcial)
        lista_cnes = df_extraido["cnes"].value_counts().index.tolist()

        logger.info(
            "Extração da lista dos códigos CNES do município "
            + codigo_municipio
            + " realizada com sucesso"
        )

    except:
        logger.info(
            "Erro ao realizar a requisição para o municipio: "
            + codigo_municipio
        )
        pass

    return lista_cnes