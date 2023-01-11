# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT

""" Extrai informações dos estabelecimentos de saúde a partir da página do CNES """

import warnings

warnings.filterwarnings("ignore")
import json

import numpy as np
import pandas as pd
import requests
from prefect import task

from impulsoetl.cnes.extracao_lista_cnes import extrair_lista_cnes
from impulsoetl.loggers import habilitar_suporte_loguru, logger

COLUNAS_FICHA_VAZIA = [
    "id",
    "noEmpresarial",
    "natJuridica",
    "natJuridicaMant",
    "cnpj",
    "tpPessoa",
    "nvDependencia",
    "nuAlvara",
    "dtExpAlvara",
    "orgExpAlvara",
    "dsTpUnidade",
    "dsStpUnidade",
    "noLogradouro",
    "nuEndereco",
    "cep",
    "regionalSaude",
    "bairro",
    "noComplemento",
    "municipio",
    "noMunicipio",
    "uf",
    "tpGestao",
    "nuTelefone",
    "tpSempreAberto",
    "coMotivoDesab",
    "dsMotivoDesab",
    "cpfDiretorCln",
    "stContratoFormalizado",
    "nuCompDesab",
    "dtCarga",
    "dtAtualizacaoOrigem",
    "dtAtualizacao",
]


def tratar_ficha_vazia(cnes: str, codigo_municipio: str) -> pd.DataFrame:
    """
    Realiza tratamento para os estabelecimentos cujas fichas contendo as informações estão vazias.

     Argumentos:
        cnes: Código CNES dos estabelecimentos.
        codigo_municipio: Id sus do municipio.

     Retorna:
        Objeto [`pandas.DataFrame`] com os dados extraídos e tratados para os estabelecimendos com ficha vazia.
    """

    dados = list()

    for coluna in COLUNAS_FICHA_VAZIA:
        dados.append(np.nan)

    df = dict(zip(COLUNAS_FICHA_VAZIA, dados))
    df_sem_ficha = pd.DataFrame([df])

    df_sem_ficha["noFantasia"] = "NAO IDENTIFICADO"
    df_sem_ficha["cnes"] = cnes
    df_sem_ficha["municipio"] = codigo_municipio

    return df_sem_ficha


@task(
    name="Extrair Informações dos Estabelecimentos Identificados",
    description=(
        "Extrai os dados dos estabelecimentos de saúde de cada município"
        + "a partir da página do CNES."
    ),
    tags=["cnes", "estabelecimentos", "extracao"],
    retries=2,
    retry_delay_seconds=120,
)
def extrair_informacoes_estabelecimentos(
    codigo_municipio: str, lista_cnes: list
) -> pd.DataFrame:
    """
    Extrai informaçãoes dos estabelecimentos de saúde a partir da página do CNES

     Argumentos:
        codigo_municipio: Id sus do municipio.
        lista_cnes: Lista contento os códigos CNES dos estabelecimentos presentes no município
                    (conforme retornado pela função [`extrair_lista_cnes()`][]).

     Retorna:
        Objeto [`pandas.DataFrame`] com os dados extraídos.
    """
    habilitar_suporte_loguru()
    df_extraido = pd.DataFrame()

    for cnes in lista_cnes:
        try:
            url = (
                "http://cnes.datasus.gov.br/services/estabelecimentos/"
                + codigo_municipio
                + cnes
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
            df_parcial = pd.DataFrame([parsed])

            df_extraido = df_extraido.append(df_parcial)

        except json.JSONDecodeError:
            logger.info(
                "Não foi possível extrair as informações para o estabelecimento: "
                + cnes
            )
            df_extraido = df_extraido.append(
                tratar_ficha_vazia(
                    cnes=cnes, codigo_municipio=codigo_municipio
                )
            )

    return df_extraido
