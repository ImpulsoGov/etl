import warnings

warnings.filterwarnings("ignore")
import json

import numpy as np
import pandas as pd
import requests

from impulsoetl.cnes.extracao_lista_cnes import extrair_lista_cnes
from impulsoetl.loggers import logger


def tratar_ficha_vazia(cnes: str, codigo_municipio: str) -> pd.DataFrame:

    colunas = [
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

    dados = list()

    for coluna in colunas:
        dados.append(np.nan)

    df = dict(zip(colunas, dados))
    df_sem_ficha = pd.DataFrame([df])

    df_sem_ficha["noFantasia"] = "NAO IDENTIFICADO"
    df_sem_ficha["cnes"] = cnes
    df_sem_ficha["municipio"] = codigo_municipio

    return df_sem_ficha


def extrair_informacoes_estabelecimentos(
    codigo_municipio: str, lista_cnes: list
) -> pd.DataFrame:

    df_extraido = pd.DataFrame()

    logger.info(
        "Iniciando a extração das informações dos estabelecimentos do município: "
        + codigo_municipio
    )

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
