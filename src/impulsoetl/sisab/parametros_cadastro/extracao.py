# SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


import urllib
from datetime import date
from io import StringIO
from typing import Final

import pandas as pd
import requests
from prefect import task

from impulsoetl.loggers import habilitar_suporte_loguru
from impulsoetl.sisab.parametros_requisicao import head

VISOES_EQUIPE_CODIGOS: Final[dict[str, str]] = {
    "todas-equipes": "",
    "equipes-homologadas": "|HM|",
    "equipes-validas": "|HM|NC|AQ|",
}

NIVEL_AGREGACAO_CODIGOS: Final[dict[str, str]] = {
    "municipios": "ibge",
    "estabelecimentos_equipes": "cnes_ine",
}


def _extrair_parametros(
    visao_equipe: str,
    competencia: date,
    nivel_agregacao: str,
) -> str:

    url = (
        "https://sisab.saude.gov.br/paginas/acessoRestrito/relatorio/federal"
        + "/indicadores/indicadorCadastro.xhtml"
    )
    hd = head(url)
    vs = hd[1]
    ponderacao = ""
    visao_equipe_codigos = urllib.parse.quote(
        VISOES_EQUIPE_CODIGOS[visao_equipe]
    )
    headers = hd[0]
    payload = (
        "j_idt44=j_idt44&selectLinha="
        + NIVEL_AGREGACAO_CODIGOS[nivel_agregacao]
        + "&opacao-capitacao="
        + visao_equipe_codigos
        + ponderacao
        + "&competencia={:%Y%m}".format(competencia)
        + "&javax.faces.ViewState="
        + vs
        + "&j_idt83=j_idt83"
    )
    response = requests.request(
        "POST",
        url,
        headers=headers,
        data=payload,
        timeout=120,
    )
    return response.text


@task(
    name="Extrair Parâmetros de Cadastro",
    description=(
        "Extrai os dados dos parâmetros de cadastro do Previne Brasil a "
        + "partir do portal público do Sistema de Informação em Saúde para a "
        + "Atenção Básica do SUS."
    ),
    tags=["aps", "sisab", "parametros_cadastro", "extracao"],
    retries=2,
    retry_delay_seconds=120,
)
def extrair_parametros(
    visao_equipe: str,
    competencia: date,
    nivel_agregacao: str,
) -> pd.DataFrame:
    habilitar_suporte_loguru()
    resposta = _extrair_parametros(
        visao_equipe=visao_equipe,
        competencia=competencia,
        nivel_agregacao=nivel_agregacao,
    )

    df = pd.read_csv(
        StringIO(resposta), delimiter="\t", header=None, engine="python"
    )
    dados = df.iloc[9:-4]
    df = pd.DataFrame(data=dados)
    df = df[0].str.split(";", expand=True)
    if nivel_agregacao == "municipios":
        df.columns = [
            "Uf",
            "IBGE",
            "Municipio",
            "quantidade",
            "parametro",
            "coluna",
        ]
    else:
        df.columns = [
            "Uf",
            "IBGE",
            "Municipio",
            "CNES",
            "Nome UBS",
            "INE",
            "Sigla",
            "quantidade",
            "parametro",
            "Coluna",
        ]
    return df
