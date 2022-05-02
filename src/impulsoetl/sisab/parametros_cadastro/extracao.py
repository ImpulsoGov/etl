# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


from __future__ import annotations

import urllib
from datetime import datetime
from io import StringIO
<<<<<<< HEAD
from datetime import date
=======
from typing import Final

import pandas as pd
import requests

>>>>>>> f19009cf7eaf7e0e5cf4c3b1035b8d06a05c0a6d
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
<<<<<<< HEAD
    visao_equipe: str,
    competencia: date,
    nivel_agregacao: str
) -> str:

=======
    visao_equipe: str, competencia: datetime, nivel_agregacao: str
) -> str:
>>>>>>> f19009cf7eaf7e0e5cf4c3b1035b8d06a05c0a6d
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
    response = requests.request("POST", url, headers=headers, data=payload)
    return response.text


def extrair_parametros(
<<<<<<< HEAD
    visao_equipe: str,
    competencia: date,
    nivel_agregacao: str
=======
    visao_equipe: str, competencia: datetime, nivel_agregacao: str
>>>>>>> f19009cf7eaf7e0e5cf4c3b1035b8d06a05c0a6d
) -> pd.DataFrame:

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
