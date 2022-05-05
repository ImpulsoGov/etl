# SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


from __future__ import annotations

import urllib
from datetime import date
from io import StringIO
from typing import Final

import pandas as pd
import requests

from impulsoetl.sisab.cadastros_individuais.parametros_requisicao import head


VISOES_EQUIPE_CODIGOS: Final[dict[str, str]] = {
    "todas-equipes": "",
    "equipes-homologadas": "|HM|",
    "equipes-validas": "|HM|NC|AQ|",
}


def _extrair_cadastros_individuais(
    visao_equipe: str,
    com_ponderacao: bool,
    competencia: date,
) -> str:

    url = (
        "https://sisab.saude.gov.br/paginas/acessoRestrito/relatorio/federal"
        + "/indicadores/indicadorCadastro.xhtml"
    )
    hd = head(url)
    vs = hd[1]
    ponderacao = "&beneficiarios=on" if com_ponderacao else ""
    visao_equipe_codigos = urllib.parse.quote(
        VISOES_EQUIPE_CODIGOS[visao_equipe],
    )
    headers = hd[0]
    payload = (
        "j_idt44=j_idt44&selectLinha=cnes_ine&opacao-capitacao="
        + visao_equipe_codigos
        + ponderacao
        + "&competencia={:%Y%m}".format(competencia)
        + "&javax.faces.ViewState="
        + vs
        + "&j_idt83=j_idt83"
    )
    response = requests.request("POST", url, headers=headers, data=payload)
    return response.text


def extrair_cadastros_individuais(
    visao_equipe: str,
    com_ponderacao: bool,
    competencia: date,
) -> pd.DataFrame:

    resposta = _extrair_cadastros_individuais(
        visao_equipe=visao_equipe,
        com_ponderacao=com_ponderacao,
        competencia=competencia,
    )

    df = pd.read_csv(
        StringIO(resposta),
        delimiter="\t",
        header=None,
        engine="python",
    )

    if not com_ponderacao:
        if visao_equipe == "todas-equipes":
            dados = df.iloc[8:-4]
            df = pd.DataFrame(data=dados)
            df = df[0].str.split(";", expand=True)
            df.columns = [
                "Uf",
                "IBGE",
                "Municipio",
                "CNES",
                "Nome UBS",
                "INE",
                "Sigla",
                "quantidade",
                "Parametro",
            ]
        else:
            dados = df.iloc[9:-4]
            df = pd.DataFrame(data=dados)
            df = df[0].str.split(";", expand=True)
            df.columns = [
                "Uf",
                "IBGE",
                "Municipio",
                "CNES",
                "Nome UBS",
                "INE",
                "Sigla",
                "quantidade",
                "Parametro",
                "Coluna",
            ]
    else:
        if visao_equipe == "todas-equipes":
            dados = df.iloc[9:-4]
            df = pd.DataFrame(data=dados)
            df = df[0].str.split(";", expand=True)
            df.columns = [
                "Uf",
                "IBGE",
                "Municipio",
                "CNES",
                "Nome UBS",
                "INE",
                "Sigla",
                "quantidade",
                "Coluna",
            ]
        else:
            dados = df.iloc[10:-4]
            df = pd.DataFrame(data=dados)
            df = df[0].str.split(";", expand=True)
            df.columns = [
                "Uf",
                "IBGE",
                "Municipio",
                "CNES",
                "Nome UBS",
                "INE",
                "Sigla",
                "quantidade",
                "Coluna",
            ]
    return df
