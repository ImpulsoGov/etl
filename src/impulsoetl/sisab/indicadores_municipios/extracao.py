# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Extrai dados de indicadores de desempenho a partir do SISAB."""


from __future__ import annotations

from datetime import date
from io import StringIO
from typing import Final

import pandas as pd
import requests

from impulsoetl.sisab.parametros_requisicao import head

INDICADORES_CODIGOS: Final[dict[str, str]] = {
    "Pré-Natal (6 consultas)": "1",
    "Pré-Natal (Sífilis e HIV)": "2",
    "Gestantes Saúde Bucal": "3",
    "Cobertura Citopatológico": "4",
    "Cobertura Polio e Penta": "5",
    "Hipertensão (PA Aferida)": "6",
    "Diabetes (Hemoglobina Glicada)": "7",
}

VISOES_EQUIPE_CODIGOS: Final[dict[str, str]] = {
    "todas-equipes": "",
    "equipes-homologadas": "|HM|",
    "equipes-validas": "|HM|NC|",
}


def _extrair_indicadores(
    indicador: str, visao_equipe: str, quadrimestre: date
) -> str:
    url = "https://sisab.saude.gov.br/paginas/acessoRestrito/relatorio/federal/indicadores/indicadorPainel.xhtml"
    hd = head(url)
    vs = hd[1]
    payload = (
        "j_idt50=j_idt50"
        "&coIndicador="
        + INDICADORES_CODIGOS[indicador]
        + "&selectLinha=ibge"
        + "&quadrimestre={:%Y%m}".format(quadrimestre)
        + "&visaoEquipe="
        + VISOES_EQUIPE_CODIGOS[visao_equipe]
        + "&javax.faces.ViewState="
        + vs
        + "&j_idt84=j_idt84"
    )
    headers = hd[0]
    response = requests.request("POST", url, headers=headers, data=payload,timeout=120)
    return response.text


def extrair_indicadores(
    visao_equipe: str, quadrimestre: date, indicador: str
) -> pd.DataFrame:

    resposta = _extrair_indicadores(
        visao_equipe=visao_equipe,
        quadrimestre=quadrimestre,
        indicador=indicador,
    )

    df = pd.read_csv(
        StringIO(resposta), delimiter="\t", header=None, engine="python"
    )
    dados = df.iloc[11:-4]
    df = pd.DataFrame(data=dados)
    df = df[0].str.split(";", expand=True)
    df.columns = [
        "uf",
        "ibge",
        "municipio",
        "numerador",
        "denominador_informado",
        "denominador_estimado",
        "nota",
        "coluna",
    ]
    return df
