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

from impulsoetl.loggers import logger,habilitar_suporte_loguru
from impulsoetl.sisab.parametros_requisicao import head

VISOES_EQUIPE_CODIGOS: Final[dict[str, str]] = {
    "todas-equipes": "",
    "equipes-homologadas": "|HM|",
    "equipes-validas": "|HM|NC|AQ|",
}

def escapar_texto(visao_equipe:str):
    return (
        urllib.parse.quote(
        VISOES_EQUIPE_CODIGOS[visao_equipe],
        )
    )

def adiciona_parametro_ponderacao(com_ponderacao:bool,payload):
    if com_ponderacao:
        payload = payload + ("&beneficiarios=on")
    else:
        payload
    
    return payload


def extrair_requisicao(
    visao_equipe: str,
    com_ponderacao: bool,
    competencia: date,
) -> str:

    url = (
        "https://sisab.saude.gov.br/paginas/acessoRestrito/relatorio/federal/indicadores/indicadorCadastro.xhtml"
    )

    parametros_requisicaco = head(url)
    headers = parametros_requisicaco[0]
    view_state = parametros_requisicaco[1]

    visao_equipe_codigos = escapar_texto(visao_equipe=visao_equipe)

    ponderacao = "&beneficiarios=on" if com_ponderacao else ""
    payload = (
        "j_idt44=j_idt44&selectLinha=cnes_ine&opacao-capitacao="
        + visao_equipe_codigos
        + ponderacao
        + "&competencia={:%Y%m}".format(competencia)
        + "&javax.faces.ViewState="
        + view_state
        + "&j_idt85=j_idt85"
    )
    
    payload = adiciona_parametro_ponderacao(com_ponderacao=com_ponderacao,payload=payload)
    
    response = requests.request(
        "POST",
        url,
        headers=headers,
        data=payload,
        timeout=120,
    )
    
    return response.text

def definir_posicao_cabecalho(
    visao_equipe:str,
    com_ponderacao:bool,
    ):

    if not com_ponderacao:
        if visao_equipe == "todas-equipes":
            header = 6
        else:
            header = 7
    else:
        if visao_equipe == "todas-equipes":
            header = 7
        else:
            header = 8

    return header

@task(
    name="Extrair Cadastros Individuais",
    description=(
        "Extrai os dados de cadastros individuais a partir do portal público "
        + "do Sistema de Informação em Saúde para a Atenção Básica do SUS."
    ),
    tags=["aps", "sisab", "cadastros_individuais", "extracao"],
    retries=2,
    retry_delay_seconds=120,
)

def extrair_cadastros_individuais(
    visao_equipe: str,
    com_ponderacao: bool,
    competencia: date,
) -> pd.DataFrame:
    """Extrai relatório de Cadastros Individuais do SISAB.

    Argumentos:
        visao_equipe: Indica a situação da equipe considerada para a contagem
            dos cadastros.
        periodo: Referente ao mês/ano de disponibilização do relatório.
        com_ponderacao: Lista de booleanos indicando quais tipos de população
            devem ser filtradas no cadastro - onde `True` indica apenas as
            populações com critério de ponderação e `False` indica todos os
            cadastros. Por padrão, o valor é `[True, False]`, indicando que
            ambas as possibilidades são extraídas do SISAB e carregadas para a
            mesma tabela de destino.

    Retorna:
        Um objeto `pandas.DataFrame` com dados capturados pela requisição.
    """

    habilitar_suporte_loguru()

    resposta = extrair_requisicao(
        visao_equipe=visao_equipe,
        com_ponderacao=com_ponderacao,
        competencia=competencia,
    )
    header = definir_posicao_cabecalho(visao_equipe=visao_equipe,com_ponderacao=com_ponderacao)
    try:
        df_extraido = pd.read_csv(
            StringIO(resposta),
            delimiter=";",
            header=header,
            encoding="ISO-8859-1",
            engine="python",
            skipfooter=4,
            thousands=".",
            dtype="object",
        )
        return df_extraido

    except pd.errors.ParserError:
        logger.error("Data da competência do relatório não está disponível")
    
