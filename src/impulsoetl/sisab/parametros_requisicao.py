# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Funções utilizadas para a extração de todos os relatórios do SISAB."""


import urllib

import requests
from bs4 import BeautifulSoup as bs


def get_cookie(url) -> tuple[dict[str, str], str]:
    """Obter cookies e *view state* a serem utilizados nas requisições.

    Argumentos:
        url: URL do site alvo.

    Retorna:
        Tupla contendo um dicionário de cookies e uma string correspondente ao
        '*view state*' da página.
    """
    resposta = requests.get(url)
    ck = resposta.cookies.get_dict()
    soup = bs(resposta.text, "html.parser")
    vs = urllib.parse.quote(soup.findAll("input")[1].attrs["value"])
    return ck, vs


def head(url):
    """Gera dicionario com os headers necessarios à execução da requisição"""
    cookies = get_cookie(url)
    vs = cookies[1]
    headers = {
        "Connection": "keep-alive",
        "Cache-Control": "max-age=0",
        "sec-ch-ua": (
            '" Not A;Brand";v="99", "Chromium";v="96", "Google Chrome";v="96"'
        ),
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "Upgrade-Insecure-Requests": "1",
        "Origin": "https://sisab.saude.gov.br",
        "Content-Type": "application/x-www-form-urlencoded",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            + "(KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36"
        ),
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;"
            + "q=0.9,image/avif,image/webp,image/apng,*/*;"
            + "q=0.8,application/signed-exchange;v=b3;q=0.9"
        ),
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-User": "?1",
        "Sec-Fetch-Dest": "document",
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "Cookie": "BIGipServerpool_sisab_jboss="
        + cookies[0]["BIGipServerpool_sisab_jboss"]
        + ";JSESSIONID="
        + cookies[0]["JSESSIONID"],
    }
    return headers, vs
