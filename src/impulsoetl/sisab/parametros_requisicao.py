# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


# Funções utilizadas para a extração de todos os relatórios.
# **get_cookie:**
# >  *Parametros* : URL do site alvo
# > *Atribuições* : Obter cookies e view state a serem utilizados nas requisições
# **head:**
# > *Parametros* : URL do site alvo
# > *Atribuições* : Gerar dicionario com os headers necessarios à execução da requisição
# **save_csv:**
# > *Parametros* :
# *   ***rp*** - Resposta da requisição (String)
# *   **rel** - Qual o relatório utilizado sendo as opções : 'cadastro' , 'indicadores' , 'produçao' , 'validacao' (String)
# *   **ind** - Indicador utilizado, valido apenas para o relatório de indicadores de desempenho, para os demais utilizar string vazia (String)
# *   **eq** - Visão de equipe, sendo as opções : 'todas-equipes' , 'equipes-homologadas' , 'equipes-validas' (String)
# *   **quad** - Periodo de referencia (String)
# > *Atribuições* : Criar arquivo .csv

import urllib

import requests
from bs4 import BeautifulSoup as bs


def get_cookie(url):
    response = requests.get(url)
    ck = response.cookies.get_dict()
    soup = bs(response.text, "html.parser")
    vs = urllib.parse.quote(soup.findAll("input")[1].attrs["value"])
    return ck, vs


def head(url):
    cookies = get_cookie(url)
    vs = cookies[1]
    headers = {
        "Connection": "keep-alive",
        "Cache-Control": "max-age=0",
        "sec-ch-ua": '" Not A;Brand";v="99", "Chromium";v="96", "Google Chrome";v="96"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "Upgrade-Insecure-Requests": "1",
        "Origin": "https://sisab.saude.gov.br",
        "Content-Type": "application/x-www-form-urlencoded",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
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
