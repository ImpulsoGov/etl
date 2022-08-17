# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


import urllib
from io import StringIO

import pandas as pd
import requests
from bs4 import BeautifulSoup as bs


def get_cookie(url):
    resposta = requests.get(url, timeout=120)
    ck = resposta.cookies.get_dict()
    soup = bs(resposta.text, "html.parser")
    vs = urllib.parse.quote(soup.findAll("input")[1].attrs["value"])
    cookie = (
        "BIGipServerpool_sisab_jboss="
        + ck["BIGipServerpool_sisab_jboss"]
        + ";JSESSIONID="
        + ck["JSESSIONID"]
    )
    return cookie, vs


def head(url):
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
        "Cookie": cookies[0],
    }
    return headers, vs


def save_csv(rp, rel, ind, eq, quad):
    df = pd.read_csv(StringIO(rp), sep="\t", header=None)
    df.columns = df.loc[10]
    df = df.iloc[11:-4]
    src = "/home/silas/Documentos/Impulso"
    path = src + ind + "-" + eq + "-" + quad + ".csv"
    df.to_csv(path, index=False, encoding="utf-8")
    print("Salvo: " + path)
