# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


""" Extrai relatório de financiamento a partir do e-Gestor."""


from __future__ import annotations

from datetime import date
from functools import lru_cache

import requests
from bs4 import BeautifulSoup
from frozenlist import FrozenList

from impulsoetl.loggers import logger


MESES: FrozenList[str] = FrozenList([
    "JAN",
    "FEV",
    "MAR",
    "ABR",
    "MAI",
    "JUN",
    "JUL",
    "AGO",
    "SET",
    "OUT",
    "NOV",
    "DEZ",
])

URL_BASE = "https://egestorab.saude.gov.br"


@lru_cache(12)
def extrair(periodo_mes: date) -> bytes:
    logger.info(
        "Iniciando extração de dados de financiamento para o mês de {:%m/%Y}",
        periodo_mes,
    )
    pagina_consulta_caminho = "/gestaoaps/relFinanciamentoParcela.xhtml"
    cabecalhos = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "pt-BR,pt;q=0.9",
        "Connection": "keep-alive",
        "Host": "egestorab.saude.gov.br",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Sec-GPC": "1",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.5112.102 Safari/537.36",
    }

    logger.info("Iniciando sessão do e-Gestor...")
    with requests.Session() as sessao:
        sessao.headers=cabecalhos
        url_consulta = URL_BASE + pagina_consulta_caminho
        pagina_consulta = sessao.get(url_consulta)

        logger.info("Lendo o formulário de definição do relatório...")
        pagina_consulta_processada = BeautifulSoup(
            pagina_consulta.text,
            "lxml",
        )
        formulario = pagina_consulta_processada.find("form", id="j_idt58")
        javax_view_state = formulario.find(
            "input",
            id="javax.faces.ViewState",
        )["value"]

        logger.info("Selecionando UFs...")
        payload = {
            "j_idt58": "j_idt58",
            "javax.faces.ViewState": javax_view_state,
            "j_idt58:uf": "00",
            "j_idt58:municipio": "99",
            "j_idt58:ano": "99",
            "j_idt58:compInicio": "99",
            "j_idt58:compFim": "99",
            "javax.faces.source": "j_idt58:uf",
            "javax.faces.partial.event": "change",
            "javax.faces.partial.execute": "j_idt58:uf j_idt58:uf",
            "javax.faces.partial.render": (
                "j_idt58:municipio j_idt58:tela j_idt58:compInicio "
                + "j_idt58:competenciaFim j_idt58:ano"
            ),
            "javax.faces.behavior.event": "valueChange",
            "javax.faces.partial.ajax": "true",
        }
        sessao.headers.update({
            "Accept": "*/*",
            "Faces-Request": "partial/ajax",
            "Origin": URL_BASE,
            "Referer": url_consulta,
            "Cache-Control": "max-age=0",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "Sec-GPC": "1",
        })
        del sessao.headers["Sec-Fetch-User"]
        del sessao.headers["Upgrade-Insecure-Requests"]
        relatorio_caminho = URL_BASE + formulario["action"]
        requisicao_selecionar_uf = requests.Request(
            "POST",
            relatorio_caminho,
            data=payload,
        )
        requisicao_selecionar_uf_preparada = sessao.prepare_request(
            requisicao_selecionar_uf,
        )
        requisicao_selecionar_uf_resposta = sessao.send(
            requisicao_selecionar_uf_preparada,
        )
        formulario = BeautifulSoup(
            requisicao_selecionar_uf_resposta.text,
            "xml",
        )
        javax_view_state = formulario.find(id="javax.faces.ViewState").text

        logger.info("Selecionando municípios e ano de referência...")
        payload.update({
            "javax.faces.ViewState": javax_view_state,
            "j_idt58:municipio": "00",
            "j_idt58:ano": "{:%Y}".format(periodo_mes),
            "javax.faces.source": "j_idt58:ano",
            "javax.faces.partial.execute": "j_idt58:ano j_idt58:ano",
            "javax.faces.partial.render": (
                "j_idt58:compInicio j_idt58:compFim j_idt58:visualizacao"
            ),
        })
        del payload["j_idt58:compFim"]
        requisicao_selecionar_municipio = requests.Request(
            "POST",
            relatorio_caminho,
            data=payload,
        )
        requisicao_selecionar_municipio_preparada = sessao.prepare_request(
            requisicao_selecionar_municipio,
        )
        requisicao_selecionar_municipio_resposta = sessao.send(
            requisicao_selecionar_municipio_preparada,
        )
        formulario = BeautifulSoup(
            requisicao_selecionar_municipio_resposta.text,
            "xml",
        )
        javax_view_state = formulario.find(id="javax.faces.ViewState").text

        logger.info("Selecionando competência de referência...")
        payload.update({
            "javax.faces.ViewState": javax_view_state,
            "j_idt58:compInicio": "{:%Y%m}".format(periodo_mes),
            "javax.faces.partial.execute": (
                "j_idt58:compInicio j_idt58:compInicio"
            ),
            "javax.faces.partial.render": "j_idt58:compFim",
        })
        requisicao_selecionar_competencia = requests.Request(
            "POST",
            relatorio_caminho,
            data=payload,
        )
        requisicao_selecionar_competencia_preparada = sessao.prepare_request(
            requisicao_selecionar_competencia,
        )
        requisicao_selecionar_competencia_resposta = sessao.send(
            requisicao_selecionar_competencia_preparada,
        )
        formulario = BeautifulSoup(
            requisicao_selecionar_competencia_resposta.text,
            "xml",
        )
        javax_view_state = formulario.find(id="javax.faces.ViewState").text

        logger.info("Iniciando o download do relatório...")
        payload.update({
            "javax.faces.ViewState": javax_view_state,
            "j_idt58:Download20": "Download",
        })
        del payload["javax.faces.source"]
        del payload["javax.faces.partial.event"]
        del payload["javax.faces.partial.execute"]
        del payload["javax.faces.partial.render"]
        del payload["javax.faces.behavior.event"]
        del payload["javax.faces.partial.ajax"]
        sessao.headers.update({
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;q=0.9,"
                + "image/avif,image/webp,image/apng,*/*;q=0.8"
            ),
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
        })
        del sessao.headers["Faces-Request"]
        requisicao_relatorio = requests.Request(
            "POST",
            relatorio_caminho,
            data=payload,
        )
        requisicao_relatorio_preparada = sessao.prepare_request(
            requisicao_relatorio,
        )
        requisicao_relatorio_resposta = sessao.send(
            requisicao_relatorio_preparada,
        )
        if requisicao_relatorio_resposta.ok:
            logger.info(
                "Download do relatório de financiamento concluído com "
                + "sucesso!",
            )
        else:
            logger.error(
                "Erro ao obter o relatório de financiamento do e-Gestor: "
                + "{codigo} - {motivo}.",
                codigo=requisicao_relatorio_resposta.status_code,
                motivo=requisicao_relatorio_resposta.reason,
            )
            requisicao_relatorio_resposta.raise_for_status()

        return requisicao_relatorio_resposta.content
