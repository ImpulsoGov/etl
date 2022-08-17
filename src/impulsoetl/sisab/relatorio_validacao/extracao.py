# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Extrai dados do relatório de validação por produção a partir do SISAB."""

from __future__ import annotations
from typing import Final
from datetime import date
from io import StringIO
import pandas as pd
import requests
from impulsoetl.sisab.parametros_requisicao import head
from impulsoetl.loggers import logger


def verificar_colunas (
    df_extraido:pd.DataFrame
    ) -> int:
	""" Verifica se 'Dataframe' possui 5 colunas como esperado"""
	return df_extraido.shape[1] 

def verificar_linhas (
    df_extraido:pd.DataFrame
    ) -> int:
	""" Verifica se 'Dataframe' possui mais do que 5000 registros como esperado"""
	return df_extraido['IBGE'].nunique()

def extrair_dados(
    periodo_competencia:date,
    envio_prazo:bool,
    url = "https://sisab.saude.gov.br/paginas/acessoRestrito/relatorio/federal/envio/RelValidacao.xhtml"
    ) -> str:
    """ Captura dados do relatório de indicadores do SISAB

        Argumentos:
            sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
                acessar a base de dados da ImpulsoGov.
            periodo_competencia: Data do quadrimestre da competência em referência
            envio_prazo: Indica se os relatórios de validação a serem considerados
            apenas os enviados no prazo (`True`) ou se devem considerar tanto
            envios no prazo quanto fora do prazo (`False`).
            url : url do relatório de dados a serem extraídos.

        Retorna:
            Objeto [`pandas.DataFrame`] com os dados enriquecidos e tratados.

                [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
                [`pandas.DataFrame`]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
         
                """
    # Captura de cookies e view state para requisição
    hd = head(url)
    VIEWSTATE=hd[1]
    HEADERS = hd[0]

    # Check box envio requisições no prazo marcado?
    envio_prazo_on = ""
    if envio_prazo:
        envio_prazo_on += "&envioPrazo=on"
    
    PAYLOAD=("j_idt44=j_idt44"
            +"&unidGeo=brasil"
            +"&estadoMunicipio="
            +"&periodo=producao"
            +"&j_idt70={:%Y%m}".format(periodo_competencia)
            +"&colunas=ibge"
            +"&colunas=cnes"
            +"&colunas=ine"
            +"&j_idt87=2"
            +"&j_idt87=4"
            +"&j_idt87=7"
            +"&j_idt87=8"
            +envio_prazo_on
            +"&javax.faces.ViewState="+VIEWSTATE
            +"&j_idt102=j_idt102")

    logger.info("Iniciando conexão com o SISAB ...")
    response = requests.request(
        "POST",
        url, 
        headers=HEADERS, 
        data=PAYLOAD,
        timeout=240
        )
    logger.info("Criando dataframe com dados extraídos do relatório...")
    df_extraido = (
        pd.read_csv(
            StringIO(response.text),
            delimiter=';'
            ,header=5
            ,encoding='ISO-8859-1'
            ,engine="python"
            ,skipfooter=4
            )
        )
    df_extraido = df_extraido.drop(
        ["Unnamed: 5"]
        , axis=1
        )
    logger.info("Verificando a estrutura da tabela...")
    assert verificar_colunas(df_extraido=df_extraido) == 5
    assert verificar_linhas(df_extraido=df_extraido) > 5000
    logger.info(f"Extração do relatório realizada | Total de registros : {df_extraido.shape[0]}")

    return df_extraido

