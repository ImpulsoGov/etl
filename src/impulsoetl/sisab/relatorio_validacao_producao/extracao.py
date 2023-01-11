# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Extrai dados do relatório de validação por ficha por aplicação a partir do SISAB."""

from datetime import date
from io import StringIO
from typing import Final

import pandas as pd
import requests
from prefect import task

from impulsoetl.loggers import habilitar_suporte_loguru, logger
from impulsoetl.sisab.parametros_requisicao import head

FICHA_CODIGOS: Final[dict[str, str]] = {
    "Cadastro Individual": "2",
    "Atendimento Individual": "4",
    "Procedimentos": "7",
    "Visita Domiciliar": "8",
}

APLICACAO_CODIGOS: Final[dict[str, str]] = {
    "CDS-offline": "0",
    "CDS-online": "1",
    "PEC": "2",
    "Sistema proprio": "3",
    "Android ACS": "4",
    "Android AC": "5",
}


def verificar_colunas(df_extraido: pd.DataFrame) -> bool:
    """Verifica se 'Dataframe' possui 7 colunas como esperado"""
    return df_extraido.shape[1] == 7


@task(
    name="Extrair Relatórios de Validação da Produção",
    description=(
        "Extrai os dados dos relatórios de validação da produção a partir do "
        + "portal público do Sistema de Informação em Saúde para a Atenção "
        + "Básica do SUS."
    ),
    tags=["aps", "sisab", "validacao_producao", "extracao"],
    retries=2,
    retry_delay_seconds=120,
)
def extrair_dados(
    periodo_competencia: date,
    envio_prazo: bool,
    ficha: str,
    aplicacao: str,
) -> str:
    """Captura dados do relatório de indicadores do SISAB

    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov.
        periodo_competencia: Data do quadrimestre da competência em referência
        envio_prazo: Indica se os relatórios de validação a serem considerados
        apenas os enviados no prazo (`True`) ou se devem considerar tanto
        envios no prazo quanto fora do prazo (`False`).
        ficha: Nome da ficha requisitada
        aplicacao: Nome da aplicacao requisitada
        url : url do relatório de dados a serem extraídos.

    Retorna:
        Objeto [`pandas.DataFrame`] com os dados enriquecidos e tratados.

            [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
            [`pandas.DataFrame`]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html

    """
    habilitar_suporte_loguru()

    # Captura de cookies e view state para requisição
    url = "https://sisab.saude.gov.br/paginas/acessoRestrito/relatorio/federal/envio/RelValidacao.xhtml"
    hd = head(url)
    view_state = hd[1]
    headers = hd[0]

    # Check box envio requisições no prazo marcado?
    envio_prazo_on = ""
    if envio_prazo:
        envio_prazo_on += "&envioPrazo=on"

    payload = (
        "j_idt44=j_idt44"
        + "&unidGeo=brasil"
        + "&estadoMunicipio="
        + "&periodo=producao"
        + "&j_idt70={:%Y%m}".format(periodo_competencia)
        + "&colunas=ibge"
        + "&colunas=cnes"
        + "&colunas=tp_unidade"
        + "&colunas=ine"
        + "&colunas=tp_equipe"
        + "&j_idt87="
        + FICHA_CODIGOS[ficha]
        + "&j_idt92="
        + APLICACAO_CODIGOS[aplicacao]
        + envio_prazo_on
        + "&javax.faces.ViewState="
        + view_state
        + "&j_idt102=j_idt102"
    )

    logger.info("Iniciando conexão com o SISAB ...")
    response = requests.request(
        "POST", url, headers=headers, data=payload, timeout=680
    )
    logger.info("Criando dataframe com dados extraídos do relatório...")
    df_extraido = pd.read_csv(
        StringIO(response.text),
        delimiter=";",
        header=6,
        encoding="ISO-8859-1",
        engine="python",
        skipfooter=4,
        thousands=".",
        dtype="object",
    )
    df_extraido = df_extraido.drop(["Unnamed: 7"], axis=1)
    logger.info("Verificando a estrutura da tabela...")
    assert verificar_colunas(df_extraido=df_extraido)
    logger.info(
        "Extração do relatório realizada | Total de registros : '{numero_linhas}'",
        numero_linhas=df_extraido.shape[0],
    )

    return df_extraido
