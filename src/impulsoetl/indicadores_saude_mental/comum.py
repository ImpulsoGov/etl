# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Consolida as informações de RAAS para um usuário em um estabelecimento."""


from __future__ import annotations

from datetime import datetime, timedelta, timezone

import janitor  # noqa: F401  # nopycln: import
import pandas as pd
from sqlalchemy.orm import Session

from impulsoetl.bd import tabelas
from impulsoetl.loggers import logger

tabela_raas = tabelas["saude_mental.painel_raas"]
tabela_usuarios_raas_resumo = tabelas["saude_mental.usuarios_raas_resumo"]
tabela_abandonos = tabelas["saude_mental._usuarios_abandonaram"]


def primeiro_com_info(serie: pd.Series):
    for elemento in serie:
        if pd.notna(elemento) and elemento != "Sem informação":
            return elemento
    return serie.iloc[0]


def consultar_raas(
    sessao: Session,
    unidade_geografica_id_sus: str,
    periodo_data_inicio: datetime = datetime(
        # data de início do uso das RAAS
        2013,
        1,
        1,
        tzinfo=timezone(-timedelta(hours=3)),
    ),
) -> pd.DataFrame:
    logger.info(
        "Obtendo arquivos de disseminação da RAAS do banco da Impulso...",
    )
    requisicao_dados_raas = (
        sessao.query(tabela_raas)
        .filter(tabela_raas.c.municipio_id == unidade_geografica_id_sus)
        .filter(
            tabela_raas.c.realizacao_periodo_data_inicio >= periodo_data_inicio
        )
        .statement
    )
    raas = pd.read_sql(requisicao_dados_raas, sessao.bind)
    logger.info("OK.")
    return raas


def consultar_usuarios_raas_resumo(
    sessao: Session,
    unidade_geografica_id_sus: str,
) -> pd.DataFrame:
    logger.info(
        "Obtendo resumos das RAAS para os usuários do município de ID {} "
        "na competência de {:%m%Y}...",
        unidade_geografica_id_sus,
    )
    requisicao_dados_usuario_raas_resumo = (
        sessao.query(tabela_usuarios_raas_resumo)
        .filter(
            tabela_usuarios_raas_resumo.c.municipio_id
            == unidade_geografica_id_sus
        )
        .statement
    )
    usuarios_raas_resumo = pd.read_sql(
        requisicao_dados_usuario_raas_resumo,
        sessao.bind,
    )
    logger.info("OK.")
    return usuarios_raas_resumo


def consultar_abandonos(
    sessao: Session,
    unidade_geografica_id_sus: str,
    periodo_data_inicio: datetime,
) -> pd.DataFrame:
    logger.info(
        "Obtendo dados de abandonos em CAPS do banco da Impulso...",
    )
    requisicao_dados_abandonos = (
        sessao.query(tabela_abandonos)
        .filter(tabela_abandonos.c.municipio_id == unidade_geografica_id_sus)
        .filter(tabela_abandonos.c.competencia >= periodo_data_inicio)
        .statement
    )
    abandonos = pd.read_sql(requisicao_dados_abandonos, sessao.bind)
    logger.info("OK.")
    return abandonos
