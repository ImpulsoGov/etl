# SPDX-FileCopyrightText: 2021 ImpulsoGov <contato@impulsogov.org>
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


def consultar_painel_raas(
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
    """Consulta visão das RAAS pré-processadas na banco de dados da Impulso."""

    logger.info(
        "Obtendo RAAS pré-processadas para o painel de saúde mental para o "
        "município de ID {} a partir da competência de {:%m%Y}...",
        unidade_geografica_id_sus,
        periodo_data_inicio,
    )

    tabela_painel_raas = tabelas["saude_mental.painel_raas"]
    requisicao_painel_raas = (
        sessao.query(tabela_painel_raas)
        .filter(tabela_painel_raas.c.municipio_id == unidade_geografica_id_sus)
        .filter(
            tabela_painel_raas.c.competencia_realizacao >= periodo_data_inicio
        )
        .statement
    )
    painel_raas = pd.read_sql(requisicao_painel_raas, sessao.bind)

    logger.info("OK.")
    return painel_raas


def consultar_usuarios_raas_resumo(
    sessao: Session,
    unidade_geografica_id_sus: str,
) -> pd.DataFrame:
    """Obtém os dados cadastrais dos usuários conforme informados nas RAAS."""

    logger.info(
        "Obtendo resumos de dados cadastrais em RAAS para os usuários do "
        "município de ID {}...",
        unidade_geografica_id_sus,
    )

    tabela_usuarios_raas_resumo = tabelas["saude_mental.usuarios_raas_resumo"]
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

    tabela_abandonos = tabelas["saude_mental._usuarios_abandonaram"]
    requisicao_dados_abandonos = (
        sessao.query(tabela_abandonos)
        .filter(tabela_abandonos.c.municipio_id == unidade_geografica_id_sus)
        .filter(tabela_abandonos.c.competencia >= periodo_data_inicio)
        .statement
    )
    abandonos = pd.read_sql(requisicao_dados_abandonos, sessao.bind)

    logger.info("OK.")
    return abandonos
