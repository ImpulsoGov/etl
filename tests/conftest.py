# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


import logging

import pytest
from _pytest.logging import caplog as _caplog  # noqa: F401  # nopycln: import
from dotenv import load_dotenv
from loguru import logger
from sqlalchemy.orm import sessionmaker

from impulsoetl.bd import engine


@pytest.fixture(scope="session", autouse=True)
def carregar_variaveis_ambiente():
    """Buscar arquivo .env e carrega variáveis de ambiente."""
    load_dotenv()


@pytest.fixture
def caplog(_caplog):  # noqa: F811
    """Propagar logs com Loguru para o módulo logging nativo do Python."""

    # Necessário para analisar a saída dos logs gerados com Loguru. Ver:
    # https://loguru.readthedocs.io/en/stable/resources/migration.html
    # #making-things-work-with-pytest-and-caplog

    class PropogateHandler(logging.Handler):
        def emit(self, record):
            logging.getLogger(record.name).handle(record)

    handler_id = logger.add(PropogateHandler(), format="{message}")
    yield _caplog
    logger.remove(handler_id)


@pytest.fixture(scope="session")
def envelope_sessao():
    Sessao = sessionmaker(bind=engine)
    with Sessao() as sessao:
        yield sessao


@pytest.fixture(scope="class")
def sessao(envelope_sessao):
    yield envelope_sessao
    envelope_sessao.rollback()
