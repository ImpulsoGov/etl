# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


import logging
import os

import pytest
from _pytest.logging import caplog as _caplog  # noqa: F401  # nopycln: import
from dotenv import load_dotenv
from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, URL
from sqlalchemy.orm import sessionmaker


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
def engine() -> Engine:
    BD_HOST = os.getenv("IMPULSOETL_BD_HOST", "localhost")
    BD_PORTA = int(os.getenv("IMPULSOETL_BD_PORTA", "5432"))
    BD_NOME = os.getenv("IMPULSOETL_BD_NOME", "principal")
    BD_USUARIO = os.getenv("IMPULSOETL_BD_USUARIO", "etl")
    BD_SENHA = os.getenv("IMPULSOETL_BD_SENHA", None)

    BD_URL = URL.create(
        drivername="postgresql+psycopg2",
        username=BD_USUARIO,
        password=BD_SENHA,
        host=BD_HOST,
        port=BD_PORTA,
        database=BD_NOME,
    )
    return create_engine(BD_URL, pool_pre_ping=True)


@pytest.fixture(scope="session")
def envelope_sessao(engine):
    Sessao = sessionmaker(bind=engine)
    with Sessao() as sessao:
        yield sessao


@pytest.fixture(scope="class")
def sessao(envelope_sessao):
    yield envelope_sessao
    envelope_sessao.rollback()
