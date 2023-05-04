# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


import os

import pytest
from dotenv import load_dotenv
from prefect.testing.utilities import prefect_test_harness
from sqlalchemy import create_engine
from sqlalchemy.engine import URL, Engine
from sqlalchemy.orm import sessionmaker


@pytest.fixture(scope="session", autouse=True)
def carregar_variaveis_ambiente():
    """Buscar arquivo .env e carrega variáveis de ambiente."""
    load_dotenv()


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


@pytest.fixture(scope="session")
def passo():
    # reduz o tamanho do lote para inserção no banco de dados
    lote_tamanho_original = os.getenv("IMPULSOETL_LOTE_TAMANHO")
    os.environ["IMPULSOETL_LOTE_TAMANHO"] = "100"
    try:
        yield 100
    finally:
        os.environ["IMPULSOETL_LOTE_TAMANHO"] = lote_tamanho_original


@pytest.fixture(autouse=True, scope="session")
def prefect_test_fixture():
    # VER: https://docs.prefect.io/tutorials/testing/#unit-testing-flows
    with prefect_test_harness():
        yield
