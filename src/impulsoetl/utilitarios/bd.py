# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Configurações gerais de conexão com o banco de dados.

Atributos:
    SQLALCHEMY_DATABASE_URL: Cadeia de conexão com o banco de dados PostgreSQL.
    engine: Objeto de conexão entre o SQLAlchemy e o banco de dados.
    Base: Base para a definição de modelos objeto-relacionais (ORM) segundo no
        [paradigma declarativo do SQLAlchemy][sqlalchemy-declarativo].

[sqlalchemy-declarativo]: https://docs.sqlalchemy.org/en/13/orm/extensions/declarative/index.html
"""


import os

import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

SQLALCHEMY_DATABASE_URL = os.environ["IMPULSOETL_POSTGRES_CONEXAO"]


engine = sa.create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()
