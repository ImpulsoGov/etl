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
from typing import Any, Final

import numpy as np
import sqlalchemy as sa
from dotenv import load_dotenv
from psycopg2.extensions import AsIs, register_adapter
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from impulsoetl.loggers import logger
from impulsoetl.utilitarios.bd import TabelasRefletidasDicionario

logger.info("Configurando interface com o banco de dados...")

logger.info("Obtendo parâmetros de conexão com o banco de dados...")
load_dotenv()

BD_HOST: Final[str] = os.getenv("IMPULSOETL_BD_HOST", "localhost")
BD_PORTA: Final[int] = int(os.getenv("IMPULSOETL_BD_PORTA", "5432"))
BD_NOME: Final[str] = os.getenv("IMPULSOETL_BD_NOME", "principal")
BD_USUARIO: Final[str] = os.getenv("IMPULSOETL_BD_USUARIO", "etl")
BD_SENHA: Final[str | None] = os.getenv("IMPULSOETL_BD_SENHA", None)

BD_URL: Final[sa.engine.URL] = sa.engine.URL.create(
    drivername="postgresql+psycopg2",
    username=BD_USUARIO,
    password=BD_SENHA,
    host=BD_HOST,
    port=BD_PORTA,
    database=BD_NOME,
)
logger.debug("Banco de dados: {uri}", uri=BD_URL.render_as_string())
logger.info("OK")

logger.info("Criando motor de conexão com o banco de dados...")
engine = sa.create_engine(BD_URL, pool_pre_ping=True)
logger.info("OK")

logger.info("Criando sessão...")
Sessao = sessionmaker(autocommit=False, autoflush=False, bind=engine)
logger.info("OK")

logger.info("Definindo metadados...")
meta = sa.MetaData(bind=engine)
logger.info("OK")

# obter esquemas diretamente do banco de dados e refletí-los como um dicionário
# contendo as classes de objetos equivalentes
logger.info(
    "Espelhando a estrutura das tabelas pré-existentes no banco de dados...",
)
tabelas = TabelasRefletidasDicionario(meta, views=True)
logger.info("OK")

logger.info("Criando base declarativa para a definição de novos modelos...")
Base = declarative_base(metadata=meta)
logger.info("OK")

logger.info("Definindo parâmetros para versionamento de tabelas...")
versionamento_parametros: dict[str, Any] = {
    "table_name": "%s_versoes",
    "transaction_column_name": "transacao_id",
    "end_transaction_column_name": "transacao_final_id",
    "operation_type_column_name": "transacao_tipo",
}
logger.debug("Parâmetros: {parametros}", parametros=versionamento_parametros)

logger.info("Interface com o banco de dados configurada com sucesso.")

logger.info("Configurando adaptadores tipos numpy no psycopg2...")
register_adapter(np.int64, AsIs)
register_adapter(np.float64, AsIs)
logger.info("OK.")
