# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Funções e classes úteis para interagir com o banco de dados da Impulso."""


from sqlalchemy.exc import InvalidRequestError
from sqlalchemy.schema import MetaData, Table

from impulsoetl.loggers import logger


class TabelasRefletidasDicionario(dict):
    """Representa um dicionário de tabelas refletidas de um banco de dados."""

    def __init__(self, metadata_obj: MetaData, **kwargs):
        """Instancia um dicionário de tabelas refletidas de um banco de dados.

        Funciona exatamente como o dicionário de tabelas refletidas de um banco
        de dados acessível por meio da propriedade `tables` de um objeto
        [`sqlalchemy.schema.MetaData`][], com a exceção de que as chaves
        referentes a tabelas ou consultas ainda não refletidas são espelhadas
        sob demanda quando requisitadas pelo método `__getitem__()`
        (equivalente a obter a representação de uma tabela do dicionário
        chamando `dicionario["nome_do_schema.nome_da_tabela"]`).

        Argumentos:
            metadata_obj: instância da classe [`sqlalchemy.schema.MetaData`][]
                da biblioteca SQLAlchemy,
            **kwargs: Parâmetros adicionais a serem passados para o método
                [`reflect()`][] do objeto de metadados ao se tentar obter uma
                tabela ainda não espelhada no banco de dados.

        [`sqlalchemy.schema.MetaData`]: https://docs.sqlalchemy.org/en/14/core/metadata.html#sqlalchemy.schema.MetaData
        [`reflect()`][]: https://docs.sqlalchemy.org/en/14/core/metadata.html#sqlalchemy.schema.MetaData.reflect
        """
        self.meta = metadata_obj
        self.kwargs = kwargs

    def __getitem__(self, chave: str) -> Table:
        try:
            return self.meta.tables[chave]
        except (InvalidRequestError, KeyError):
            schema = None
            try:
                schema, tabela_nome = chave.split(".", maxsplit=1)
            except ValueError:
                tabela_nome = chave
            logger.info("Espelhando tabela `{}`...", chave)
            self.meta.reflect(schema=schema, only=[tabela_nome], **self.kwargs)
            logger.info("OK.")
            return self.meta.tables[chave]

    def __setitem__(self, chave: str, valor: Table) -> None:
        self.meta.tables[chave] = valor

    def __repr__(self) -> str:
        return self.meta.tables.__repr__()

    def update(self, *args, **kwargs) -> None:
        for chave, valor in dict(*args, **kwargs).items():
            self[chave] = valor
