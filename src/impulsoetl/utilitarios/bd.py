# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Funções e classes úteis para interagir com o banco de dados da Impulso."""


from __future__ import annotations

import csv
from io import StringIO
from typing import Iterable

import pandas as pd
from pandas.io.sql import SQLTable
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import DBAPIError, InvalidRequestError
from sqlalchemy.orm.session import Session
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
            logger.debug("Espelhando tabela `{}`...", chave)
            self.meta.reflect(schema=schema, only=[tabela_nome], **self.kwargs)
            logger.debug("OK.")
            return self.meta.tables[chave]

    def __setitem__(self, chave: str, valor: Table) -> None:
        self.meta.tables[chave] = valor

    def __repr__(self) -> str:
        return self.meta.tables.__repr__()

    def update(self, *args, **kwargs) -> None:
        for chave, valor in dict(*args, **kwargs).items():
            self[chave] = valor


def postgresql_copiar_dados(
    tabela_dados: SQLTable,
    conexao: Connection | Engine,
    colunas: Iterable[str],
    dados_iterador: Iterable,
) -> None:
    """Inserir dados em uma tabela usando o comando COPY do PostgreSQL.

    Esta função deve ser passada como valor para o argumento `method` do
    método [`pandas.DataFrame.to_sql()`][].

    Argumentos:
        tabela_dados: objeto [`pandas.io.sql.SQLTable`][] com a representação
            da tabela SQL a ser inserida.
        conexao: objeto [`sqlalchemy.engine.Engine`][] ou
            [`sqlalchemy.engine.Connection`][] contendo a conexão de alto nível
            com o banco de dados gerenciada pelo SQLAlchemy.
        colunas: lista com os nomes de colunas a serem inseridas.
        dados_iterador: Iterável com os dados a serem inseridos.

    Exceções:
        Levanta uma subclasse da exceção [`psycopg2.Error`][] caso algum erro
        seja retornado pelo backend.

    Veja também:
        - [Documentação][io-sql-method] do Pandas sobre a implementação de
        funções personalizadas de inserção.
        - [Artigo][insert-a-pandas-dataframe-into-postgres] com comparação da
        performance de métodos de inserção de DataFrames bancos de dados
        PostgreSQL.

    [`pandas.DataFrame.to_sql()`]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_sql.html
    [`sqlalchemy.engine.Engine`]: https://docs.sqlalchemy.org/en/14/core/connections.html#sqlalchemy.engine.Engine
    [`sqlalchemy.engine.Connection`]: https://docs.sqlalchemy.org/en/14/core/connections.html#sqlalchemy.engine.Connection
    [`pandas.io.sql.SQLTable`]: https://github.com/pandas-dev/pandas/blob/a8968bfa696d51f73769c54f2630a9530488236a/pandas/io/sql.py#L762
    [`psycopg2.Error`]: https://www.psycopg.org/docs/module.html#psycopg2.Error
    [io-sql-method]: https://pandas.pydata.org/pandas-docs/stable/user_guide/io.html#io-sql-method
    [insert-a-pandas-dataframe-into-postgres]: https://ellisvalentiner.com/post/a-fast-method-to-insert-a-pandas-dataframe-into-postgres
    """

    try:
        # obter conexão de DBAPI a partir de uma conexão existente
        conector_dbapi = conexao.connection  # type: ignore
    except AttributeError:
        # obter conexão de DBAPI diretamente a partir da engine;
        # ver https://docs.sqlalchemy.org/en/14/core/connections.html
        # #working-with-the-dbapi-cursor-directly
        conector_dbapi = conexao.raw_connection()  # type: ignore

    with conector_dbapi.cursor() as cursor:  # type: ignore
        buffer = StringIO()
        writer = csv.writer(buffer)
        writer.writerows(dados_iterador)
        buffer.seek(0)

        enumeracao_colunas = ", ".join(
            '"{}"'.format(coluna) for coluna in colunas
        )
        if tabela_dados.schema:
            tabela_nome = "{}.{}".format(
                tabela_dados.schema,
                tabela_dados.name,
            )
        else:
            tabela_nome = tabela_dados.name

        expressao_sql = "COPY {} ({}) FROM STDIN WITH CSV".format(
            tabela_nome,
            enumeracao_colunas,
        )
        cursor.copy_expert(sql=expressao_sql, file=buffer)  # type: ignore

    return None


def carregar_dataframe(
    sessao: Session,
    df: pd.DataFrame,
    tabela_destino: str,
    passo: int | None = 10000,
    teste: bool = False,
) -> int:
    """Carrega dados públicos para o banco de dados analítico da ImpulsoGov.

    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov.
        df: [`DataFrame`][] contendo os dados a serem carregados na tabela de
            destino, já no formato utilizado pelo banco de dados da ImpulsoGov.
        tabela_destino: nome da tabela de destino, qualificado com o nome do
            schema (formato `nome_do_schema.nome_da_tabela`).
        passo: Indica quantos registros devem ser enviados para a base de dados
            de cada vez. Por padrão, são inseridos 10.000 registros em cada
            transação. Se o valor for `None`, todo o DataFrame é carregado de
            uma vez.
        teste: Indica se o carregamento deve ser executado em modo teste. Se
            verdadeiro, faz *rollback* de todas as operações; se falso, libera
            o ponto de recuperação criado.

    Retorna:
        Código de saída do processo de carregamento. Se o carregamento
        for bem sucedido, o código de saída será `0`.

    Note:
        Esta função não faz *commit* das alterações do banco. Após o retorno
        desta função, o commit deve ser feito manualmente (método
        `sessao.commit()`) ou implicitamente por meio do término sem erros de
        um gerenciador de contexto (`with Sessao() as sessao: # ...`) no qual a
        função de carregamento tenha sido chamada.

    [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
    [`DataFrame`]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    [`transformar_pa()`]: impulsoetl.siasus.procedimentos.transformar_pa
    """

    num_registros = len(df)
    schema_nome, tabela_nome = tabela_destino.split(".", maxsplit=1)

    logger.info(
        "Carregando {num_registros} registros de procedimentos ambulatoriais "
        "para a tabela `{tabela_destino}`...",
        num_registros=num_registros,
        tabela_destino=tabela_destino,
    )

    logger.debug("Formatando colunas de data...")
    colunas_data = df.select_dtypes(include="datetime").columns
    df[colunas_data] = df[colunas_data].applymap(
        lambda dt: dt.isoformat() if pd.notna(dt) else None
    )

    logger.info("Copiando registros...")
    engine = sessao.get_bind()
    engine = engine.execution_options(isolation_level="AUTOCOMMIT")
    with engine.connect() as conexao:
        ponto_de_recuperacao = conexao.begin_nested()

        try:
            df.to_sql(
                name=tabela_nome,
                con=conexao,
                schema=schema_nome,
                if_exists="append",
                index=False,
                chunksize=passo,
                method=postgresql_copiar_dados,
            )
        # trata exceções levantadas pelo backend
        except DBAPIError as erro:
            ponto_de_recuperacao.rollback()
            logger.error(
                "Erro ao inserir registros na tabela `{}` (Código {})",
                tabela_destino,
                erro.orig.pgcode,
            )
            erro.hide_parameters = True
            logger.debug(
                "({}.{}) {}",
                erro.orig.__class__.__module__,
                erro.orig.__class__.__name__,
                erro.orig.pgerror,
            )
            return erro.orig.pgcode

        if teste:
            ponto_de_recuperacao.rollback()
        else:
            ponto_de_recuperacao.commit()

    logger.info("Carregamento concluído.")

    return 0
