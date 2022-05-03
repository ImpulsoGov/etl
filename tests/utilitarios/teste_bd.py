# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Casos de teste para funções utilitárias relacionadas ao banco de dados."""


import pandas as pd
import pytest

from psycopg2 import errorcodes
from sqlalchemy.engine import Engine
from sqlalchemy.schema import MetaData, Table

from impulsoetl.utilitarios.bd import (
    carregar_dataframe,
    TabelasRefletidasDicionario,
    postgresql_copiar_dados,
)


class TesteTabelasRefletidasDicionario(object):
    @pytest.fixture(scope="function")
    def metadata_obj(self, engine: Engine) -> MetaData:
        """Retorna um objeto de metadados do SQLAlchemy."""
        meta = MetaData(bind=engine)
        # Garantir que algumas tabelas já estejam refletidas
        meta.reflect(schema="configuracoes", only=["capturas_operacoes"])
        return meta

    @pytest.mark.parametrize("kwargs", [dict(), {"views": True}])
    def teste_inicializar(self, metadata_obj, kwargs):
        """Testa inicializar um dicionário de tabelas refletidas do BD."""
        tabelas = TabelasRefletidasDicionario(
            metadata_obj=metadata_obj,
            **kwargs,
        )
        assert isinstance(tabelas, dict)
        assert hasattr(tabelas, "meta")
        assert isinstance(tabelas.meta, MetaData)
        assert hasattr(tabelas, "kwargs")
        if kwargs:
            assert tabelas.kwargs

    @pytest.fixture(scope="function")
    def tabela_refletida_dicionario(
        self,
        metadata_obj: MetaData
    ) -> TabelasRefletidasDicionario:
        """Retorna um dicionário de tabelas refletidas do banco de dados."""
        return TabelasRefletidasDicionario(
            metadata_obj=metadata_obj,
            views=True,
        )

    def teste_obter_tabela_refletida_previamente(
        self,
        tabela_refletida_dicionario: TabelasRefletidasDicionario
    ):
        """Testa obter tabela refletida antes de inicializar o dicionário."""
        capturas_operacoes = tabela_refletida_dicionario[
            "configuracoes.capturas_operacoes"
        ]
        assert isinstance(capturas_operacoes, Table)
        assert capturas_operacoes.name == "capturas_operacoes"

    def teste_obter_tabela_refletida_demanda(
        self,
        tabela_refletida_dicionario: TabelasRefletidasDicionario
    ):
        unidades_geograficas_por_projuto = tabela_refletida_dicionario[
            "configuracoes.unidades_geograficas_por_projuto"
        ]
        assert isinstance(unidades_geograficas_por_projuto, Table)
        assert (
            unidades_geograficas_por_projuto.name
            == "unidades_geograficas_por_projuto"
        )

    def teste_obter_consulta_refletida_demanda(
        self,
        tabela_refletida_dicionario: TabelasRefletidasDicionario
    ):
        periodos_sucessao = tabela_refletida_dicionario[
            "listas_de_codigos.periodos_sucessao"
        ]
        assert isinstance(periodos_sucessao, Table)
        assert periodos_sucessao.name == "periodos_sucessao"


@pytest.fixture(scope="function")
def dataframe_exemplo() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date_": [
                pd.Timestamp("2010-10-18"),
                pd.Timestamp("2010-10-19"),
                pd.Timestamp("2010-10-20"),
            ],
            "col_1": ["X", "Y", "Z"],
            "col_2": [27.5, -12.5, 5.73],
            "col_3": [True, False, True],
        },
        index=pd.Index([26, 42, 63], name="id"),
    )


@pytest.fixture(scope="function")
def tabela_teste(sessao):
    try:
        sessao.execute(
            "CREATE TABLE IF NOT EXISTS dados_publicos.__teste123 ("
            + "id int4, "
            + "date_ date, "
            + "col_1 varchar(5), "
            + "col_2 numeric, "
            + "col_3 bool"
            + ");"
        )
        sessao.commit()
        yield "dados_publicos.__teste123"
    finally:
        sessao.execute("DROP TABLE IF EXISTS dados_publicos.__teste123;")
        sessao.commit()


def teste_postgresql_copiar_dados(
    sessao,
    dataframe_exemplo,
    tabela_teste,
):
    schema, tabela = tabela_teste.split(".", maxsplit=1)
    engine = sessao.get_bind()
    with engine.connect() as conexao:
        ponto_de_recuperacao = conexao.begin_nested()
        dataframe_exemplo.to_sql(
            name=tabela,
            con=engine,
            schema=schema,
            if_exists="replace",
            method=postgresql_copiar_dados,
            index=False,
        )
        ponto_de_recuperacao.commit()
    tabela_inserida = Table(
        tabela,
        MetaData(schema=schema),
        autoload_with=engine,
    )
    registros_inseridos = sessao.query(tabela_inserida).all()
    assert registros_inseridos
    assert len(registros_inseridos) == len(dataframe_exemplo)


def teste_carregar_dataframe(sessao, dataframe_exemplo, tabela_teste, passo):
    carregamento_status = carregar_dataframe(
        sessao=sessao,
        df=dataframe_exemplo,
        tabela_destino=tabela_teste,
        passo=passo,
        teste=True,
    )
    assert carregamento_status == 0
    sessao.commit()
    schema, tabela = tabela_teste.split(".", maxsplit=1)
    tabela_inserida = Table(
        tabela,
        MetaData(schema=schema),
        autoload_with=sessao.get_bind(),
    )
    registros_inseridos = sessao.query(tabela_inserida).all()
    sessao.commit()
    assert registros_inseridos
    assert len(registros_inseridos) == len(dataframe_exemplo)


@pytest.mark.parametrize(
    "tabela,erro_esperado",
    [
        ("blablabla.tabela", "INVALID_SCHEMA_NAME"),
        ("pg_catalog.tabela", "INSUFFICIENT_PRIVILEGE")
    ]
)
def teste_carregar_dataframe_com_erro(
    sessao,
    dataframe_exemplo,
    tabela,
    erro_esperado,
    passo,
    caplog,
):
    carregamento_status = carregar_dataframe(
        sessao=sessao,
        df=dataframe_exemplo,
        tabela_destino=tabela,
        passo=passo,
        teste=True,
    )

    assert errorcodes.lookup(carregamento_status) == erro_esperado
    assert "Erro ao inserir registros na tabela" in caplog.text
