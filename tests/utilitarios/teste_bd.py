# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Casos de teste para funções utilitárias relacionadas ao banco de dados."""


import pytest

from sqlalchemy.engine import Engine
from sqlalchemy.schema import MetaData, Table

from impulsoetl.utilitarios.bd import TabelasRefletidasDicionario


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
