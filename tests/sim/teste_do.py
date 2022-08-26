# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT

"""Casos de teste para o ETL de Declarações de Óbito."""


import re
from datetime import date

import pandas as pd
import pytest

from impulsoetl.bd import tabelas
from impulsoetl.sim.do import (
    COLUNAS_DATA_DDMMAAAA,
    DE_PARA_DO,
    TIPOS_DO,
    extrair_do,
    obter_do,
    transformar_do,
)
from impulsoetl.utilitarios.bd import carregar_dataframe


@pytest.fixture(scope="module")
def _do():
    return pd.read_parquet("tests/sim/SIM_DORR2020_.parquet")


@pytest.fixture(scope="function")
def do(_do):
    return _do.copy()


@pytest.fixture(scope="module")
def _do_transformada():
    return pd.read_parquet("tests/sim/do_transformada.parquet")


@pytest.fixture(scope="function")
def do_transformada(_do_transformada):
    return _do_transformada.copy()


@pytest.fixture(scope="function")
def tabela_teste(sessao):
    try:
        # copiar estrutura da tabela original
        sessao.execute(
            "create table dados_publicos.__sim_do_disseminacao ("
            + "like dados_publicos.sim_do_disseminacao "
            + "including all"
            + ");",
        )
        sessao.commit()
        yield "dados_publicos.__sim_do_disseminacao"
    finally:
        sessao.rollback()
        sessao.execute(
            "drop table if exists dados_publicos.__sim_do_disseminacao;",
        )
        sessao.commit()


def teste_de_para(do):
    """Testa se todas as colunas no arquivo de origem estão no De-Para."""

    colunas_origem = [col.strip() for col in do.columns]
    colunas_de = list(DE_PARA_DO.keys())

    for col in colunas_de:
        assert col in colunas_origem, (
            "Coluna no De-Para não existe no arquivo de Declarações de Óbito: "
            + "'{}'".format(col)
        )
    for col in colunas_origem:
        assert col in colunas_de, (
            "Coluna existente no arquivo de Declarações de Óbito não "
            + "encontrada no De-Para: '{}'".format(col)
        )


def teste_tipos():
    """Testa se as colunas com tipos definidos correspondem à tabela no BD."""

    tabela_destino = tabelas["dados_publicos.sim_do_disseminacao"]
    colunas_destino = tabela_destino.columns

    for col in TIPOS_DO.keys():
        assert (
            col in colunas_destino
        ), "Coluna inexistente na tabela de destino: '{}'".format(col)
    for col in colunas_destino.keys():
        assert col in TIPOS_DO, "Coluna sem tipo definido: '{}'".format(col)


def teste_colunas_datas():
    assert all(col in TIPOS_DO.keys() for col in COLUNAS_DATA_DDMMAAAA)


@pytest.mark.parametrize(
    "uf_sigla,periodo_data_inicio",
    [("RR", date(2020, 1, 1))],
)
def teste_extrair_do(uf_sigla, periodo_data_inicio):
    iterador_registros_do = extrair_do(
        uf_sigla=uf_sigla,
        periodo_data_inicio=periodo_data_inicio,
        passo=10,
    )
    lote_1 = next(iterador_registros_do)
    assert isinstance(lote_1, pd.DataFrame)
    assert len(lote_1) == 10
    for coluna in DE_PARA_DO.keys():
        assert coluna in lote_1
    lote_2 = next(iterador_registros_do)
    assert isinstance(lote_2, pd.DataFrame)
    assert len(lote_2) > 0


@pytest.mark.parametrize(
    "condicoes",
    [
        "CODMUNOCOR == '140010'",  # óbitos ocorridos em Boa Vista - RR
        None,  # Sem filtros
    ],
)
def teste_transformar_do(sessao, do, condicoes):
    do_transformada = transformar_do(
        sessao=sessao,
        do=do,
        condicoes=condicoes,
    )

    assert isinstance(do_transformada, pd.DataFrame)
    assert len(do_transformada) > 1

    colunas_processadas = do_transformada.columns
    colunas_esperadas = list(TIPOS_DO.keys())
    for col in colunas_processadas:
        assert re.match(
            "[a-z_]+", col
        ), "Caracteres proibidos no nome da coluna '{}'".format(col)
        assert (
            col in colunas_esperadas
        ), "Coluna '{}' não definida na tabela de destino.".format(col)
        if "data" in col:
            assert (
                str(do_transformada[col].dtype) == "datetime64[ns]"
            ), "Coluna de data com tipo incorreto: '{}'".format(col)

    for col in colunas_esperadas:
        assert col in colunas_esperadas, "Coluna não encontrada: '{}'.".format(
            col
        )


def teste_carregar_do(sessao, do_transformada, caplog, tabela_teste, passo):
    codigo_saida = carregar_dataframe(
        sessao=sessao,
        df=do_transformada.iloc[:10],
        tabela_destino=tabela_teste,
        passo=passo,
        teste=True,
    )

    assert codigo_saida == 0

    logs = caplog.text
    assert "Carregamento concluído" in logs


@pytest.mark.integracao
@pytest.mark.parametrize(
    "uf_sigla,periodo_data_inicio",
    [("RR", date(2020, 1, 1))],
)
@pytest.mark.parametrize(
    "parametros",
    [
        {"condicoes": "CODMUNOCOR == '140010'"},  # óbitos em Boa Vista - RR
        {},  # Sem filtros
    ],
)
def teste_obter_do(
    sessao,
    uf_sigla,
    periodo_data_inicio,
    caplog,
    tabela_teste,
    parametros,
):
    obter_do(
        sessao=sessao,
        uf_sigla=uf_sigla,
        periodo_data_inicio=periodo_data_inicio,
        tabela_destino=tabela_teste,
        teste=True,
        **parametros,
    )

    logs = caplog.text
    assert "Carregamento concluído" in logs
