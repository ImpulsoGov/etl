# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT

"""Casos de teste para o ETL de BPA individualizados."""


import re

import pandas as pd
import pytest

from impulsoetl.siasus.bpa_i import (
    COLUNAS_DATA_AAAAMM,
    COLUNAS_DATA_AAAAMMDD,
    DE_PARA_BPA_I,
    TIPOS_BPA_I,
    obter_bpa_i,
    transformar_bpa_i,
)
from impulsoetl.utilitarios.bd import carregar_dataframe


@pytest.fixture(scope="module")
def _bpa_i():
    return pd.read_parquet("tests/siasus/SIA_BISE2108_.parquet")


@pytest.fixture(scope="function")
def bpa_i(_bpa_i):
    return _bpa_i.copy()


@pytest.fixture(scope="module")
def _bpa_i_transformada():
    return pd.read_parquet("tests/siasus/bpa_i_transformada.parquet")


@pytest.fixture(scope="function")
def bpa_i_transformada(_bpa_i_transformada):
    return _bpa_i_transformada.copy()


@pytest.fixture(scope="function")
def tabela_teste(sessao):
    try:
        sessao.execute(
            "create table dados_publicos._siasus_bpa_i_disseminacao "
            + "(like dados_publicos.siasus_bpa_i_disseminacao including all);",
        )
        sessao.commit()
        yield "dados_publicos._siasus_bpa_i_disseminacao"
    finally:
        sessao.rollback()
        sessao.execute(
            "drop table if exists dados_publicos._siasus_bpa_i_disseminacao;",
        )
        sessao.commit()


def teste_de_para(bpa_i):
    colunas_bpa_i = [col.strip() for col in bpa_i.columns]
    colunas_de_para = list(DE_PARA_BPA_I.keys())
    for col in colunas_de_para:
        assert (
            col in colunas_bpa_i
        ), "Coluna no De-Para não existe na BPA-i: '{}'".format(
            col,
        )
    for col in colunas_bpa_i:
        assert (
            col in colunas_de_para
        ), "Coluna existente na BPA-i não encontrada no De-Para: '{}'".format(
            col,
        )


def teste_tipos():
    assert all(col in TIPOS_BPA_I.keys() for col in DE_PARA_BPA_I.values())


def teste_colunas_datas():
    assert all(col in TIPOS_BPA_I.keys() for col in COLUNAS_DATA_AAAAMM)
    assert all(col in TIPOS_BPA_I.keys() for col in COLUNAS_DATA_AAAAMMDD)


@pytest.mark.integracao
def teste_transformar_bpa_i(sessao, bpa_i):
    bpa_i_transformada = transformar_bpa_i(
        sessao=sessao,
        bpa_i=bpa_i,
    )

    assert isinstance(bpa_i_transformada, pd.DataFrame)
    assert len(bpa_i_transformada) > 1

    colunas_processadas = bpa_i_transformada.columns
    colunas_esperadas = list(TIPOS_BPA_I.keys())
    for col in colunas_processadas:
        assert re.match(
            "[a-z_]+", col
        ), "Caracteres proibidos no nome da coluna '{}'".format(col)
        assert (
            col in colunas_esperadas
        ), "Coluna '{}' não definida na tabela de destino.".format(col)
        if "data" in col:
            assert (
                str(bpa_i_transformada[col].dtype) == "datetime64[ns]"
            ), "Coluna de data com tipo incorreto: '{}'".format(col)

    for col in colunas_esperadas:
        assert col in colunas_esperadas, "Coluna não encontrada: '{}'.".format(
            col
        )


def teste_carregar_bpa_i(sessao, bpa_i_transformada, caplog, tabela_teste):

    codigo_saida = carregar_dataframe(
        sessao=sessao,
        df=bpa_i_transformada.iloc[:10],
        tabela_destino=tabela_teste,
        passo=10,
        teste=True,
    )

    assert codigo_saida == 0

    logs = caplog.text
    assert "Carregamento concluído" in logs


@pytest.mark.integracao
@pytest.mark.parametrize(
    "uf_sigla",
    ["SE"],
)
@pytest.mark.parametrize(
    "ano,mes",
    [(2021, 8)],
)
def teste_obter_bpa_i(sessao, uf_sigla, ano, mes, caplog, tabela_teste):
    obter_bpa_i(
        sessao=sessao,
        uf_sigla=uf_sigla,
        ano=ano,
        mes=mes,
        tabela_destino=tabela_teste,
        teste=True,
    )

    logs = caplog.text
    assert "Carregamento concluído" in logs
