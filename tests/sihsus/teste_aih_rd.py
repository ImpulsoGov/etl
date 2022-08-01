# SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT

"""Casos de teste para o ETL de autorizações de internação hospitalar."""


import re
from datetime import date

import pandas as pd
import pytest

from impulsoetl.bd import tabelas
from impulsoetl.sihsus.aih_rd import (
    COLUNAS_DATA_AAAAMMDD,
    DE_PARA_AIH_RD,
    TIPOS_AIH_RD,
    extrair_aih_rd,
    obter_aih_rd,
    transformar_aih_rd,
)
from impulsoetl.utilitarios.bd import carregar_dataframe


@pytest.fixture(scope="module")
def _aih_rd():
    return pd.read_parquet("tests/sihsus/SIH_RDSE2108_.parquet")


@pytest.fixture(scope="function")
def aih_rd(_aih_rd):
    return _aih_rd.copy()


@pytest.fixture(scope="module")
def _aih_rd_transformada():
    return pd.read_parquet("tests/sihsus/aih_rd_transformada.parquet")


@pytest.fixture(scope="function")
def aih_rd_transformada(_aih_rd_transformada):
    return _aih_rd_transformada.copy()


@pytest.fixture(scope="function")
def tabela_teste(sessao):
    try:
        # copiar estrutura da tabela original
        sessao.execute(
            "create table "
            + "dados_publicos.__sihsus_aih_reduzida_disseminacao ("
            + "like dados_publicos._sihsus_aih_reduzida_disseminacao "
            + "including all"
            + ");",
        )
        sessao.commit()
        yield "dados_publicos.__sihsus_aih_reduzida_disseminacao"
    finally:
        sessao.rollback()
        sessao.execute(
            "drop table if exists "
            + "dados_publicos.__sihsus_aih_reduzida_disseminacao;",
        )
        sessao.commit()


def teste_de_para(aih_rd):
    colunas_origem = [col.strip() for col in aih_rd.columns]
    colunas_de = list(DE_PARA_AIH_RD.keys())

    for col in colunas_de:
        assert col in colunas_origem, (
            "Coluna no De-Para não existe no arquivo de internações: "
            + "'{}'".format(col)
        )


def teste_tipos(aih_rd):
    tabela_destino = tabelas[
        "dados_publicos._sihsus_aih_reduzida_disseminacao"
    ]
    colunas_destino = tabela_destino.columns

    for col in TIPOS_AIH_RD.keys():
        assert (
            col in colunas_destino
        ), "Coluna inexistente na tabela de destino: '{}'".format(col)
    for col in colunas_destino.keys():
        assert col in TIPOS_AIH_RD, "Coluna sem tipo definido: '{}'".format(
            col
        )


def teste_colunas_datas():
    assert all(col in TIPOS_AIH_RD.keys() for col in COLUNAS_DATA_AAAAMMDD)


@pytest.mark.parametrize(
    "uf_sigla,periodo_data_inicio",
    [("SE", date(2021, 8, 1))],
)
def teste_extrair_pa(uf_sigla, periodo_data_inicio, passo):
    iterador_registros_procedimentos = extrair_aih_rd(
        uf_sigla=uf_sigla,
        periodo_data_inicio=periodo_data_inicio,
        passo=passo,
    )
    lote_1 = next(iterador_registros_procedimentos)
    assert isinstance(lote_1, pd.DataFrame)
    assert len(lote_1) == passo
    for coluna in DE_PARA_AIH_RD.keys():
        assert coluna in lote_1
    lote_2 = next(iterador_registros_procedimentos)
    assert isinstance(lote_2, pd.DataFrame)
    assert len(lote_2) > 0


@pytest.mark.integracao
def teste_transformar_aih_rd(sessao, aih_rd):
    aih_rd_transformada = transformar_aih_rd(
        sessao=sessao,
        aih_rd=aih_rd,
    )

    assert isinstance(aih_rd_transformada, pd.DataFrame)
    assert len(aih_rd_transformada) > 1

    colunas_processadas = aih_rd_transformada.columns
    colunas_esperadas = list(TIPOS_AIH_RD.keys())
    for col in colunas_processadas:
        assert re.match(
            "[a-z_]+", col
        ), "Caracteres proibidos no nome da coluna '{}'".format(col)
        assert (
            col in colunas_esperadas
        ), "Coluna '{}' não definida na tabela de destino.".format(col)
        if "data" in col:
            assert (
                str(aih_rd_transformada[col].dtype) == "datetime64[ns]"
            ), "Coluna de data com tipo incorreto: '{}'".format(col)

    for col in colunas_esperadas:
        assert col in colunas_esperadas, "Coluna não encontrada: '{}'.".format(
            col
        )


def teste_carregar_aih_rd(
    sessao,
    aih_rd_transformada,
    tabela_teste,
    passo,
    caplog,
):
    carregamento_status = carregar_dataframe(
        sessao=sessao,
        df=aih_rd_transformada.iloc[:10],
        tabela_destino=tabela_teste,
        passo=passo,
        teste=True,
    )

    assert carregamento_status == 0

    logs = caplog.text
    assert "Carregamento concluído" in logs


@pytest.mark.integracao
@pytest.mark.parametrize(
    "uf_sigla,periodo_data_inicio",
    [("SE", date(2021, 8, 1))],
)
def teste_obter_aih_rd(
    sessao,
    uf_sigla,
    periodo_data_inicio,
    tabela_teste,
    caplog,
):
    obter_aih_rd(
        sessao=sessao,
        uf_sigla=uf_sigla,
        periodo_data_inicio=periodo_data_inicio,
        tabela_destino=tabela_teste,
        teste=True,
    )
    sessao.commit()

    logs = caplog.text
    assert "Carregamento concluído" in logs
