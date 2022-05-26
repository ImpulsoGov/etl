# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT

"""Casos de teste para o ETL de procedimentos ambulatoriais."""


import re
from datetime import date

import pandas as pd
import pytest

from impulsoetl.bd import tabelas
from impulsoetl.siasus.procedimentos import (
    COLUNAS_DATA_AAAAMM,
    DE_PARA_PA,
    TIPOS_PA,
    extrair_pa,
    obter_pa,
    transformar_pa,
)
from impulsoetl.utilitarios.bd import carregar_dataframe


@pytest.fixture(scope="module")
def _pa():
    return pd.read_parquet("tests/siasus/SIA_PASE2108_.parquet")


@pytest.fixture(scope="function")
def pa(_pa):
    return _pa.copy()


@pytest.fixture(scope="module")
def _pa_transformada():
    return pd.read_parquet("tests/siasus/pa_transformada.parquet")


@pytest.fixture(scope="function")
def pa_transformada(_pa_transformada):
    return _pa_transformada.copy()


@pytest.fixture(scope="function")
def tabela_teste(sessao):
    try:
        # copiar estrutura da tabela original
        sessao.execute(
            "create table dados_publicos._siasus_procedimentos_ambulatoriais ("
            + "like dados_publicos.siasus_procedimentos_ambulatoriais "
            + "including all"
            + ");",
        )
        sessao.commit()
        yield "dados_publicos._siasus_procedimentos_ambulatoriais"
    finally:
        sessao.rollback()
        sessao.execute(
            "drop table if exists "
            + "dados_publicos._siasus_procedimentos_ambulatoriais;",
        )
        sessao.commit()


def teste_de_para(pa):
    colunas_origem = [col.strip() for col in pa.columns]
    colunas_de = list(DE_PARA_PA.keys())

    for col in colunas_de:
        assert col in colunas_origem, (
            "Coluna no De-Para não existe no arquivo de procedimentos: "
            + "'{}'".format(col)
        )
    for col in colunas_origem:
        assert col in colunas_de, (
            "Coluna existente no arquivo de procedimentos não encontrada no "
            + "De-Para: '{}'".format(col)
        )


def teste_tipos(pa):
    tabela_destino = tabelas[
        "dados_publicos.siasus_procedimentos_ambulatoriais"
    ]
    colunas_destino = tabela_destino.columns

    for col in TIPOS_PA.keys():
        assert (
            col in colunas_destino
        ), "Coluna inexistente na tabela de destino: '{}'".format(col)
    for col in colunas_destino.keys():
        assert col in TIPOS_PA, "Coluna sem tipo definido: '{}'".format(col)


def teste_colunas_datas():
    assert all(col in TIPOS_PA.keys() for col in COLUNAS_DATA_AAAAMM)


@pytest.mark.parametrize(
    "uf_sigla,periodo_data_inicio",
    [("SE", date(2021, 8, 1))],
)
def teste_extrair_pa(uf_sigla, periodo_data_inicio, passo):
    iterador_registros_procedimentos = extrair_pa(
        uf_sigla=uf_sigla,
        periodo_data_inicio=periodo_data_inicio,
        passo=passo,
    )
    lote_1 = next(iterador_registros_procedimentos)
    assert isinstance(lote_1, pd.DataFrame)
    assert len(lote_1) == passo
    for coluna in DE_PARA_PA.keys():
        assert coluna in lote_1
    lote_2 = next(iterador_registros_procedimentos)
    assert isinstance(lote_2, pd.DataFrame)
    assert len(lote_2) > 0


@pytest.mark.integracao
def teste_transformar_pa(sessao, pa):
    pa_transformada = transformar_pa(
        sessao=sessao,
        pa=pa,
    )

    assert isinstance(pa_transformada, pd.DataFrame)
    assert len(pa_transformada) > 1

    colunas_processadas = pa_transformada.columns
    colunas_esperadas = list(TIPOS_PA.keys())
    for col in colunas_processadas:
        assert re.match(
            "[a-z_]+", col
        ), "Caracteres proibidos no nome da coluna '{}'".format(col)
        assert (
            col in colunas_esperadas
        ), "Coluna '{}' não definida na tabela de destino.".format(col)
        if "data" in col:
            assert (
                str(pa_transformada[col].dtype) == "datetime64[ns]"
            ), "Coluna de data com tipo incorreto: '{}'".format(col)

    for col in colunas_esperadas:
        assert col in colunas_esperadas, "Coluna não encontrada: '{}'.".format(
            col
        )


def teste_carregar_pa(sessao, pa_transformada, caplog, tabela_teste, passo):
    codigo_saida = carregar_dataframe(
        sessao=sessao,
        df=pa_transformada.iloc[:10],
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
    [("SE", date(2021, 8, 1))],
)
def teste_obter_pa(
    sessao,
    uf_sigla,
    periodo_data_inicio,
    caplog,
    tabela_teste,
):
    obter_pa(
        sessao=sessao,
        uf_sigla=uf_sigla,
        periodo_data_inicio=periodo_data_inicio,
        tabela_destino=tabela_teste,
        teste=True,
    )

    logs = caplog.text
    assert "Carregamento concluído" in logs
