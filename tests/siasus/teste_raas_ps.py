# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Casos de teste para o ETL de RAAS."""


import re

import pandas as pd
import pytest

from impulsoetl.siasus.raas_ps import (
    COLUNAS_DATA_AAAAMM,
    COLUNAS_DATA_AAAAMMDD,
    DE_PARA_RAAS_PS,
    TIPOS_RAAS_PS,
    obter_raas_ps,
    transformar_raas_ps,
)
from impulsoetl.utilitarios.bd import carregar_dataframe


@pytest.fixture(scope="module")
def _raas_ps():
    return pd.read_parquet("tests/siasus/SIA_PSSE2108_.parquet")


@pytest.fixture(scope="function")
def raas_ps(_raas_ps):
    return _raas_ps.copy()


@pytest.fixture(scope="module")
def _raas_ps_transformada():
    return pd.read_parquet("tests/siasus/raas_ps_transformada.parquet")


@pytest.fixture(scope="function")
def raas_ps_transformada(_raas_ps_transformada):
    return _raas_ps_transformada.copy()


@pytest.fixture(scope="function")
def tabela_teste(sessao):
    try:
        # copiar estrutura da tabela original
        sessao.execute(
            "create table "
            + "dados_publicos._siasus_raas_psicossocial_disseminacao ("
            + "like dados_publicos.siasus_raas_psicossocial_disseminacao "
            + "including all"
            + ");",
        )
        sessao.commit()
        yield "dados_publicos._siasus_raas_psicossocial_disseminacao"
    finally:
        sessao.rollback()
        sessao.execute(
            "drop table if exists "
            + "dados_publicos._siasus_raas_psicossocial_disseminacao;",
        )
        sessao.commit()


def teste_de_para(raas_ps):
    colunas_raas = [col.strip() for col in raas_ps.columns]
    colunas_de_para = list(DE_PARA_RAAS_PS.keys())
    for col in colunas_de_para:
        assert (
            col in colunas_raas
        ), "Coluna no De-Para não existe na RAAS: '{}'".format(
            col,
        )
    for col in colunas_raas:
        assert (
            col in colunas_de_para
        ), "Coluna existente na RAAS não encontrada no De-Para: '{}'".format(
            col,
        )


def teste_tipos():
    assert all(col in TIPOS_RAAS_PS.keys() for col in DE_PARA_RAAS_PS.values())


def teste_colunas_datas():
    assert all(col in TIPOS_RAAS_PS.keys() for col in COLUNAS_DATA_AAAAMM)
    assert all(col in TIPOS_RAAS_PS.keys() for col in COLUNAS_DATA_AAAAMMDD)


@pytest.mark.integracao
def teste_transformar_raas_ps(sessao, raas_ps):
    raas_ps_transformada = transformar_raas_ps(
        sessao=sessao,
        raas_ps=raas_ps,
    )

    assert isinstance(raas_ps_transformada, pd.DataFrame)
    assert len(raas_ps_transformada) > 1

    colunas_processadas = raas_ps_transformada.columns
    colunas_esperadas = list(TIPOS_RAAS_PS.keys())
    for col in colunas_processadas:
        assert re.match(
            "[a-z_]+", col
        ), "Caracteres proibidos no nome da coluna '{}'".format(col)
        assert (
            col in colunas_esperadas
        ), "Coluna '{}' não definida na tabela de destino.".format(col)
        if "data" in col:
            assert (
                str(raas_ps_transformada[col].dtype) == "datetime64[ns]"
            ), "Coluna de data com tipo incorreto: '{}'".format(col)

    for col in colunas_esperadas:
        assert col in colunas_esperadas, "Coluna não encontrada: '{}'.".format(
            col
        )


def teste_carregar_raas_ps(
    sessao,
    raas_ps_transformada,
    tabela_teste,
    passo,
    caplog,
):
    codigo_saida = carregar_dataframe(
        sessao=sessao,
        df=raas_ps_transformada.iloc[:10],
        tabela_destino=tabela_teste,
        passo=passo,
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
def teste_obter_raas_ps(sessao, uf_sigla, ano, mes, tabela_teste, caplog):
    obter_raas_ps(
        sessao=sessao,
        uf_sigla=uf_sigla,
        ano=ano,
        mes=mes,
        tabela_destino=tabela_teste,
        teste=True,
    )
    sessao.commit()

    logs = caplog.text
    assert "Carregamento concluído" in logs
