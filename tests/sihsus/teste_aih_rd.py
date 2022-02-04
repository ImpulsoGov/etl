# SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT

"""Casos de teste para o ETL de autorizações de internação hospitalar."""


import re

import pandas as pd
import pytest

from impulsoetl.bd import tabelas
from impulsoetl.sihsus.aih_rd import (
    COLUNAS_DATA_AAAAMMDD,
    DE_PARA_AIH_RD,
    TIPOS_AIH_RD,
    carregar_aih_rd,
    obter_aih_rd,
    transformar_aih_rd,
)


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


def teste_de_para(aih_rd):
    colunas_origem = [col.strip() for col in aih_rd.columns]
    colunas_de = list(DE_PARA_AIH_RD.keys())

    for col in colunas_de:
        assert col in colunas_origem, (
            "Coluna no De-Para não existe no arquivo de internações: "
            + "'{}'".format(col)
        )


def teste_tipos(aih_rd):
    tabela_destino = tabelas["dados_publicos.sihsus_aih_reduzida_disseminacao"]
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


def teste_carregar_aih_rd(sessao, aih_rd_transformada, caplog):
    codigo_saida = carregar_aih_rd(
        sessao=sessao,
        aih_rd_transformada=aih_rd_transformada.iloc[:10],
    )

    assert codigo_saida == 0

    logs = caplog.text
    assert (
        "Carregamento concluído para a tabela "
        + "`dados_publicos.sihsus_aih_reduzida_disseminacao`"
    ) in logs, "Carregamento para a tabela de destino não foi concluído."

    linhas_esperadas = 10
    assert (
        "adicionadas {} novas linhas.".format(linhas_esperadas) in logs
    ), "Número incorreto de linhas adicionadas à tabela."


@pytest.mark.integracao
@pytest.mark.parametrize(
    "uf_sigla",
    ["SE"],
)
@pytest.mark.parametrize(
    "ano,mes",
    [(2021, 8)],
)
def teste_obter_aih_rd(sessao, uf_sigla, ano, mes, caplog):
    obter_aih_rd(
        sessao=sessao,
        uf_sigla=uf_sigla,
        ano=ano,
        mes=mes,
        teste=True,
    )

    logs = caplog.text
    assert "Carregamento concluído para a tabela " in logs
    linhas_adicionadas = re.search("adicionadas ([0-9]+) novas linhas.", logs)
    assert linhas_adicionadas
    num_linhas_adicionadas = sum(
        int(num) for num in linhas_adicionadas.groups()
    )
    assert num_linhas_adicionadas > 0
