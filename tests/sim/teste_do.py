# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT

"""Casos de teste para o ETL de Declarações de Óbito."""


from __future__ import annotations

import re
from datetime import date

import pandas as pd
import pytest

from impulsoetl.bd import tabelas
from impulsoetl.sim.do import (
    COLUNAS_DATA_DDMMAAAA,
    DE_PARA_DO,
    DE_PARA_DO_ADICIONAIS,
    TIPOS_DO,
    TIPOS_DO_ADICIONAIS,
    extrair_do,
    obter_do,
    transformar_do,
)
from impulsoetl.utilitarios.bd import carregar_dataframe


PERIODOS_IDS: dict[str, str] = {
    "AP1996": "06308e37-6f75-7632-8e6a-62d3bb6b69dd",
    "AC2002": "06308e37-6f76-7988-8baa-6631d4c1f831",
    "RR2008": "06308e37-6f76-7988-8baa-6631d4c1f831",
    "AP2014": "06308e37-6f78-7b69-aa16-18555c9f2440",
    "RR2020": "06308e37-6f7a-76df-9b4e-cc1b394219a6",
}


@pytest.fixture(
    scope="function",
    name="do",
    params=("AP1996", "AC2002", "RR2008", "AP2014", "RR2020"),
)
def _do(request) -> tuple[pd.DataFrame, str]:
    caminho_arquivo = "tests/sim/SIM_DO{}_.parquet".format(request.param)
    return pd.read_parquet(caminho_arquivo), PERIODOS_IDS[request.param]


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

    do, _ = do

    colunas_origem = [col.strip().upper() for col in do.columns]
    colunas_de = list(DE_PARA_DO.keys())

    for col in colunas_de:
        assert col in colunas_origem, (
            "Coluna no De-Para não existe no arquivo de Declarações de Óbito: "
            + "'{}'".format(col)
        )
    
    colunas_de += list(DE_PARA_DO_ADICIONAIS.keys())
    for col in colunas_origem:
        assert col in colunas_de, (
            "Coluna existente no arquivo de Declarações de Óbito não "
            + "encontrada no De-Para: '{}'".format(col)
        )


def teste_tipos():
    """Testa se as colunas com tipos definidos correspondem à tabela no BD."""

    tabela_destino = tabelas["dados_publicos.sim_do_disseminacao"]
    colunas_destino = tabela_destino.columns

    colunas_tipos = list(TIPOS_DO.keys()) + list(TIPOS_DO_ADICIONAIS.keys())
    for col in colunas_tipos:
        assert (
            col in colunas_destino
        ), "Coluna inexistente na tabela de destino: '{}'".format(col)
    for col in colunas_destino.keys():
        assert col in colunas_tipos, "Coluna sem tipo definido: '{}'".format(
            col,
        )


def teste_colunas_datas():
    assert all(
        col in TIPOS_DO or col in TIPOS_DO_ADICIONAIS
        for col in COLUNAS_DATA_DDMMAAAA
    )


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
        "IDADE > '420'",  # óbitos de pessoas com mais de 20 anos
        None,  # Sem filtros
    ],
)
def teste_transformar_do(sessao, do, condicoes, tabela_teste):

    do, periodo_id = do

    do_transformada = transformar_do(
        sessao=sessao,
        do=do,
        periodo_id=periodo_id,
        condicoes=condicoes,
    )

    assert isinstance(do_transformada, pd.DataFrame)
    assert len(do_transformada) > 1

    colunas_processadas = do_transformada.columns
    colunas_esperadas = list(tabelas[tabela_teste].columns.keys())
    for col in colunas_processadas:
        assert re.match(
            "[a-z_]+", col
        ), "Caracteres proibidos no nome da coluna '{}'".format(col)
        assert (
            col in colunas_esperadas
        ), "Coluna '{}' não definida na tabela de destino.".format(col)
        if "data" in col and not col.startswith("_nao_documentado"):
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
