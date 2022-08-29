# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Casos de teste para o ETL de dados de vínculos profissionais do SCNES."""


import re
from datetime import date

import pandas as pd
import pytest

from impulsoetl.bd import tabelas
from impulsoetl.scnes.habilitacoes import (
    COLUNAS_DATA_AAAAMM,
    DE_PARA_HABILITACOES,
    TIPOS_HABILITACOES,
    extrair_habilitacoes,
    obter_habilitacoes,
    transformar_habilitacoes,
)
from impulsoetl.utilitarios.bd import carregar_dataframe


@pytest.fixture(scope="module")
def _habilitacoes():
    return pd.read_parquet("CNES_HBSE2111_.parquet")


@pytest.fixture(scope="function")
def habilitacoes(_habilitacoes):
    return _habilitacoes.copy()


@pytest.fixture(scope="module")
def _habilitacoes_transformado():
    return pd.read_parquet("habilitacoes_transformado.parquet")


@pytest.fixture(scope="function")
def habilitacoes_transformado(_habilitacoes_transformado):
    return _habilitacoes_transformado.copy()


@pytest.fixture(scope="function")
def tabela_teste(sessao):
    try:
        # copiar estrutura da tabela original
        # DUVIDA: __scnes_habilitacoes_disseminacao já existe?
        sessao.execute(
            "create table "
            + "dados_publicos.__scnes_habilitacoes_disseminacao ("
            + "like dados_publicos.scnes_habilitacoes_disseminacao "
            + "including all"
            + ");",
        )
        sessao.commit()
        yield "dados_publicos.__scnes_habilitacoes_disseminacao"
    finally:
        sessao.rollback()
        sessao.execute(
            "drop table if exists "
            + "dados_publicos.__scnes_habilitacoes_disseminacao;",
        )
        sessao.commit()


def teste_de_para(habilitacoes):
    colunas_origem = [col.strip() for col in habilitacoes.columns]
    colunas_de = list(DE_PARA_HABILITACOES.keys())

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


def teste_tipos(habilitacoes):
    tabela_destino = tabelas["dados_publicos.scnes_habilitacoes_disseminacao"]
    colunas_destino = tabela_destino.columns

    for col in TIPOS_HABILITACOES.keys():
        assert (
            col in colunas_destino
        ), "Coluna inexistente na tabela de destino: '{}'".format(col)
    for col in colunas_destino.keys():
        assert (
            col in TIPOS_HABILITACOES
        ), "Coluna sem tipo definido: '{}'".format(col)


def teste_colunas_datas():
    assert all(col in TIPOS_HABILITACOES.keys() for col in COLUNAS_DATA_AAAAMM)


@pytest.mark.parametrize(
    "uf_sigla,periodo_data_inicio",
    [("SE", date(2021, 8, 1))],
)
def teste_extrair_pa(uf_sigla, periodo_data_inicio, passo):
    iterador_registros_procedimentos = extrair_habilitacoes(
        uf_sigla=uf_sigla, periodo_data_inicio=periodo_data_inicio, passo=passo
    )
    lote_1 = next(iterador_registros_procedimentos)
    assert isinstance(lote_1, pd.DataFrame)
    assert len(lote_1) == passo
    for coluna in DE_PARA_HABILITACOES.keys():
        assert coluna in lote_1
    lote_2 = next(iterador_registros_procedimentos)
    assert isinstance(lote_2, pd.DataFrame)
    assert len(lote_2) > 0


@pytest.mark.integracao
def teste_transformar_habilitacoes(sessao, habilitacoes):
    habilitacoes_transformado = transformar_habilitacoes(
        sessao=sessao,
        habilitacoes=habilitacoes,
    )

    assert isinstance(habilitacoes_transformado, pd.DataFrame)
    assert len(habilitacoes_transformado) > 1

    colunas_processadas = habilitacoes_transformado.columns
    colunas_esperadas = list(TIPOS_HABILITACOES.keys())
    for col in colunas_processadas:
        assert re.match(
            "[a-z_]+", col
        ), "Caracteres proibidos no nome da coluna '{}'".format(col)
        assert (
            col in colunas_esperadas
        ), "Coluna '{}' não definida na tabela de destino.".format(col)
        if "data" in col:
            assert (
                str(habilitacoes_transformado[col].dtype) == "datetime64[ns]"
            ), "Coluna de data com tipo incorreto: '{}'".format(col)

    for col in colunas_esperadas:
        assert col in colunas_esperadas, "Coluna não encontrada: '{}'.".format(
            col
        )


def teste_carregar_habilitacoes(
    sessao,
    habilitacoes_transformado,
    tabela_teste,
    passo,
    caplog,
):
    carregamento_status = carregar_dataframe(
        sessao=sessao,
        df=habilitacoes_transformado.iloc[:10],
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
def teste_obter_habilitacoes(
    sessao,
    uf_sigla,
    periodo_data_inicio,
    tabela_teste,
    caplog,
):
    obter_habilitacoes(
        sessao=sessao,
        uf_sigla=uf_sigla,
        periodo_data_inicio=periodo_data_inicio,
        tabela_destino=tabela_teste,
        teste=True,
    )

    logs = caplog.text
    assert "Carregamento concluído" in logs
