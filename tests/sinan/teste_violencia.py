# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT

"""Casos de teste para o ETL de notificações de agravos de violência."""


from __future__ import annotations

import re
from datetime import date

import pandas as pd
import pytest

from impulsoetl.bd import tabelas
from impulsoetl.sinan.violencia import (
    COLUNAS_DATA,
    DE_PARA_AGRAVOS_VIOLENCIA,
    DE_PARA_AGRAVOS_VIOLENCIA_ADICIONAIS,
    TIPOS_AGRAVOS_VIOLENCIA,
    extrair_agravos_violencia,
    obter_agravos_violencia,
    transformar_agravos_violencia,
)
from impulsoetl.utilitarios.bd import carregar_dataframe

PERIODOS_IDS: dict[str, str] = {
    "09": "06308e37-6f77-7d63-842c-cfe68927c255",
    "19": "06308e37-6f79-77dd-a599-527b5d15ad2f",
}


@pytest.fixture(
    scope="function",
    name="agravos_violencia",
    params=("09", "19"),
)
def _agravos_violencia(request) -> tuple[pd.DataFrame, str]:
    caminho_arquivo = "tests/sinan/SINAN_VIOLBR{}_.parquet".format(
        request.param,
    )
    return pd.read_parquet(caminho_arquivo), PERIODOS_IDS[request.param]


@pytest.fixture(
    scope="function",
    name="agravos_violencia_transformada",
    params=("09", "19"),
)
def _agravos_violencia_transformada(request):
    caminho_arquivo = "tests/sinan/violbr{}_transformada.parquet".format(
        request.param,
    )
    return pd.read_parquet(caminho_arquivo)


@pytest.fixture(scope="function")
def tabela_teste(sessao):
    try:
        # copiar estrutura da tabela original
        sessao.execute(
            "create table dados_publicos.__sinan_violencia_disseminacao ("
            + "like dados_publicos.sinan_violencia_disseminacao "
            + "including all"
            + ");",
        )
        sessao.commit()
        yield "dados_publicos.__sinan_violencia_disseminacao"
    finally:
        sessao.rollback()
        sessao.execute(
            "drop table if exists "
            + "dados_publicos.__sinan_violencia_disseminacao;",
        )
        sessao.commit()


def teste_de_para(agravos_violencia):
    """Testa se todas as colunas no arquivo de origem estão no De-Para."""

    agravos_violencia, _ = agravos_violencia

    colunas_origem = [col.strip().upper() for col in agravos_violencia.columns]
    colunas_de = list(DE_PARA_AGRAVOS_VIOLENCIA.keys())

    for col in colunas_de:
        assert col in colunas_origem, (
            "Coluna no De-Para não existe no arquivo de agravos: "
            + "'{}'".format(col)
        )

    colunas_de += list(DE_PARA_AGRAVOS_VIOLENCIA_ADICIONAIS.keys())
    for col in colunas_origem:
        assert col in colunas_de, (
            "Coluna existente no arquivo de agravos não encontrada no De-Para:"
            + " '{}'".format(col)
        )


def teste_tipos():
    """Testa se as colunas com tipos definidos correspondem à tabela no BD."""

    tabela_destino = tabelas["dados_publicos.sinan_violencia_disseminacao"]
    colunas_destino = tabela_destino.columns

    colunas_tipos = list(TIPOS_AGRAVOS_VIOLENCIA.keys())
    for col in colunas_tipos:
        assert (
            col in colunas_destino
        ), "Coluna inexistente na tabela de destino: '{}'".format(col)
    for col in colunas_destino.keys():
        assert col in colunas_tipos, "Coluna sem tipo definido: '{}'".format(
            col,
        )


def teste_colunas_datas():
    assert all(col in TIPOS_AGRAVOS_VIOLENCIA for col in COLUNAS_DATA)


@pytest.mark.parametrize(
    "periodo_data_inicio",
    [date(2009, 1, 1), date(2019, 1, 1)],
)
def teste_extrair_agravos_violencia(periodo_data_inicio):
    iterador_registros_do = extrair_agravos_violencia(
        periodo_data_inicio=periodo_data_inicio,
        passo=10,
    )
    lote_1 = next(iterador_registros_do)
    assert isinstance(lote_1, pd.DataFrame)
    assert len(lote_1) == 10
    for coluna in DE_PARA_AGRAVOS_VIOLENCIA.keys():
        assert coluna in lote_1
    lote_2 = next(iterador_registros_do)
    assert isinstance(lote_2, pd.DataFrame)
    assert len(lote_2) > 0


@pytest.mark.parametrize(
    "condicoes",
    [
        "SG_UF == '35'",  # agravos de residentes de São Paulo
        None,  # Sem filtros
    ],
)
def teste_transformar_agravos_violencia(
    sessao,
    agravos_violencia,
    condicoes,
    tabela_teste,
):
    agravos_violencia, periodo_id = agravos_violencia

    agravos_violencia_transformada = transformar_agravos_violencia(
        sessao=sessao,
        agravos_violencia=agravos_violencia,
        periodo_id=periodo_id,
        condicoes=condicoes,
    )

    assert isinstance(agravos_violencia_transformada, pd.DataFrame)
    assert len(agravos_violencia_transformada) > 1

    colunas_processadas = agravos_violencia_transformada.columns
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
                str(agravos_violencia_transformada[col].dtype)
                == "datetime64[ns]"
            ), "Coluna de data com tipo incorreto: '{}'".format(col)

    for col in colunas_esperadas:
        assert col in colunas_esperadas, "Coluna não encontrada: '{}'.".format(
            col
        )


def teste_carregar_agravos_violencia(
    sessao,
    agravos_violencia_transformada,
    caplog,
    tabela_teste,
    passo,
):
    codigo_saida = carregar_dataframe(
        sessao=sessao,
        df=agravos_violencia_transformada.iloc[:10],
        tabela_destino=tabela_teste,
        passo=passo,
        teste=True,
    )
    assert codigo_saida == 0

    logs = caplog.text
    assert "Carregamento concluído" in logs


@pytest.mark.integracao
@pytest.mark.parametrize(
    "periodo_data_inicio,periodo_id",
    [
        (date(2009, 1, 1), "06308e37-6f77-7d63-842c-cfe68927c255"),
        (date(2019, 1, 1), "06308e37-6f79-77dd-a599-527b5d15ad2f"),
    ],
)
@pytest.mark.parametrize(
    "parametros",
    [
        {"condicoes": "SG_UF == '35'"},  # agravos de residentes de São Paulo
        {},  # Sem filtros
    ],
)
def teste_obter_do(
    sessao,
    periodo_data_inicio,
    periodo_id,
    caplog,
    tabela_teste,
    parametros,
):
    obter_agravos_violencia(
        sessao=sessao,
        periodo_data_inicio=periodo_data_inicio,
        periodo_id=periodo_id,
        tabela_destino=tabela_teste,
        teste=True,
        **parametros,
    )

    logs = caplog.text
    assert "Carregamento concluído" in logs
