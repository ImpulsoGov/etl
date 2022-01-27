# SPDX-FileCopyrightText: 2021 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Casos de teste para o cálculo da taxa de abandono em CAPS."""


import re
from datetime import datetime

import pandas as pd
import pytest
from numpy import nan
from pandas.api.types import is_dtype_equal

from impulsoetl.indicadores_saude_mental.abandonos import (
    _maxima_movel_retroativa,
    caracterizar_abandonos,
    carregar_abandonos,
    obter_abandonos,
)


@pytest.fixture
def df_exemplo() -> pd.DataFrame:
    return pd.DataFrame(
        data={
            "A": [0, 1, 0, 0, 0, 0, 2, 1, 0, 0, 0, 1],
            "B": ["foo"] * 6 + ["bar"] * 6,
            "C": list(pd.date_range("2021-01-01", periods=6, freq="MS")) * 2,
        }
    )


@pytest.fixture(scope="module")
def _painel_raas() -> pd.DataFrame:
    return pd.read_csv(
        "tests/indicadores_saude_mental/painel_raas.csv",
        dtype={"municipio_id": "object"},
        parse_dates=[
            "competencia_realizacao",
            "usuario_abertura_raas",
            "raas_competencia_inicio",
        ],
    )


@pytest.fixture
def painel_raas(_painel_raas) -> pd.DataFrame:
    return _painel_raas.copy()


@pytest.fixture(scope="module")
def _abandonos() -> pd.DataFrame:
    return pd.read_parquet("tests/indicadores_saude_mental/abandonos.parquet")


@pytest.fixture
def abandonos(_abandonos) -> pd.DataFrame:
    return _abandonos.copy()


@pytest.mark.parametrize(
    "coluna_nome,agrupar_por,coluna_destino,janela,valores_esperados",
    [
        ("A", ["B"], "D", 3, [1, 1, 0, 0, nan, nan, 2, 1, 0, 1, nan, nan]),
    ],
)
def teste_maxima_movel_retroativa(
    df_exemplo,
    coluna_nome,
    agrupar_por,
    coluna_destino,
    janela,
    valores_esperados,
):
    df_transformado = _maxima_movel_retroativa(
        df_exemplo,
        coluna_nome=coluna_nome,
        agrupar_por=agrupar_por,
        coluna_destino=coluna_destino,
        janela=janela,
    )
    assert isinstance(df_transformado, pd.DataFrame)
    assert len(df_transformado) > 0
    assert coluna_nome in df_transformado.columns
    for col in agrupar_por:
        assert col in df_transformado.columns
    assert coluna_destino in df_transformado.columns
    resultados = df_transformado[coluna_destino]
    for resultado, valor_esperado in zip(resultados, valores_esperados):
        assert (resultado == valor_esperado) or (
            pd.isna(resultado) and pd.isna(valor_esperado)
        )


def teste_caracterizar_abandonos(sessao, painel_raas):
    abandonos = caracterizar_abandonos(sessao=sessao, painel_raas=painel_raas)
    assert isinstance(abandonos, pd.DataFrame)
    assert len(abandonos) > 0

    colunas_esperadas = {
        "unidade_geografica_id": "object",
        "periodo_id": "object",
        "estabelecimento_nome": "object",
        "usuario_cns_criptografado": "object",
        "inicia_inatividade": "bool",
        "abandonou": "bool",
    }
    for col, tipo in colunas_esperadas.items():
        assert col in abandonos.columns, "Coluna faltando: {}".format(col)
        assert is_dtype_equal(
            abandonos[col].dtype,
            tipo,
        ), "Tipo incorreto para a coluna '{}': '{}' (esperado: '{}')".format(
            col,
            abandonos[col].dtype,
            tipo,
        )
    for col in abandonos.columns:
        assert col in colunas_esperadas, "Coluna inesperada: {}".format(col)
        assert sum(pd.isna(abandonos[col])) < len(
            abandonos[col]
        ), "Coluna nula: {}".format(col)

    # testes de fumaça
    assert any(abandonos["inicia_inatividade"]), "Sem registros de inatividade"
    assert any(abandonos["abandonou"]), "Sem registros de abandono"
    assert not all(abandonos["inicia_inatividade"]), "Todos ficaram inativos"
    assert any(abandonos["abandonou"]), "Todos abandonaram"


def teste_carregar_abandonos(sessao, abandonos, caplog):
    codigo_saida = carregar_abandonos(
        sessao=sessao,
        abandonos=abandonos,
    )

    assert codigo_saida == 0

    logs = caplog.text
    assert (
        "Carregamento concluído para a tabela "
        + "`saude_mental.abandonos_usuarios_recentes`"
    ) in logs, "Carregamento para a tabela de destino não foi concluído."


@pytest.mark.integracao
@pytest.mark.parametrize(
    "unidade_geografica_id_sus,periodo_data_inicio",
    [("280030", datetime(2021, 1, 1))],
)
def teste_obter_abandonos(
    sessao,
    unidade_geografica_id_sus,
    periodo_data_inicio,
    caplog,
):
    obter_abandonos(
        sessao=sessao,
        unidade_geografica_id_sus=unidade_geografica_id_sus,
        periodo_data_inicio=periodo_data_inicio,
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
