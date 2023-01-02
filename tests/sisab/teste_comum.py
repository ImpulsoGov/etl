# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Testa a interação com elementos genéricos de formulários do SISAB."""


from typing import Generator

import pytest

from impulsoetl.sisab.comum import RelatorioAbstrato


@pytest.fixture(scope="module")
def caminho_relatorio_csv() -> str:
    return "tests/sisab/RelatorioSaudeProducao.csv"


@pytest.fixture
def relatorio_csv(caminho_relatorio_csv) -> Generator[str, None, None]:
    with open(caminho_relatorio_csv, encoding="ISO-8859-1") as arquivo:
        yield arquivo.read()


@pytest.fixture
def classe_relatorio_minimo() -> object:
    class RelatorioMinimo(RelatorioAbstrato):
        pass

    return RelatorioMinimo


def teste_ler_relatorio_csv(classe_relatorio_minimo, relatorio_csv):
    """Testa processar um relatório genérico do SISAB."""
    relatorio = classe_relatorio_minimo(relatorio_csv)
    assert relatorio._cabecalho
    assert relatorio._conteudo_csv
    assert relatorio._rodape
    assert relatorio.dados.shape[0] > 0
    assert relatorio.dados.shape[1] > 0
    assert "Atendimento Individual" in relatorio.dados.columns
    assert "Atendimento Odontológico" in relatorio.dados.columns
    assert "Procedimento" in relatorio.dados.columns
    assert "Visita Domiciliar" in relatorio.dados.columns
    assert relatorio.dados.iloc[0]["Atendimento Individual"] > 10
