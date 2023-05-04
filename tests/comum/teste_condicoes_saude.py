# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Testes de categorias de datas utilizadas em vários processos de ETL."""


import pytest

from impulsoetl.comum.condicoes_saude import e_cid10, remover_ponto_cid10


@pytest.mark.parametrize(
    "texto",
    ["F99", "R296", "K09.2", "M10.06", "M45.X3"],
)
def teste_e_cid10_sim(texto):
    """Testa identificar que um texto é um CID10 válido."""
    assert e_cid10(texto)


@pytest.mark.parametrize(
    "texto",
    ["F9X", "foo", "98U", "P3", "N189I500"],
)
def teste_e_cid10_nao(texto):
    """Testa identificar que um texto é um CID10 válido."""
    assert not e_cid10(texto)


@pytest.mark.parametrize(
    "texto,resultado_esperado",
    [
        ("F99", "F99"),
        ("R296", "R296"),
        ("K09.2", "K092"),
        ("M10.06", "M1006"),
        ("M45.X3", "M45X3"),
    ],
)
def teste_remover_ponto_cid10(texto, resultado_esperado):
    """Testa identificar que um texto é um CID10 válido."""
    assert remover_ponto_cid10(texto) == resultado_esperado
