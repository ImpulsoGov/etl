# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Testes de categorias geográficas utilizadas em vários processos de ETL."""


from __future__ import annotations

import pytest

from impulsoetl.comum.geografias import (
    id_impulso_para_id_sus,
    id_sim_para_id_impulso,
    id_sus_para_id_impulso,
    uf_id_ibge_para_sigla,
)


@pytest.mark.parametrize(
    "id_ibge,sigla_esperada",
    [("28", "SE")],
)
def teste_uf_id_ibge_para_sigla(id_ibge, sigla_esperada, sessao):
    """Testa obter a sigla de uma UF a partir do código IBGE."""
    sigla = uf_id_ibge_para_sigla(sessao=sessao, id_ibge=id_ibge)
    assert sigla == sigla_esperada


@pytest.mark.parametrize(
    "id_sus,id_esperado",
    [("280030", "e8cb5dcc-46d4-45af-a237-4ab683b8ce8e")],
)
def teste_id_sus_para_id_impulso(id_sus, id_esperado, sessao):
    """Testa converter identificador SUS para o usado no banco da Impulso."""
    id_impulso = id_sus_para_id_impulso(id_sus=id_sus, sessao=sessao)
    assert id_impulso
    assert isinstance(id_impulso, str)
    assert id_impulso == id_esperado


@pytest.mark.parametrize(
    "id_sim,id_esperado",
    [("539914", "0630e740-d46f-7dca-a009-6f1232e66823")],  # Planaltina-DF
)
def teste_id_sim_para_id_impulso(id_sim, id_esperado, sessao):
    """Testa converter identificador SIM para o usado no banco da Impulso."""
    id_impulso = id_sim_para_id_impulso(id_sim=id_sim, sessao=sessao)
    assert id_impulso
    assert isinstance(id_impulso, str)
    assert id_impulso == id_esperado


@pytest.mark.parametrize(
    "id_impulso,id_esperado",
    [("e8cb5dcc-46d4-45af-a237-4ab683b8ce8e", "280030")],
)
def teste_id_impulso_para_id_sus(id_impulso, id_esperado, sessao):
    """Testa converter identificador SUS para o usado no banco da Impulso."""
    id_sus = id_impulso_para_id_sus(id_impulso=id_impulso, sessao=sessao)
    assert id_sus
    assert isinstance(id_sus, str)
    assert id_sus == id_esperado
