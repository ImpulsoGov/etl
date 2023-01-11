# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Testa os scripts de captura de dados para os produtos de saúde mental."""


import pytest

from impulsoetl.scripts.geral import (
    ceps,
    habilitacoes_disseminacao,
    obitos_disseminacao,
    vinculos_disseminacao,
)


@pytest.mark.integracao
def teste_habilitacoes_disseminacao():
    """Testa obter habilitações de estabelecimentos do SCNES."""
    habilitacoes_disseminacao(teste=True)


@pytest.mark.integracao
def teste_vinculos_disseminacao():
    """Testa obter vínculos profissionais do SCNES."""
    vinculos_disseminacao(teste=True)


@pytest.mark.integracao
def teste_obitos_disseminacao():
    """Testa obter declarações de óbito do SIM."""
    obitos_disseminacao(teste=True)


@pytest.mark.integracao
def teste_ceps():
    """Testa obter Códigos de Endereçamento Postal."""
    ceps(teste=True)
