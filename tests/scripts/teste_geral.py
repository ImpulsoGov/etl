# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Testa os scripts de captura de dados para os produtos de saúde mental."""


import pytest

from impulsoetl.scripts.geral import (
    ceps,
    habilitacoes_disseminacao,
    vinculos_disseminacao,
)


@pytest.mark.integracao
def teste_habilitacoes_disseminacao(sessao):
    """Testa obter habilitações de estabelecimentos do SCNES."""
    habilitacoes_disseminacao(sessao=sessao, teste=True)


@pytest.mark.integracao
def teste_vinculos_disseminacao(sessao):
    """Testa obter vínculos profissionais do SCNES."""
    vinculos_disseminacao(sessao=sessao, teste=True)


@pytest.mark.integracao
def teste_ceps(sessao):
    """Testa obter Códigos de Endereçamento Postal."""
    ceps(sessao=sessao, teste=True)
