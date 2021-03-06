# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Testa os scripts de captura de dados para os produtos de saúde mental."""


import pytest

from impulsoetl.scripts.geral import ceps, vinculos_disseminacao


@pytest.mark.integracao
def teste_vinculos_disseminacao(sessao):
    """Testa obter vínculos profissionais do CNES."""
    vinculos_disseminacao(sessao=sessao, teste=True)


@pytest.mark.integracao
def teste_ceps(sessao):
    """Testa obter Códigos de Endereçamento Postal."""
    ceps(sessao=sessao, teste=True)
