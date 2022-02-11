# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Casos de teste para o ponto de entrada das rotinas de ETL."""


import pytest

from impulsoetl.principal import principal


@pytest.mark.integracao
def teste_principal() -> None:
    principal(teste=True)
