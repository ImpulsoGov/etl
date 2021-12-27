# SPDX-FileCopyrightText: 2021 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Testes de categorias de datas utilizadas em vários processos de ETL."""


from __future__ import annotations

import pandas as pd
import pytest

from impulsoetl.comum.datas import obter_proximo_periodo, periodo_por_data


@pytest.mark.parametrize(
    "data,tipo_periodo,id_esperado",
    [
        (
            pd.Timestamp(2021, 9, 1),
            "mensal",
            "9883e787-10c9-4de8-af11-9de1df09543b",
        ),
        (
            pd.Timestamp(2021, 9, 1),
            "quadrimestral",
            "8b2cce45-92ac-4491-b657-f40ff9f2d2f0",
        ),
    ],
)
def teste_periodo_por_data(data, tipo_periodo, id_esperado, sessao):
    """Testa buscar o identificador do período no qual uma data está incluída."""
    periodo = periodo_por_data(
        data=data,
        tipo_periodo=tipo_periodo,
        sessao=sessao,
    )
    periodo_id = periodo.id
    assert periodo_id
    assert isinstance(periodo_id, str)
    assert periodo_id == id_esperado


@pytest.mark.parametrize(
    "periodo_id,id_esperado",
    [
        (
            "9883e787-10c9-4de8-af11-9de1df09543b",  # 2021.M9
            "1edab5a5-bbab-4c83-8992-1ec1944ec3b3",  # 2021.M10
        ),
        (
            "8b2cce45-92ac-4491-b657-f40ff9f2d2f0",  # 2021.Q2
            "27485f4a-cf46-4790-a49c-687fc1411c49",  # 2021.Q3
        ),
    ],
)
def teste_obter_proximo_periodo(periodo_id, id_esperado, sessao):
    proximo_periodo = obter_proximo_periodo(
        periodo_id=periodo_id,
        sessao=sessao,
    )
    assert proximo_periodo.id == id_esperado
