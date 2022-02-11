# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Testa interagir com tabelas de metadados de capturas de dados públicos."""


from __future__ import annotations

import pytest

from impulsoetl.comum.capturas import (
    atualizar_proxima_captura,
    unidades_pendentes_por_periodo,
)


@pytest.mark.parametrize("tabela_destino", [("dados_publicos.teste",)])
def teste_unidades_pendentes_por_periodo(
    sessao,
    tabela_destino,
):
    agendamentos = unidades_pendentes_por_periodo(
        sessao=sessao,
        tabela_destino=tabela_destino,
    )
    assert isinstance(agendamentos, dict)
    for periodo_id, unidades_geograficas_ids in agendamentos.items():
        assert isinstance(periodo_id, str)
        assert isinstance(unidades_geograficas_ids, list)
        assert all(isinstance(id_, str) for id_ in unidades_geograficas_ids)


@pytest.mark.parametrize(
    "tabela_destino,unidade_geografica_id," + "periodo_id,proximo_periodo_id",
    [
        (
            "dados_publicos.teste",
            "e8cb5dcc-46d4-45af-a237-4ab683b8ce8e",  # Aracaju
            "9883e787-10c9-4de8-af11-9de1df09543b",  # 2021.M9
            "1edab5a5-bbab-4c83-8992-1ec1944ec3b3",  # 2021.M10
        ),
    ],
)
def teste_atualizar_proxima_captura(
    sessao,
    tabela_destino,
    unidade_geografica_id,
    periodo_id,
    proximo_periodo_id,
):
    """Testa atualizar a próxima captura de um conjunto de dados."""
    codigo_retorno = atualizar_proxima_captura(
        sessao=sessao,
        tabela_destino=tabela_destino,
        unidade_geografica_id=unidade_geografica_id,
    )
    assert codigo_retorno == 0
