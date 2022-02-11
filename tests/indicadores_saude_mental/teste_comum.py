# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Casos de teste de funções comuns ao cálculo dos indicadores de saúde mental."""


from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest
from numpy import nan
from pandas.api.types import is_dtype_equal

from impulsoetl.indicadores_saude_mental.abandonos import (
    colunas_a_agrupar as abandono_colunas,
)
from impulsoetl.indicadores_saude_mental.comum import (
    consultar_abandonos,
    consultar_raas,
    consultar_usuarios_raas_resumo,
    primeiro_com_info,
)
from impulsoetl.siasus.raas_ps import TIPOS_RAAS_PS


@pytest.mark.parametrize(
    "serie,valor_esperado",
    [(pd.Series([nan, nan, 1, 2]), 1)],
)
def teste_primeiro_com_info(serie, valor_esperado):
    assert primeiro_com_info(serie) == 1


@pytest.mark.parametrize("unidade_geografica_id_sus", [("280030",)])
@pytest.mark.parametrize(
    "periodo_data_inicio",
    [(datetime(2021, 8, 1, tzinfo=timezone(-timedelta(hours=3))),)],
)
def teste_consultar_raas(
    sessao,
    unidade_geografica_id_sus,
    periodo_data_inicio,
):
    raas = consultar_raas(
        sessao=sessao,
        unidade_geografica_id_sus=unidade_geografica_id_sus,
        periodo_data_inicio=periodo_data_inicio,
    )
    assert isinstance(raas, pd.DataFrame)
    assert len(raas) > 0
    assert len(raas.columns) > 0
    for coluna, tipo in TIPOS_RAAS_PS.items():
        assert coluna in raas.columns
        assert is_dtype_equal(raas[coluna].dtype, tipo)


@pytest.skip
@pytest.mark.parametrize("unidade_geografica_id_sus", [("280030",)])
def teste_consultar_usuarios_raas_resumo(sessao, unidade_geografica_id_sus):
    usuarios_raas_resumo = consultar_usuarios_raas_resumo(
        sessao=sessao,
        unidade_geografica_id_sus=unidade_geografica_id_sus,
    )
    assert isinstance(usuarios_raas_resumo, pd.DataFrame)
    assert len(usuarios_raas_resumo) > 0
    assert len(usuarios_raas_resumo.columns) > 0


@pytest.skip
@pytest.mark.parametrize("unidade_geografica_id_sus", [("280030",)])
@pytest.mark.parametrize(
    "periodo_data_inicio",
    [(datetime(2021, 8, 1, tzinfo=timezone(-timedelta(hours=3))),)],
)
def teste_consultar_abandonos(
    sessao,
    unidade_geografica_id_sus,
    periodo_data_inicio,
):
    abandonos = consultar_abandonos(
        sessao=sessao,
        unidade_geografica_id_sus=unidade_geografica_id_sus,
        periodo_data_inicio=periodo_data_inicio,
    )
    assert isinstance(abandonos, pd.DataFrame)
    assert len(abandonos) > 0
    assert len(abandonos.columns) > 0
    for coluna in abandono_colunas:
        assert coluna in abandonos.columns
