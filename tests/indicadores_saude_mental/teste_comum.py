# SPDX-FileCopyrightText: 2021 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Casos de teste de funções comuns ao cálculo dos indicadores de saúde mental."""


from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest
from pandas.api.types import is_dtype_equal

from impulsoetl.indicadores_saude_mental.abandonos import (
    colunas_a_agrupar as abandono_colunas,
)
from impulsoetl.indicadores_saude_mental.comum import (
    consultar_abandonos,
    consultar_painel_raas,
    consultar_usuarios_raas_resumo,
)


@pytest.mark.parametrize(
    "unidade_geografica_id_sus,periodo_data_inicio",
    [
        ("280030", datetime(2021, 8, 1, tzinfo=timezone(-timedelta(hours=3)))),
    ],
)
def teste_consultar_painel_raas(
    sessao,
    unidade_geografica_id_sus,
    periodo_data_inicio,
):
    painel_raas = consultar_painel_raas(
        sessao=sessao,
        unidade_geografica_id_sus=unidade_geografica_id_sus,
        periodo_data_inicio=periodo_data_inicio,
    )
    assert isinstance(painel_raas, pd.DataFrame)
    assert len(painel_raas) > 0
    assert len(painel_raas.columns) > 0

    colunas_esperadas = {
        "municipio_id": "object",
        "competencia_realizacao": "datetime64[ns, UTC]",
        "usuario_id": "object",
        "usuario_abertura_raas": "datetime64[ns, UTC]",
        "usuario_situacao_rua": "object",
        "quantidade_registrada": "int64",
        "quantidade_aprovada": "int64",
        "servico_descricao": "object",
        "servico_classificacao_descricao": "object",
        "estabelecimento_tipo_descricao": "object",
        "estabelecimento_nome": "object",
        "estabelecimento_endereco_logradouro": "object",
        "estabelecimento_endereco_numero": "object",
        "estabelecimento_endereco_cep": "object",
        "estabelecimento_latitude": "float64",
        "estabelecimento_longitude": "float64",
        "atividade_descricao": "object",
        "estabelecimento_referencia_nome": "object",
        "estabelecimento_referencia_endereco_logradouro": "object",
        "estabelecimento_referencia_endereco_numero": "object",
        "estabelecimento_referencia_endereco_cep": "object",
        "estabelecimento_referencia_latitude": "float64",
        "estabelecimento_referencia_longitude": "float64",
        "procedimento_nome": "object",
        "usuario_sexo": "object",
        "usuario_raca_cor": "object",
        "usuario_faixa_etaria": "object",
        "cid_descricao": "object",
        "cid_grupo_descricao_longa": "object",
        "cid_grupo_descricao_curta": "object",
        "raas_competencia_inicio": "datetime64[ns, UTC]",
        "usuario_tempo_no_servico": "object",
        "usuario_novo": "object",
        "usuario_substancias_abusa": "object",
        "encaminhamento_origem_descricao": "object",
        "procedimento_local": "object",
        "estabelecimento_linha_publico": "object",
        "estabelecimento_linha_perfil": "object",
    }
    for col, tipo in colunas_esperadas.items():
        assert col in painel_raas.columns
        assert is_dtype_equal(
            painel_raas[col].dtype, tipo
        ), "Tipo incorreto para a coluna '{}': '{}' (esperado: '{}')".format(
            col,
            painel_raas[col].dtype,
            tipo,
        )
    for col in painel_raas.columns:
        assert col in colunas_esperadas, "Coluna inesperada: {}".format(col)


@pytest.mark.parametrize("unidade_geografica_id_sus", ["280030"])
def teste_consultar_usuarios_raas_resumo(sessao, unidade_geografica_id_sus):
    usuarios_raas_resumo = consultar_usuarios_raas_resumo(
        sessao=sessao,
        unidade_geografica_id_sus=unidade_geografica_id_sus,
    )
    assert isinstance(usuarios_raas_resumo, pd.DataFrame)
    assert len(usuarios_raas_resumo) > 0
    assert len(usuarios_raas_resumo.columns) > 0
    colunas_esperadas = {
        "municipio_id": "object",
        "estabelecimento_nome": "object",
        "usuario_id": "object",
        "usuario_abertura_raas": "datetime64[ns, UTC]",
        "raas_competencia_inicio": "datetime64[ns, UTC]",
        "competencia_primeiro_procedimento": "datetime64[ns, UTC]",
        "atividade_descricao": "object",
        "cid_descricao": "object",
        "cid_grupo_descricao_curta": "object",
        "encaminhamento_origem_descricao": "object",
        "estabelecimento_referencia_nome": "object",
        "usuario_faixa_etaria": "object",
        "usuario_raca_cor": "object",
        "usuario_sexo": "object",
        "usuario_situacao_rua": "object",
        "usuario_substancias_abusa": "object",
    }
    for col, tipo in colunas_esperadas.items():
        assert (
            col in usuarios_raas_resumo.columns
        ), "Coluna faltando: {}".format(col)
        assert is_dtype_equal(
            usuarios_raas_resumo[col].dtype, tipo
        ), "Tipo incorreto para a coluna '{}': '{}' (esperado: '{}')".format(
            col,
            usuarios_raas_resumo[col].dtype,
            tipo,
        )
    for col in usuarios_raas_resumo.columns:
        assert col in colunas_esperadas, "Coluna inesperada: {}".format(col)


@pytest.mark.skip
@pytest.mark.parametrize("unidade_geografica_id_sus", ["280030"])
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
