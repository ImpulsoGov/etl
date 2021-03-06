# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Processa dados de indicadores de desempenho para o formato usado no BD."""


from datetime import date, datetime

import pandas as pd
from sqlalchemy import or_
from sqlalchemy.orm import Session

from impulsoetl.comum.datas import periodo_por_codigo, periodo_por_data
from impulsoetl.comum.geografias import id_sus_para_id_impulso
from impulsoetl.sisab.indicadores_municipios.modelos import indicadores_regras


def indicadores_regras_id_por_periodo(  # noqa: WPS122
    sessao: Session,
    indicador: str,
    data=date,
):

    return (
        sessao.query(indicadores_regras)  # type: ignore
        .filter(indicadores_regras.c.nome == indicador)
        .filter(indicadores_regras.c.versao_inicio <= data)
        .filter(
            or_(
                indicadores_regras.c.versao_fim >= data,
                indicadores_regras.c.versao_fim is None,
            )
        )
        .first()
        .id
    )


def tratamento_dados(
    sessao: Session,
    dados_sisab_indicadores: pd.DataFrame,
    periodo: date,
    indicador: str,
) -> pd.DataFrame:

    tabela_consolidada = pd.DataFrame(
        columns=[
            "municipio_id_sus",
            "periodo_id",
            "periodo_codigo",
            "indicadores_nome",
            "indicadores_regras_id",
            "numerador",
            "denominador_estimado",
            "denominador_informado",
            "nota_porcentagem",
        ]
    )

    tabela_consolidada[
        [
            "municipio_id_sus",
            "numerador",
            "denominador_estimado",
            "denominador_informado",
            "nota_porcentagem",
        ]
    ] = dados_sisab_indicadores.loc[
        :,
        [
            "ibge",
            "numerador",
            "denominador_estimado",
            "denominador_informado",
            "nota",
        ],
    ]
    tabela_consolidada["indicadores_nome"] = indicador
    indicadores_regras_id = indicadores_regras_id_por_periodo(
        sessao=sessao, indicador=indicador, data=periodo
    )
    tabela_consolidada["indicadores_regras_id"] = indicadores_regras_id
    tabela_consolidada["criacao_data"] = datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    tabela_consolidada["atualizacao_data"] = datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    periodo_cod = periodo_por_data(sessao=sessao, data=periodo)
    tabela_consolidada["periodo_codigo"] = periodo_cod[3]
    periodo_obj = periodo_por_codigo(sessao=sessao, codigo=periodo_cod[3])
    tabela_consolidada["periodo_id"] = periodo_obj.id
    tabela_consolidada["unidade_geografica_id"] = tabela_consolidada[
        "municipio_id_sus"
    ].apply(
        lambda municipio_id_sus: id_sus_para_id_impulso(
            sessao=sessao,
            id_sus=municipio_id_sus,
        )
    )
    tabela_consolidada.reset_index(drop=True, inplace=True)

    # Formata????o de tipo
    tabela_consolidada["municipio_id_sus"] = tabela_consolidada[
        "municipio_id_sus"
    ].astype("string")
    tabela_consolidada["periodo_id"] = tabela_consolidada["periodo_id"].astype(
        "string"
    )
    tabela_consolidada["periodo_codigo"] = tabela_consolidada[
        "periodo_codigo"
    ].astype("string")
    tabela_consolidada["unidade_geografica_id"] = tabela_consolidada[
        "unidade_geografica_id"
    ].astype("string")
    tabela_consolidada["indicadores_nome"] = tabela_consolidada[
        "indicadores_nome"
    ].astype("string")
    tabela_consolidada["indicadores_regras_id"] = tabela_consolidada[
        "indicadores_regras_id"
    ].astype("string")
    tabela_consolidada["numerador"] = tabela_consolidada["numerador"].astype(
        int
    )
    tabela_consolidada["denominador_estimado"] = tabela_consolidada[
        "denominador_estimado"
    ].astype(int)
    tabela_consolidada["denominador_informado"] = tabela_consolidada[
        "denominador_informado"
    ].astype(int)
    tabela_consolidada["nota_porcentagem"] = tabela_consolidada[
        "nota_porcentagem"
    ].astype(int)
    tabela_consolidada["criacao_data"] = tabela_consolidada[
        "criacao_data"
    ].astype("string")
    tabela_consolidada["atualizacao_data"] = tabela_consolidada[
        "atualizacao_data"
    ].astype("string")

    return tabela_consolidada
