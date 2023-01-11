# SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


from datetime import date, datetime

import pandas as pd
from prefect import task
from sqlalchemy.orm import Session
from uuid6 import uuid7

from impulsoetl.comum.datas import periodo_por_codigo, periodo_por_data
from impulsoetl.comum.geografias import id_sus_para_id_impulso
from impulsoetl.loggers import habilitar_suporte_loguru


@task(
    name="Transformar Cadastros Individuais",
    description=(
        "Transforma os dados de cadastros individuais extraídos do portal "
        + "público do Sistema de Informação em Saúde para a Atenção Básica do "
        + "SUS."
    ),
    tags=["aps", "sisab", "cadastros_individuais", "transformacao"],
    retries=0,
    retry_delay_seconds=None,
)
def tratamento_dados(
    sessao: Session,
    dados_sisab_cadastros: pd.DataFrame,
    com_ponderacao: bool,
    periodo: date,
) -> pd.DataFrame:
    habilitar_suporte_loguru()
    tabela_consolidada = pd.DataFrame(
        columns=[
            "id",
            "municipio_id_sus",
            "periodo_id",
            "periodo_codigo",
            "cnes_id",
            "cnes_nome",
            "equipe_id_ine",
            "quantidade",
            "criterio_pontuacao",
            "criacao_data",
            "atualizacao_data",
        ]
    )

    periodo_cod = periodo_por_data(sessao=sessao, data=periodo)
    tabela_consolidada[
        [
            "municipio_id_sus",
            "cnes_id",
            "cnes_nome",
            "equipe_id_ine",
            "quantidade",
        ]
    ] = dados_sisab_cadastros.loc[
        :, ["IBGE", "CNES", "Nome UBS", "INE", "quantidade"]
    ]
    tabela_consolidada["criterio_pontuacao"] = com_ponderacao
    tabela_consolidada["periodo_codigo"] = periodo_cod[3]
    tabela_consolidada.reset_index(drop=True, inplace=True)
    tabela_consolidada["id"] = tabela_consolidada.apply(
        lambda row: uuid7(), axis=1
    )
    tabela_consolidada["criacao_data"] = datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    tabela_consolidada["atualizacao_data"] = datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )

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

    tabela_consolidada["id"] = tabela_consolidada["id"].astype("string")
    tabela_consolidada["municipio_id_sus"] = tabela_consolidada[
        "municipio_id_sus"
    ].astype("string")
    tabela_consolidada["periodo_id"] = tabela_consolidada["periodo_id"].astype(
        "string"
    )
    tabela_consolidada["periodo_codigo"] = tabela_consolidada[
        "periodo_codigo"
    ].astype("string")
    tabela_consolidada["cnes_id"] = tabela_consolidada["cnes_id"].astype(
        "string"
    )
    tabela_consolidada["cnes_nome"] = tabela_consolidada["cnes_nome"].astype(
        "string"
    )
    tabela_consolidada["unidade_geografica_id"] = tabela_consolidada[
        "unidade_geografica_id"
    ].astype("string")
    tabela_consolidada["equipe_id_ine"] = tabela_consolidada[
        "equipe_id_ine"
    ].astype("string")
    tabela_consolidada["quantidade"] = tabela_consolidada["quantidade"].astype(
        int
    )
    tabela_consolidada["criterio_pontuacao"] = tabela_consolidada[
        "criterio_pontuacao"
    ].astype(bool)
    tabela_consolidada["criacao_data"] = tabela_consolidada[
        "criacao_data"
    ].astype("string")
    tabela_consolidada["atualizacao_data"] = tabela_consolidada[
        "atualizacao_data"
    ].astype("string")

    return tabela_consolidada
