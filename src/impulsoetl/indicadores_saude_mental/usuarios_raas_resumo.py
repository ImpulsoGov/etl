# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Consolida as informações de RAAS para um usuário em um CAPS."""


from __future__ import annotations

from functools import lru_cache

import janitor  # noqa: F401  # nopycln: import
import numpy as np
import pandas as pd
from sqlalchemy.orm import Session

from impulsoetl.indicadores_saude_mental.comum import (
    consultar_raas,
    primeiro_com_info,
)
from impulsoetl.loggers import logger


def resumir_raas_por_usuario(raas: pd.DataFrame) -> pd.DataFrame:
    logger.info("Resumindo RAAS por combinação usuário - estabelecimento.")
    resumos_raas_por_usuario = (
        raas.sort_values(
            [
                "municipio_id",
                "estabelecimento_nome",
                "usuario_id",
                "competencia_realizacao",
            ]
        )
        .groupby(
            [
                "municipio_id",
                "estabelecimento_nome",
                "usuario_id",
            ],
            dropna=False,
        )
        .agg(
            {
                "atividade_descricao": primeiro_com_info,
                "cid_descricao": primeiro_com_info,
                "cid_grupo_descricao_curta": primeiro_com_info,
                "cid_grupo_descricao_longa": primeiro_com_info,
                "encaminhamento_origem_descricao": primeiro_com_info,
                "estabelecimento_latitude": primeiro_com_info,
                "estabelecimento_longitude": primeiro_com_info,
                "estabelecimento_tipo_descricao": primeiro_com_info,
                "raas_competencia_inicio": "min",
                "servico_classificacao_descricao": primeiro_com_info,
                "servico_descricao": primeiro_com_info,
                "usuario_abertura_raas": "min",
                "usuario_estabelecimento_referencia_latitude": primeiro_com_info,
                "usuario_estabelecimento_referencia_longitude": primeiro_com_info,
                "usuario_estabelecimento_referencia_nome": primeiro_com_info,
                "usuario_faixa_etaria": primeiro_com_info,
                "usuario_raca_cor": primeiro_com_info,
                "usuario_sexo": primeiro_com_info,
                "usuario_situacao_rua": primeiro_com_info,
                "usuario_substancias_abusa": primeiro_com_info,
            }
        )
        .reset_index()
        # indicar estabelecimentos sem referência geográfica
        .join_apply(
            lambda i: (
                isinstance(
                    i["usuario_estabelecimento_referencia_latitude"], float
                )
                and isinstance(
                    i["usuario_estabelecimento_referencia_longitude"], float
                )
            ),
            new_column_name="usuario_estabelecimento_referencia_temlatlong",
        )
        .update_where(
            "not usuario_estabelecimento_referencia_temlatlong",
            target_column_name="usuario_estabelecimento_referencia_latitude",
            target_val=np.nan,
        )
        .update_where(
            "not usuario_estabelecimento_referencia_temlatlong",
            target_column_name="usuario_estabelecimento_referencia_longitude",
            target_val=np.nan,
        )
        .remove_columns(["usuario_estabelecimento_referencia_temlatlong"])
    )
    logger.info("OK.")
    return resumos_raas_por_usuario


def carregar_resumos_raas(
    sessao: Session,
    raas_resumidas_por_usuario: pd.DataFrame,
) -> int:

    tabela_nome = "saude_mental.usuarios_raas_resumo"
    num_registros = len(raas_resumidas_por_usuario)

    logger.info(
        "Preparando carregamento de {num_registros} resumos de RAAS por "
        "usuário para a tabela `{tabela_nome}`...",
        num_registros=num_registros,
        tabela_nome=tabela_nome,
    )
    conector = sessao.connection()
    raas_resumidas_por_usuario.to_sql(
        name=tabela_nome.split(".")[-1],
        con=conector,
        schema=tabela_nome.split(".")[0],
        chunksize=1000,
        if_exists="replace",  # TODO!: mudar para append, removendo seletiva/e
        index=False,
        method="multi",
    )
    logger.info("OK.")

    return 0


@lru_cache(1)
def obter_resumos_raas_por_usuario(
    sessao: Session,
    unidade_geografica_id_sus: str,
    teste: bool = False,
) -> None:
    logger.info(
        "Resumindo RAAS por usuário no município de ID {}",
        unidade_geografica_id_sus,
    )
    raas = consultar_raas(
        sessao=sessao,
        unidade_geografica_id_sus=unidade_geografica_id_sus,
    )
    raas_resumidas_por_usuario = resumir_raas_por_usuario(raas=raas)
    carregar_resumos_raas(
        sessao=sessao,
        raas_resumidas_por_usuario=raas_resumidas_por_usuario,
    )
    if not teste:
        sessao.commit()
