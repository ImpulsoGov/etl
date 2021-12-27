# SPDX-FileCopyrightText: 2021 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Obtém dados mensais sobre o abandono de usuários recentes em CAPS."""


from __future__ import annotations

from datetime import datetime

import janitor  # noqa: F401  # nopycln: import
import pandas as pd
from sqlalchemy.orm import Session

from impulsoetl.indicadores_saude_mental.comum import (
    consultar_abandonos,
    consultar_usuarios_raas_resumo,
)
from impulsoetl.loggers import logger

colunas_a_agrupar = [
    "competencia_realizacao",
    "servico_descricao",
    "servico_classificacao_descricao",
    "estabelecimento_tipo_descricao",
    "estabelecimento_nome",
    "estabelecimento_latitude",
    "estabelecimento_longitude",
    "encaminhamento_origem_descricao",
    "usuario_sexo",
    "usuario_raca_cor",
    "usuario_faixa_etaria",
    "usuario_situacao_rua",
    "usuario_substancias_abusa",
    "usuario_estabelecimento_referencia_nome",
    "usuario_estabelecimento_referencia_latitude",
    "usuario_estabelecimento_referencia_longitude",
    "usuario_tempo_no_servico",
    "usuario_novo",
    "cid_descricao",
    "cid_grupo_descricao_curta",
]


def consolidar_abandonos_mensais(
    usuarios_raas_resumo: pd.DataFrame,
    usuarios_abandonaram: pd.DataFrame,
):
    abandono_mensal = (
        usuarios_abandonaram.merge(
            usuarios_raas_resumo,
            on=["estabelecimento_nome", "usuario_id"],
            how="left",
            validate="m:1",
        )
        .groupby(list(colunas_a_agrupar))
        .agg(
            abandonaram_no_mes=("inicia_inatividade", "sum"),
            usuarios_ativos_recem_chegados=("usuario_id", "count"),
        )
        .reset_index()
        .fill_empty(["abandonaram_no_mes"], value=0)
        .astype({"abandonaram_no_mes": "int16"})
        .reorder_columns(["competencia"])
    )
    return abandono_mensal


def carregar_abandonos_mensais(
    sessao: Session,
    abandonos_mensais: pd.DataFrame,
) -> int:

    tabela_nome = "saude_mental._abandono_mensal"
    num_registros = len(abandonos_mensais)

    logger.info(
        "Preparando carregamento de {num_registros} registros taxa de abandono"
        " mensal para a tabela `{tabela_nome}`...",
        num_registros=num_registros,
        tabela_nome=tabela_nome,
    )
    conector = sessao.connection()
    abandonos_mensais.to_sql(
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


def obter_abandonos(
    sessao: Session,
    unidade_geografica_id_sus: str,
    periodo_data_inicio: datetime,
    tempo_no_caps: int = 6,
    intervalo_inatividade: int = 3,
    remover_perfil_ambulatorial: bool = True,
) -> None:

    logger.info(
        "Iniciando consolidação da taxa de abandono mensal entre usuários "
        "recentes em CAPS no município de ID "
        + "{unidade_geografica_id_sus} na competencia de "
        + "{periodo_data_inicio:%m/%Y}...",
        unidade_geografica_id_sus=unidade_geografica_id_sus,
        periodo_data_inicio=periodo_data_inicio,
    )
    usuarios_abandonaram = consultar_abandonos(
        sessao=sessao,
        unidade_geografica_id_sus=unidade_geografica_id_sus,
        periodo_data_inicio=periodo_data_inicio,
    )
    usuarios_raas_resumo = consultar_usuarios_raas_resumo(
        sessao=sessao,
        unidade_geografica_id_sus=unidade_geografica_id_sus,
    )
    abandonos_mensais = consolidar_abandonos_mensais(
        usuarios_raas_resumo=usuarios_raas_resumo,
        usuarios_abandonaram=usuarios_abandonaram,
    )
    carregar_abandonos_mensais(
        sessao=sessao,
        abandonos_mensais=abandonos_mensais,
    )
