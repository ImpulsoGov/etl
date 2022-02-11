# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Obtém dados de abandonos de usuários recentes em CAPS por coorte de 6 meses."""


from __future__ import annotations

from datetime import datetime

import janitor  # noqa: F401  # nopycln: import
import pandas as pd
from sqlalchemy.orm import Session

from impulsoetl.indicadores_saude_mental.comum import consultar_abandonos
from impulsoetl.loggers import logger


def consolidar_abandonos_6m(
    usuarios_abandonaram: pd.DataFrame,
    tempo_no_caps: int = 6,
) -> pd.DataFrame:
    return (
        usuarios_abandonaram.groupby(
            [
                "estabelecimento_nome",
                "competencia_primeiro_procedimento",
                "usuario_id",
            ]
        )
        .agg({"abandonou": "any"})
        .reset_index()
        .groupby(
            [
                "estabelecimento_nome",
                "competencia_primeiro_procedimento",
            ]
        )
        .agg(
            quantidade_usuarios_coorte=("usuario_id", "nunique"),
            quantidade_abandonos_coorte=("abandonou", "sum"),
        )
        .reset_index()
        .transform_column(
            "competencia_primeiro_procedimento",
            function=lambda dt: dt + pd.DateOffset(months=tempo_no_caps),
            dest_column_name="competencia",
        )
    )


def carregar_abandonos_6m(
    sessao: Session,
    abandonos_6m: pd.DataFrame,
) -> int:

    tabela_nome = "saude_mental._abandono_6m"
    num_registros = len(abandonos_6m)

    logger.info(
        "Preparando carregamento de {num_registros} registros taxa de abandono"
        " agregada para a tabela `{tabela_nome}`...",
        num_registros=num_registros,
        tabela_nome=tabela_nome,
    )
    conector = sessao.connection()
    abandonos_6m.to_sql(
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


def obter_abandonos_6m(
    sessao: Session,
    unidade_geografica_id_sus: str,
    periodo_data_inicio: datetime,
    tempo_no_caps: int = 6,
    teste: bool = False,
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
    abandonos_6m = consolidar_abandonos_6m(
        usuarios_abandonaram=usuarios_abandonaram,
    )
    carregar_abandonos_6m(
        sessao=sessao,
        abandonos_6m=abandonos_6m,
    )
