# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Junta etapas do fluxo de ETL de indicadores de desempenho dos municípios."""

from __future__ import annotations

from datetime import date
from typing import Final

from sqlalchemy.orm import Session

from impulsoetl.sisab.indicadores_municipios.carregamento import (
    carregar_indicadores,
)
from impulsoetl.sisab.indicadores_municipios.extracao import (
    extrair_indicadores,
)
from impulsoetl.sisab.indicadores_municipios.verificacao import (
    verificar_indicadores_municipios
)
from impulsoetl.sisab.indicadores_municipios.tratamento import (
    transformar_indicadores,
)


INDICADORES_CODIGOS: Final[dict[str, str]] = {
    "Pré-Natal (6 consultas)": "1",
    "Pré-Natal (Sífilis e HIV)": "2",
    "Gestantes Saúde Bucal": "3",
    "Cobertura Citopatológico": "4",
    "Cobertura Polio e Penta": "5",
    "Hipertensão (PA Aferida)": "6",
    "Diabetes (Hemoglobina Glicada)": "7",
}


def obter_indicadores_desempenho(
    sessao: Session,
    visao_equipe: str,
    quadrimestre: date,
    teste: bool = False,
) -> None:
    for indicador in INDICADORES_CODIGOS:
        df = extrair_indicadores(
            visao_equipe=visao_equipe,
            quadrimestre=quadrimestre,
            indicador=indicador,
        )
        df_tratado = transformar_indicadores(
            sessao=sessao,
            df_extraido=df,
            periodo=quadrimestre,
            indicador=indicador,
        )
        verificar_indicadores_municipios(df=df, df_tratado=df_tratado)
        carregar_indicadores(
            sessao=sessao,
            indicadores_transformada=df_tratado,
            visao_equipe=visao_equipe,
        )
