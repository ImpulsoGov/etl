from typing import Final
import warnings

warnings.filterwarnings("ignore")

import json

import pandas as pd
from frozendict import frozendict
from prefect import task

from impulsoetl.scnes.estabelecimentos_horarios.extracao import (
    extrair_horarios_estabelecimentos,
)
from impulsoetl.scnes.extracao_lista_cnes import extrair_lista_cnes
from impulsoetl.loggers import logger, habilitar_suporte_loguru

# COLUNAS_EXCLUIR = ["diaSemana", "hrInicioAtendimento", "hrFimAtendimento","horario"]

COLUNAS_TIPOS: Final[frozendict] = frozendict(
    {
        "municipio_id_sus": "str",
        "estabelecimento_cnes_id": "str",
        "horario_funcionamento": "str",
        "periodo_id": "str",
        "unidade_geografica_id": "str",
    }
)


def excluir_colunas(df_extraido: pd.DataFrame) -> pd.DataFrame:
    df_extraido.drop(columns=COLUNAS_EXCLUIR, inplace=True)


def tratar_tipos(df_extraido: pd.DataFrame) -> pd.DataFrame:
    df_extraido = df_extraido.astype(COLUNAS_TIPOS, errors="ignore").where(
        df_extraido.notna(), None
    )


def transformar_horarios_json(df_extraido: pd.DataFrame) -> pd.DataFrame:
    df_extraido["horario"] = (
        +df_extraido["hrInicioAtendimento"]
        + "-"
        + df_extraido["hrFimAtendimento"]
    )

    df_consolidado = pd.DataFrame()

    lista_cnes = (
        df_extraido["estabelecimento_cnes_id"].value_counts().index.tolist()
    )

    for cnes in lista_cnes:
        df = df_extraido.loc[df_extraido["estabelecimento_cnes_id"] == cnes]
        df["horario_funcionamento"] = json.dumps(
            [{a: b} for a, b in zip(df["diaSemana"], df["horario"])],
            ensure_ascii=False,
        )
        df = df[
            [
                "municipio_id_sus",
                "estabelecimento_cnes_id",
                "horario_funcionamento",
            ]
        ]
        df = df.drop_duplicates().reset_index(drop=True)
        df_consolidado = df_consolidado.append(df)

    return df_consolidado


def tratamento_dados(
    df_extraido: pd.DataFrame, periodo_id: str, unidade_geografica_id: str
) -> pd.DataFrame:
    """
    Trata os dados extraídos para as equipes de saúde a partir da página do CNES
     Argumentos:
        df_extraido: [`DataFrame`][] contendo os dados extraídos no na página do CNES
            (conforme retornado pela função [`extrair_informacoes_estabelecimentos()`][]).
        periodo_id: Código de identificação do período.
        unidade_geografica_id: Código de identificação da unidade geográfica.
     Retorna:
        Objeto [`pandas.DataFrame`] com os dados enriquecidos e tratados.
    """
    # habilitar_suporte_loguru()
    logger.info("Iniciando o tratamento dos dados ...")

    df = transformar_horarios_json(df_extraido)
    df["periodo_id"] = periodo_id
    df["unidade_geografica_id"] = unidade_geografica_id
    df_tratado = df.reset_index(drop=True)
    tratar_tipos(df_tratado)

    logger.info("Dados transformados ...")

    return df_tratado
