from typing import Final

import pandas as pd
from frozendict import frozendict

from impulsoetl.cnes.estabelecimentos_horarios.extracao import (
    extrair_horario_atendimento_estabelecimentos,
)
from impulsoetl.cnes.extracao_lista_cnes import extrair_lista_cnes
from impulsoetl.loggers import logger

COLUNAS_EXCLUIR = ["diaSemana", "hrInicioAtendimento", "hrFimAtendimento"]

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


def tratamento_dados(
    df_extraido: pd.DataFrame, periodo_id: str, unidade_geografica_id: str
) -> pd.DataFrame:

    logger.info("Iniciando o tratamento dos dados ...")

    df_extraido["horario_funcionamento"] = (
        df_extraido["diaSemana"]
        + " "
        + df_extraido["hrInicioAtendimento"]
        + "-"
        + df_extraido["hrFimAtendimento"]
    )
    df_extraido = df_extraido.groupby(
        ["municipio_id_sus", "estabelecimento_cnes_id"], as_index=False
    ).agg({"horario_funcionamento": " ".join})
    df_extraido["periodo_id"] = periodo_id
    df_extraido["unidade_geografica_id"] = unidade_geografica_id
    df_tratado = df_extraido.reset_index(drop=True)
    tratar_tipos(df_tratado)

    logger.info("Dados transformados ...")

    return df_tratado


periodo_id = "2023"
unidade_geografica_id = "sp2023"
codigo_municipio = "120001"
lista_codigos = extrair_lista_cnes(codigo_municipio)
df_extraido = extrair_horario_atendimento_estabelecimentos(
    codigo_municipio, lista_codigos
)
df_tratado = tratamento_dados(df_extraido, periodo_id, unidade_geografica_id)
print(df_tratado)
