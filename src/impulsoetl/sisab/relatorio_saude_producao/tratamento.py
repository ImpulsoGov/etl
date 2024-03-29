import warnings

warnings.filterwarnings("ignore")
from datetime import date
from typing import Final

import numpy as np
import pandas as pd
from frozendict import frozendict
from prefect import task

from impulsoetl.loggers import habilitar_suporte_loguru, logger

COLUNAS_EXCLUIR = ["uf_sigla", "municipio_nome"]

COLUNAS_RENOMEAR: Final[dict[str, str]] = {
    "Categoria do Profissional": "categoria_profissional",
    "Problema/Condição Avaliada": "problema_condicao_avaliada",
    "Conduta": "conduta",
    "quantidade_aprovada": "quantidade",
}

COLUNAS_TIPOS: Final[frozendict] = frozendict(
    {
        "municipio_id_sus": "str",
        "periodo_data_inicio": "str",
        "categoria_profissional": "str",
        "problema_condicao_avaliada": "str",
        "conduta": "str",
        "quantidade": "Int64",
    }
)


def renomear_colunas(df_extraido: pd.DataFrame) -> pd.DataFrame:
    df_extraido.rename(columns=COLUNAS_RENOMEAR, inplace=True)
    return df_extraido


def excluir_colunas(df_extraido: pd.DataFrame) -> pd.DataFrame:
    df_extraido.drop(columns=COLUNAS_EXCLUIR, inplace=True)
    return df_extraido


def tratamento_valores_negativos(df_extraido: pd.DataFrame) -> pd.DataFrame:
    df_extraido["quantidade"] = df_extraido["quantidade"].abs()
    return df_extraido


def tratar_tipos(df_extraido: pd.DataFrame) -> pd.DataFrame:
    df_extraido = df_extraido.astype(COLUNAS_TIPOS, errors="ignore").where(
        df_extraido.notna(), None
    )
    return df_extraido


def ordenar_colunas(df_extraido: pd.DataFrame, COLUNAS_TIPOS: dict):
    ordem_colunas = list(COLUNAS_TIPOS.keys())
    df_extraido = df_extraido[ordem_colunas]

    return df_extraido


@task(
    name="Transformar Relatório de Produção de Saúde ",
    description=(
        "Transforma os dados do relatório de Produção de Saúde extraído a partir da página do SISAB."
    ),
    tags=["sisab", "produção", "tratamento"],
    retries=0,
    retry_delay_seconds=None,
)
def tratamento_dados(
    df_extraido: pd.DataFrame, periodo_id: str, unidade_geografica_id: str
) -> pd.DataFrame:

    """
    Trata os dados do Relatório de Produção de Saúde do SISAB:
     Argumentos:
        df_extraido: [`DataFrame`][] contendo os dados extraídos do Relatório de Produção do SISAB
            (conforme retornado pela função [`extrair_relatório()`][]).
        periodo_id: Código de identificação do período.
        unidade_geografica_id: Código de identificação da unidade geográfica.
     Retorna:
        Objeto [`pandas.DataFrame`] com os dados enriquecidos e tratados.
    """

    habilitar_suporte_loguru()
    logger.info("Iniciando o tratamento dos dados...")

    df_extraido = excluir_colunas(df_extraido)
    df_extraido = renomear_colunas(df_extraido)
    df_extraido = tratamento_valores_negativos(df_extraido)
    df_extraido = ordenar_colunas(df_extraido, COLUNAS_TIPOS)
    df_extraido = tratar_tipos(df_extraido)
    df_extraido["periodo_id"] = periodo_id
    df_extraido["unidade_geografica_id"] = unidade_geografica_id
    df_extraido = df_extraido.reset_index(drop=True)

    logger.info("Dados tratados com sucesso ...")

    return df_extraido