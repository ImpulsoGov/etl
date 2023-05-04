import warnings

warnings.filterwarnings("ignore")
import json
from datetime import date
from typing import Final

import numpy as np
import pandas as pd
import requests
from frozendict import frozendict
from prefect import task
from sqlalchemy.orm import Session

from impulsoetl.bd import Sessao
from impulsoetl.loggers import habilitar_suporte_loguru, logger
from impulsoetl.scnes.estabelecimentos_equipes.extracao import extrair_equipes
from impulsoetl.scnes.extracao_lista_cnes import extrair_lista_cnes

COLUNAS_EXCLUIR = ["coArea", "coMunicipio", "dsArea", "seqEquipe", "tpEquipe"]

COLUNAS_RENOMEAR: Final[dict[str, str]] = {
    "dsEquipe": "equipe_tipo",
    "coEquipe": "equipe_id_ine",
    "nomeEquipe": "equipe_nome",
    "quilombola": "equipe_atencao_quilombola",
    "assentada": "equipe_atencao_assentada",
    "geral": "equipe_atencao_geral",
    "escola": "equipe_atencao_escola",
    "pronasci": "equipe_atencao_pronasci",
    "indigena": "equipe_atencao_indigena",
    "ribeirinha": "equipe_atencao_ribeirinha",
    "complem": "equipe_atencao_complementar",
    "dtAtivacao": "periodo_ativacao",
    "dtDesativacao": "periodo_desativacao",
}

COLUNAS_TIPOS: Final[frozendict] = frozendict(
    {
        "municipio_id_sus": "str",
        "estabelecimento_cnes_id": "str",
        "equipe_id_ine": "str",
        "equipe_tipo": "str",
        "equipe_tipo": "str",
        "equipe_nome": "str",
        "equipe_atencao_quilombola": "int",
        "equipe_atencao_assentada": "int",
        "equipe_atencao_geral": "int",
        "equipe_atencao_escola": "int",
        "equipe_atencao_pronasci": "int",
        "equipe_atencao_indigena": "int",
        "equipe_atencao_ribeirinha": "int",
        "equipe_atencao_complementar": "int",
        "periodo_ativacao": "str",
        "periodo_desativacao": "str",
    }
)

COLUNAS_DATA = ["periodo_ativacao", "periodo_desativacao"]


def renomear_colunas(df_extraido: pd.DataFrame) -> pd.DataFrame:
    df_extraido.rename(columns=COLUNAS_RENOMEAR, inplace=True)
    return df_extraido


def excluir_colunas(df_extraido: pd.DataFrame) -> pd.DataFrame:
    df_extraido.drop(columns=COLUNAS_EXCLUIR, inplace=True)
    return df_extraido


def tratar_tipos(df_extraido: pd.DataFrame) -> pd.DataFrame:
    for coluna in COLUNAS_DATA:
        df_extraido[coluna] = pd.to_datetime(
            df_extraido[coluna], infer_datetime_format=True
        )

    df_extraido = df_extraido.astype(COLUNAS_TIPOS, errors="ignore").where(
        df_extraido.notna(), None
    )

    return df_extraido


def tratar_tipos(df_extraido: pd.DataFrame) -> pd.DataFrame:
    for coluna in COLUNAS_DATA:
        df_extraido[coluna] = pd.to_datetime(
            df_extraido[coluna], infer_datetime_format=True, errors="coerce"
        )

    df_extraido = df_extraido.astype(COLUNAS_TIPOS, errors="ignore").where(
        df_extraido.notna(), None
    )

    return df_extraido


def ordenar_colunas(df_extraido: pd.DataFrame, COLUNAS_TIPOS: dict):
    ordem_colunas = list(COLUNAS_TIPOS.keys())
    df_extraido = df_extraido[ordem_colunas]

    return df_extraido


@task(
    name="Tratar Informações das Equipes",
    description=(
        "Trata os dados das equipes dos estabelecimentos de saúde"
        + "a partir da página do CNES."
    ),
    tags=["cnes", "equipes", "tratamento"],
    retries=2,
    retry_delay_seconds=120,
)
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
    habilitar_suporte_loguru()
    logger.info("Iniciando o tratamento dos dados ...")

    df_extraido = excluir_colunas(df_extraido)
    df_extraido = renomear_colunas(df_extraido)
    df_extraido = ordenar_colunas(df_extraido, COLUNAS_TIPOS)
    df_extraido = tratar_tipos(df_extraido)
    df_extraido["periodo_id"] = periodo_id
    df_extraido["unidade_geografica_id"] = unidade_geografica_id
    df_extraido = df_extraido.reset_index(drop=True)

    logger.info("Dados transformados ...")

    return df_extraido
