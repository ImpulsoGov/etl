import warnings

warnings.filterwarnings("ignore")
from datetime import date
from typing import Final

import numpy as np
import pandas as pd
from frozendict import frozendict
#from prefect import task

from impulsoetl.loggers import habilitar_suporte_loguru, logger
from impulsoetl.scnes.estabelecimentos_equipes.extracao import extrair_equipes
from impulsoetl.scnes.estabelecimentos_profissionais.extracao import (
    extrair_profissionais,
)
from impulsoetl.scnes.extracao_lista_cnes import extrair_lista_cnes

COLUNAS_EXCLUIR = [
    "tpSusNaoSus",
    "artigo2",
    "artigo3",
    "artigo5",
    "dtEntrada_x",
    "cnsMaster",
]

COLUNAS_RENOMEAR: Final[dict[str, str]] = {
    "INE": "equipe_id_ine",
    "nome": "profissional_nome",
    "cns": "profissional_cns",
    "cbo": "profissional_cbo",
    "dsCbo": "profissional_ocupacao",
    "vinculacao": "profissional_vinculacao",
    "vinculo": "profissional_vinculo_tipo",
    "subVinculo": "profissional_vinculo_subptipo",
    "chHosp": "carga_horaria_hospitalar",
    "chAmb": "carga_horaria_ambulatorial",
    "chOutros": "carga_horaria_outras",
    "dtEntrada_y": "periodo_data_entrada",
    "dtDesligamento": "periodo_data_desligamento",
}

COLUNAS_TIPOS: Final[frozendict] = frozendict(
    {
        "municipio_id_sus": "str",
        "estabelecimento_cnes_id": "str",
        "equipe_id_ine": "str",
        "profissional_nome": "str",
        "profissional_cns": "str",
        "profissional_cbo": "str",
        "profissional_ocupacao": "str",
        "profissional_vinculacao": "str",
        "profissional_vinculo_tipo": "str",
        "profissional_vinculo_subptipo": "str",
        "carga_horaria_hospitalar": "Int64",
        "carga_horaria_ambulatorial": "Int64",
        "carga_horaria_outras": "Int64",
        "periodo_data_entrada": "str",
        "periodo_data_desligamento": "str",
    }
)

COLUNAS_DATA = ["periodo_data_entrada", "periodo_data_desligamento"]

COLUNAS_CARGA_HORARIA = ["carga_horaria_hospitalar","carga_horaria_ambulatorial", "carga_horaria_outras"]

def renomear_colunas(df_extraido: pd.DataFrame) -> pd.DataFrame:
    df_extraido.rename(columns=COLUNAS_RENOMEAR, inplace=True)
    return df_extraido


def excluir_colunas(df_extraido: pd.DataFrame) -> pd.DataFrame:
    df_extraido.drop(columns=COLUNAS_EXCLUIR, inplace=True)
    return df_extraido


def tratar_tipos(df_extraido: pd.DataFrame) -> pd.DataFrame:
    for coluna in COLUNAS_DATA:
        df_extraido[coluna] = pd.to_datetime(
            df_extraido[coluna], infer_datetime_format=True, errors="coerce"
        )

    for coluna in COLUNAS_CARGA_HORARIA:
        df_extraido[coluna] =  df_extraido[coluna].round(0)

    df_extraido = df_extraido.astype(COLUNAS_TIPOS, errors="ignore").where(
        df_extraido.notna(), None
    )

    return df_extraido


def ordenar_colunas(df_extraido: pd.DataFrame, COLUNAS_TIPOS: dict):
    ordem_colunas = list(COLUNAS_TIPOS.keys())
    df_extraido = df_extraido[ordem_colunas]

    return df_extraido

"""
@task(
    name="Tratar Informações dos Profissionais de Saúde",
    description=(
        "Trata os dados dos profisisonais de saúde dos estabelecimentos de cada município"
        + "a partir da página do CNES."
    ),
    tags=["cnes", "profissionais", "tratamento"],
    retries=2,
    retry_delay_seconds=120,
)
"""
def tratamento_dados(
    df_extraido: pd.DataFrame, periodo_id: str, unidade_geografica_id: str
) -> pd.DataFrame:
    """
    Trata os dados extraídos para os profissionais de saúde a partir da página do CNES
     Argumentos:
        df_extraido: [`DataFrame`][] contendo os dados extraídos no na página do CNES
            (conforme retornado pela função [`extrair_informacoes_estabelecimentos()`][]).
        periodo_id: Código de identificação do período.
        unidade_geografica_id: Código de identificação da unidade geográfica.
     Retorna:
        Objeto [`pandas.DataFrame`] com os dados enriquecidos e tratados.
    """
    habilitar_suporte_loguru()
    logger.info("Iniciando o tratamento dos dados...")

    df_extraido = excluir_colunas(df_extraido)
    df_extraido = renomear_colunas(df_extraido)
    df_extraido = ordenar_colunas(df_extraido, COLUNAS_TIPOS)
    df_extraido = tratar_tipos(df_extraido)
    df_extraido["periodo_id"] = periodo_id
    df_extraido["unidade_geografica_id"] = unidade_geografica_id
    df_extraido = df_extraido.reset_index(drop=True)

    logger.info("Dados tratados com sucesso ...")

    return df_extraido
