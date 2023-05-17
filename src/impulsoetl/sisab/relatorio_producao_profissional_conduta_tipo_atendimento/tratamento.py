import warnings

warnings.filterwarnings("ignore")
from datetime import date

import pandas as pd
import numpy as np

from typing import Final
from frozendict import frozendict

from impulsoetl.loggers import logger, habilitar_suporte_loguru

COLUNAS_RENOMEAR: Final[dict[str, str]] = {
    "uf_sigla":"municipio_uf",
    "periodo_data_inicio":"periodo_data",
    "Conduta":"conduta",
    "Categoria do Profissional":"categoria_profissional",
    "Tipo de Atendimento":"tipo_atendimento",
    "quantidade_aprovada":"quantidade"
}

COLUNAS_TIPOS: Final[frozendict] = frozendict(
    {
    "municipio_uf":"str",
    "periodo_data":"str",
    "municipio_nome":"str",
    "periodo_data":"str",
    "conduta":"str",
    "categoria_profissional":"str",
    "tipo_atendimento":"str",
    "quantidade":"int"
    }
)

def tratar_tipos(df_extraido: pd.DataFrame) -> pd.DataFrame:
    df_extraido = df_extraido.astype(COLUNAS_TIPOS, errors="ignore").where(
        df_extraido.notna(), None
    )
    return df_extraido

def renomear_colunas(df_extraido: pd.DataFrame) -> pd.DataFrame:
    df_extraido.rename(columns=COLUNAS_RENOMEAR, inplace=True)
    return df_extraido

def tratamento_dados(
        df_extraido: pd.DataFrame, periodo_id: str, unidade_geografica_id: str
)-> pd.DataFrame:
    df_extraido = renomear_colunas(df_extraido)
    tratar_tipos(df_extraido)
    df_extraido["periodo_id"] = periodo_id
    df_extraido["unidade_geografica_id"] = unidade_geografica_id
    df_extraido = df_extraido.reset_index(drop=True)

    return df_extraido
