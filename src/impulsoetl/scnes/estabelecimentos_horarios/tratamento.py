from typing import Final

import pandas as pd
from frozendict import frozendict
from prefect import task

from impulsoetl.scnes.estabelecimentos_horarios.extracao import extrair_horarios_estabelecimentos
from impulsoetl.scnes.extracao_lista_cnes import extrair_lista_cnes
from impulsoetl.loggers import logger, habilitar_suporte_loguru

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
    #habilitar_suporte_loguru()
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
