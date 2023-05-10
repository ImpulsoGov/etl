# SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT

from typing import Final

import pandas as pd
from frozendict import frozendict
from prefect import task
from sqlalchemy.orm import Session

from impulsoetl.comum.geografias import id_sus_para_id_impulso
from impulsoetl.loggers import habilitar_suporte_loguru, logger

CADASTROS_COLUNAS_TIPOS: Final[frozendict] = frozendict(
    {
        "municipio_id_sus": str,
        "periodo_id": str,
        "cnes_id": str,
        "cnes_nome": str,
        "equipe_id_ine": str,
        "quantidade": "int64",
        "periodo_codigo": str,
        "criterio_pontuacao": bool,
        "unidade_geografica_id": str,
    }
)

CADASTROS_COLUNAS: Final[dict[str, str]] = {
    "IBGE": "municipio_id_sus",
    "CNES": "cnes_id",
    "Estabelecimento": "cnes_nome",
    "INE": "equipe_id_ine",
}


def renomear_colunas(
    df_extraido: pd.DataFrame,
):
    return df_extraido.rename(columns=CADASTROS_COLUNAS).rename(
        columns={df_extraido.columns[7]: "quantidade"}
    )


def excluir_colunas(
    df_tratado: pd.DataFrame,
):
    return df_tratado.drop(
        ["Uf", "Municipio", "Sigla da equipe", "Unnamed: 8"], axis=1
    ).dropna()


def garantir_tipos_colunas(
    df_tratado: pd.DataFrame,
):
    return df_tratado.astype(CADASTROS_COLUNAS_TIPOS)


def definir_coluna_criterio_pontuacao(
    df_tratado: pd.DataFrame,
    com_ponderacao: bool,
):
    return df_tratado.insert(
        1, "criterio_pontuacao", com_ponderacao, allow_duplicates=True
    )


def definir_coluna_periodo_codigo(
    df_tratado: pd.DataFrame,
    periodo_codigo: str,
):
    return df_tratado.insert(
        1, "periodo_codigo", periodo_codigo, allow_duplicates=True
    )


def definir_coluna_periodo_id(
    df_tratado: pd.DataFrame,
    periodo_id: str,
):
    return df_tratado.insert(
        1, "periodo_id", periodo_id, allow_duplicates=True
    )


def definir_coluna_unidade_geografica_id(
    df_tratado: pd.DataFrame,
    sessao: Session,
):
    df_tratado["unidade_geografica_id"] = df_tratado["municipio_id_sus"].apply(
        lambda municipio_id_sus: id_sus_para_id_impulso(
            sessao=sessao,
            id_sus=municipio_id_sus,
        )
    )
    return df_tratado


@task(
    name="Transformar Cadastros Individuais",
    description=(
        "Transforma os dados de cadastros individuais extraídos do portal "
        + "público do Sistema de Informação em Saúde para a Atenção Básica do "
        + "SUS."
    ),
    tags=["aps", "sisab", "cadastros_individuais", "transformacao"],
    retries=0,
    retry_delay_seconds=None,
)
def tratar_dados(
    sessao: Session,
    df_extraido: pd.DataFrame,
    com_ponderacao: bool,
    periodo_id: str,
    periodo_codigo: str,
) -> pd.DataFrame:
    """Inclui todas etapas de transformação dos dados de cadastros de equipes pelo SISAB.

    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov.
        df_extraido: DataFrame do relatŕorio extraído no SISAB.
        visao_equipe: Indica a situação da equipe considerada para a contagem
            dos cadastros.
        com_ponderacao: Lista de booleanos indicando quais tipos de população
            devem ser filtradas no cadastro - onde `True` indica apenas as
            populações com critério de ponderação e `False` indica todos os
            cadastros. Por padrão, o valor é `[True, False]`, indicando que
            ambas as possibilidades são extraídas do SISAB e carregadas para a
            mesma tabela de destino.
        periodo_id: Identificador único do período referente ao mês/ano de disponibilização do relatório.
        periodo_codigo: Código do período referente ao mês/ano de disponibilização do relatório.
    """

    habilitar_suporte_loguru()

    logger.info("Renomeando colunas da tabela...")
    print(df_extraido)
    df_tratado = renomear_colunas(df_extraido=df_extraido)

    logger.info("Exluindo colunas não úteis da tabela...")
    df_tratado = excluir_colunas(df_tratado=df_tratado)

    logger.info("Ennquicimento de tabela com novas colunas...")
    definir_coluna_criterio_pontuacao(
        df_tratado=df_tratado, com_ponderacao=com_ponderacao
    )
    definir_coluna_periodo_codigo(
        df_tratado=df_tratado, periodo_codigo=periodo_codigo
    )
    definir_coluna_periodo_id(df_tratado=df_tratado, periodo_id=periodo_id)
    definir_coluna_unidade_geografica_id(df_tratado=df_tratado, sessao=sessao)

    logger.info("Garantindo tipagem dos dados...")
    garantir_tipos_colunas(df_tratado=df_tratado)

    df_tratado.reset_index(drop=True, inplace=True)
    return df_tratado
