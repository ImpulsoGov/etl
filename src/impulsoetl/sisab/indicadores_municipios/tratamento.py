# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Processa dados de indicadores de desempenho para o formato usado no BD."""

from datetime import date
from typing import Final

import pandas as pd
from frozendict import frozendict
from prefect import task
from sqlalchemy.orm import Session

from impulsoetl.comum.datas import periodo_por_codigo, periodo_por_data
from impulsoetl.comum.geografias import id_sus_para_id_impulso
from impulsoetl.loggers import habilitar_suporte_loguru, logger
from impulsoetl.sisab.indicadores_municipios.modelos import indicadores_regras

TIPOS: Final[frozendict] = frozendict(
    {
        "municipio_id_sus": str,
        "numerador": int,
        "denominador_utilizado": int,
        "denominador_estimado": int,
        "denominador_informado": int,
        "nota_porcentagem": int,
        "cadastro": int,
        "base_externa": float,
        "porcentagem": int,
        "populacao": int,
    }
)

INDICADORES_RENOMEIA_COLUNAS: Final[dict[str, str]] = {
    "IBGE": "municipio_id_sus",
    "Numerador": "numerador",
    "Denominador Utilizado": "denominador_utilizado",
    "2022 Q1 (%)": "nota_porcentagem",
    "Denominador Identificado": "denominador_informado",
    "Denominador Estimado": "denominador_estimado",
    "Cadastro": "cadastro",
    "Base Externa": "base_externa",
    "Percentual": "porcentagem",
    "População": "populacao",
}


def indicadores_regras_id_por_periodo(  # noqa: WPS122 - permite argumento data
    sessao: Session,
    indicador: str,
    data: date,
):
    return (
        sessao.query(indicadores_regras)  # type: ignore
        .filter(indicadores_regras.c.nome == indicador)
        .filter(indicadores_regras.c.versao_inicio <= data)
        .filter(indicadores_regras.c.versao_fim == None)  # noqa: E711
        .first()
        .id
    )


@task(
    name="Transformar Indicadores do Previne Brasil",
    description=(
        "Transforma os dados dos relatórios de indicadores do Previne Brasil "
        + "extraídos do portal público do Sistema de Informação em Saúde para "
        + "a Atenção Básica do SUS."
    ),
    tags=["aps", "sisab", "indicadores_municipios", "transformacao"],
    retries=0,
    retry_delay_seconds=None,
)
def transformar_indicadores(
    sessao: Session,
    df_extraido: pd.DataFrame,
    periodo: date,
    indicador: str,
) -> pd.DataFrame:
    """Trata dados capturados do relatório de indicadores do SISAB.

    Realiza as seguintes operações nos dados baixados do SISAB:

        * Exclui campos não utilizados;
        * Renomeia colunas;
        * Enriquece tabela com novos campos;
        * Exclui índice;
        * Define tipos de dados.

    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov.
        df_extraido: [`pandas.DataFrame`][] contendo os dados capturados no relatório
            de Indicadores do Sisab (conforme retornado pela função
            [`extrair_dados()`][]).
        periodo: Data do quadrimestre da competência em referência
        indicador: Nome do indicador.

    Retorna:
        Um objeto [`pandas.DataFrame`][] com dados tratados para armazenamento
        em tabela do banco de dados da Impulso Gov.

    [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
    [`extrair_dados()`]: impulsoetl.sisab.indicadores_municipios.extracao.extrair_dados
    [`pandas.DataFrame`]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    """
    habilitar_suporte_loguru()
    logger.info("Iniciando tratamento dos dados...")
    df_tratado = df_extraido.rename(columns=INDICADORES_RENOMEIA_COLUNAS)
    df_tratado["indicadores_nome"] = indicador
    df_tratado["indicadores_regras_id"] = indicadores_regras_id_por_periodo(
        sessao=sessao, indicador=indicador, data=periodo
    )
    df_tratado["periodo_codigo"] = periodo_por_data(
        sessao=sessao, data=periodo, tipo_periodo="quadrimestral"
    ).codigo
    df_tratado["periodo_id"] = periodo_por_codigo(
        sessao=sessao,
        codigo=periodo_por_data(
            sessao=sessao, data=periodo, tipo_periodo="quadrimestral"
        ).codigo,
    ).id
    df_tratado.reset_index(drop=True, inplace=True)
    df_tratado["municipio_id_sus"] = (
        df_tratado["municipio_id_sus"].astype(int).astype("string")
    )
    df_tratado["unidade_geografica_id"] = df_tratado["municipio_id_sus"].apply(
        lambda municipio_id_sus: id_sus_para_id_impulso(
            sessao=sessao,
            id_sus=municipio_id_sus,
        )
    )
    df_tratado = df_tratado.astype(TIPOS)
    logger.info(
        f"Tratamento dos dados realizado | Total de registros : {df_tratado.shape[0]}"
    )
    return df_tratado
