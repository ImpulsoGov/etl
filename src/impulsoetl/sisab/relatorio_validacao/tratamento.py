# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Processa dados do relatório de validação por produção para o formato usado no BD."""

from __future__ import annotations
from typing import Final
from frozendict import frozendict
from datetime import date
import pandas as pd
import numpy as np
from sqlalchemy.orm import Session
from impulsoetl.comum.datas import periodo_por_codigo,periodo_por_data
from impulsoetl.comum.geografias import id_sus_para_id_impulso
from impulsoetl.loggers import logger

TIPOS: Final[frozendict] = frozendict(
    {
        "municipio_id_sus": str,
        "cnes_id": str,
        "equipe_ine_id":str,
        "validacao_nome": str,
        "validacao_quantidade": int,
        "periodo_id": str,
        "periodo_codigo": str,
        "unidade_geografica_id": str,
        "no_prazo": bool
    })

VALIDACAO_RENOMEIA_COLUNAS : Final[dict[str, str]] = {
    "IBGE":"municipio_id_sus",
    "CNES":"cnes_id",
    "INE":"equipe_ine_id",
    "Validação":"validacao_nome",
    "Total":"validacao_quantidade"
}
    
def tratamento_dados(
    sessao:Session,
    df_extraido:str,
    periodo_competencia:date,
    envio_prazo:bool,
    )->pd.DataFrame:
    """ Trata dados capturados do relatório de indicadores do SISAB

        Argumentos:
            sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
                acessar a base de dados da ImpulsoGov.
            df_extraido: [`DataFrame`][] contendo os dados capturados no relatório de Indicadores do Sisab
             (conforme retornado pela função
                [`extrair_dados()`][]).
            periodo_competencia: Data do quadrimestre da competência em referência
            envio_prazo: Indica se os relatórios de validação a serem considerados
            apenas os enviados no prazo (`True`) ou se devem considerar tanto
            envios no prazo quanto fora do prazo (`False`)

        Retorna:
            Objeto [`pandas.DataFrame`] com os dados enriquecidos e tratados.

                [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
                [`pandas.DataFrame`]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
                """


    logger.info("Iniciando tratamento dos dados...")
    logger.info("Renomeando colunas...")
    df_tratado = df_extraido.rename(columns=VALIDACAO_RENOMEIA_COLUNAS)
    logger.info("Enriquecendo tabela com campos complementares...")
    df_tratado['no_prazo'] = envio_prazo
    df_tratado["periodo_codigo"] = periodo_por_data(sessao=sessao, data=periodo_competencia,tipo_periodo="mensal").codigo
    df_tratado["periodo_id"] = periodo_por_codigo(sessao=sessao, codigo=periodo_por_data(sessao=sessao, data=periodo_competencia,tipo_periodo="mensal").codigo).id
    df_tratado.reset_index(drop=True, inplace=True)

    df_tratado["equipe_ine_id"] = (
        df_tratado["equipe_ine_id"]
        .fillna("0")
        .astype("int64")
        .astype("string")
        .str.zfill(10)
        .replace("0000000000", np.nan)
    )
    df_tratado["cnes_id"] = df_tratado["cnes_id"].astype(int).astype("string").str.zfill(7)
    df_tratado["unidade_geografica_id"] = df_tratado[
        "municipio_id_sus"
    ].apply(
        lambda municipio_id_sus: id_sus_para_id_impulso(
            sessao=sessao,
            id_sus=municipio_id_sus,
        )
    )
    logger.info("Garantindo tipagem dos dados...")
    df_tratado = df_tratado.astype(TIPOS)
    logger.info(f"Tratamento dos dados realizado | Total de registros : {df_tratado.shape[0]}")
    return df_tratado
