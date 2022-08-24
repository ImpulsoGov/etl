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

from impulsoetl.loggers import logger
from impulsoetl.comum.geografias import id_sus_para_id_impulso

VALIDACAO_TIPOS: Final[frozendict] = frozendict(
    {
        "municipio_id_sus": str,
        "periodo_id": str,
        "cnes_id": str,
        "cnes_tipo": str,
        "ine_id":str,
        "ine_tipo":str,
        "ficha": str,
        "aplicacao": str,
        "validacao_nome": str,
        "validacao_quantidade": int,
        "periodo_codigo": str,
        "no_prazo": bool,
        "unidade_geografica_id": str
    }
    )

VALIDACAO_COLUNAS : Final[dict[str, str]] = {
    "IBGE":"municipio_id_sus",
    "CNES":"cnes_id",
    "INE":"ine_id",
    "Validação":"validacao_nome",
    "Tipo Equipe":"ine_tipo",
    "Tipo Unidade":"cnes_tipo",
    "Total":"validacao_quantidade"
}
    

def tratamento_dados(
    sessao:Session,
    df_extraido:str,
    periodo_id:str,
    periodo_codigo:str,
    envio_prazo:bool,
    ficha:str,
    aplicacao:str,
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
            envios no prazo quanto fora do prazo (`False`).
            ficha: Nome da ficha requisitada
            aplicacao: Nome da aplicacao requisitada

        Retorna:
            Objeto [`pandas.DataFrame`] com os dados enriquecidos e tratados.

                [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
                [`pandas.DataFrame`]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
                """


    logger.info("Iniciando tratamento dos dados...")
    logger.info("Renomeando colunas...")
    df_tratado = df_extraido.rename(columns=VALIDACAO_COLUNAS)
    logger.info("Enriquecendo tabela com campos complementares...")
    df_tratado['no_prazo'] = envio_prazo
    df_tratado['ficha'] = ficha
    df_tratado['aplicacao'] = aplicacao
    df_tratado["periodo_codigo"] = periodo_codigo
    df_tratado["periodo_id"] = periodo_id
    df_tratado["unidade_geografica_id"] = df_tratado["municipio_id_sus"].apply(
        lambda municipio_id_sus: id_sus_para_id_impulso(
            sessao=sessao,
            id_sus=municipio_id_sus,
        )
    )
    
    df_tratado.reset_index(drop=True, inplace=True)
    df_tratado[["cnes_tipo", "ine_tipo"]] = (
        df_tratado[["cnes_tipo", "ine_tipo"]]
        .fillna("0")
        .replace("0", np.nan)
    )
    df_tratado["cnes_id"] = df_tratado["cnes_id"].str.zfill(7)
    df_tratado["ine_id"] = df_tratado["ine_id"].fillna("0").str.zfill(10).replace("0000000000", np.nan)
    
    validacao_quantidade = []
    for string in df_tratado['validacao_quantidade']:
        string=string.replace('.','')
        validacao_quantidade.append(string)
    df_tratado['validacao_quantidade'] = validacao_quantidade

    logger.info("Garantindo tipagem dos dados...")
    df_tratado = df_tratado.astype(VALIDACAO_TIPOS)
    logger.info(f"Tratamento dos dados realizado | Total de registros : {df_tratado.shape[0]}")
    return df_tratado
