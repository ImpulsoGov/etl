# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT

""" Realiza o tratamento das informações extraídas para os estabelecimentos de saúde para o formato usado no bando de dados da Impulso"""

import warnings

warnings.filterwarnings("ignore")


from datetime import date
from typing import Final

import numpy as np
import pandas as pd
from frozendict import frozendict
from prefect import task
from sqlalchemy.orm import Session

from impulsoetl.bd import Sessao
from impulsoetl.cnes.estabelecimentos_identificados.extracao import (
    extrair_informacoes_estabelecimentos,
)
from impulsoetl.cnes.extracao_lista_cnes import extrair_lista_cnes
from impulsoetl.comum.geografias import id_sus_para_id_impulso
from impulsoetl.loggers import habilitar_suporte_loguru, logger

COLUNAS_RENOMEAR: Final[dict[str, str]] = {
    "municipio": "municipio_id_sus",
    "cnes": "estabelecimento_cnes_id",
    "noFantasia": "estabelecimento_nome",
    "noEmpresarial": "estabelecimento_nome_empresarial",
    "natJuridica": "estabelecimento_natureza_juridica",
    "cnpj": "estabelecimento_cnpj",
    "noLogradouro": "estabelecimento_logradouro",
    "nuEndereco": "estabelecimento_logradouro_numero",
    "bairro": "estabelecimento_bairro",
    "cep": "estabelecimento_cep",
    "regionalSaude": "estabelecimento_regional_saude",
    "dsTpUnidade": "estabelecimento_tipo",
    "dsStpUnidade": "estabelecimento_subtipo",
    "nvDependencia": "estabelecimento_dependencia",
    "tpGestao": "estabelecimento_gestao_tipo",
    "nuTelefone": "estabelecimento_telefone",
    "tpSempreAberto": "sempre_aberto",
    "dtCarga": "estabelecimento_data_cadastro",
    "coMotivoDesab": "codigo_motivo_desativacao",
    "dsMotivoDesab": "descricao_motivo_desativacao",
    "dtAtualizacaoOrigem": "estabelecimento_data_atualizacao_base_local",
    "dtAtualizacao": "estabelecimento_data_atualizacao_base_nacional",
}

COLUNAS_EXCLUIR = [
    "id",
    "natJuridicaMant",
    "tpPessoa",
    "nuAlvara",
    "dtExpAlvara",
    "orgExpAlvara",
    "uf",
    "noComplemento",
    "noMunicipio",
    "cpfDiretorCln",
    "stContratoFormalizado",
    "nuCompDesab",
]

ESTABELECIMENTO_NATUREZA_JURIDICA: Final[dict[str, str]] = {
    "1": "ADMINISTRAÇÃO PÚBLICA",
    "2": "ENTIDADES EMPRESARIAIS",
    "3": "ENTIDADES SEM FINS LUCRATIVOS",
    "4": "PESSOAS FÍSICAS",
}

ESTABELECIMENTO_GESTAO_TIPO: Final[dict[str, str]] = {
    "D": "DUPLA",
    "E": "ESTADUAL",
    "M": "MUNICIPAL",
}

ESTABELECIMENTO_DEPENDENCIA: Final[dict[str, str]] = {
    "1": "INDIVIDUAL",
    "3": "MANTIDA",
}

COLUNAS_TIPOS: Final[frozendict] = frozendict(
    {
        "estabelecimento_cnes_id": "str",
        "estabelecimento_nome": "str",
        "estabelecimento_nome_empresarial": "str",
        "estabelecimento_natureza_juridica": "str",
        "estabelecimento_cnpj": "str",
        "estabelecimento_dependencia": "str",
        "estabelecimento_tipo": "str",
        "estabelecimento_subtipo": "str",
        "estabelecimento_logradouro": "str",
        "estabelecimento_logradouro_numero": "str",
        "estabelecimento_cep": "str",
        "estabelecimento_regional_saude": "str",
        "estabelecimento_bairro": "str",
        "municipio_id_sus": "str",
        "estabelecimento_gestao_tipo": "str",
        "estabelecimento_telefone": "str",
        "sempre_aberto": "boolean",
        "codigo_motivo_desativacao": "str",
        "descricao_motivo_desativacao": "str",
        "estabelecimento_data_cadastro": "str",
        "estabelecimento_data_atualizacao_base_local": "str",
        "estabelecimento_data_atualizacao_base_nacional": "str",
        "status_estabelecimento": "str",
    }
)

COLUNAS_DATA = [
    "estabelecimento_data_cadastro",
    "estabelecimento_data_atualizacao_base_local",
    "estabelecimento_data_atualizacao_base_nacional",
]


def status_estabelecimento(df_extraido: pd.DataFrame) -> pd.DataFrame:
    df_extraido["status_estabelecimento"] = np.where(
        df_extraido["codigo_motivo_desativacao"].isnull(),
        "ATIVO",
        "DESATIVADO",
    )
    df_extraido.loc[
        df_extraido["estabelecimento_nome"] == "NAO IDENTIFICADO",
        "status_estabelecimento",
    ] = "NAO IDENTIFICADO"
    return df_extraido


def renomear_colunas(df_extraido: pd.DataFrame) -> pd.DataFrame:
    df_extraido.rename(columns=COLUNAS_RENOMEAR, inplace=True)
    return df_extraido


def excluir_colunas(df_extraido: pd.DataFrame) -> pd.DataFrame:
    df_extraido.drop(columns=COLUNAS_EXCLUIR, inplace=True)
    return df_extraido


def tratar_valores_codificados(df_extraido: pd.DataFrame) -> pd.DataFrame:
    df_extraido["sempre_aberto"] = df_extraido["sempre_aberto"].map(
        {"S": True, "N": False}
    )
    df_extraido["estabelecimento_natureza_juridica"] = df_extraido[
        "estabelecimento_natureza_juridica"
    ].map(ESTABELECIMENTO_NATUREZA_JURIDICA)
    df_extraido["estabelecimento_gestao_tipo"] = df_extraido[
        "estabelecimento_gestao_tipo"
    ].map(ESTABELECIMENTO_GESTAO_TIPO)
    df_extraido["estabelecimento_dependencia"] = df_extraido[
        "estabelecimento_dependencia"
    ].map(ESTABELECIMENTO_DEPENDENCIA)
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


@task(
    name="Tratar dados dos Estabelecimentos Identificados",
    description=(
        "Realiza o tratamento dos dados extraídos dos estabelecimentos de saúde "
        + "a partir da página do CNES"
    ),
    tags=["cnes", "estabelecimentos", "tratamento"],
    retries=0,
    retry_delay_seconds=None,
)
def tratamento_dados(
    df_extraido: pd.DataFrame, periodo_id: str, unidade_geografica_id: str
) -> pd.DataFrame:
    """
    Trata os dados extraídos para os estabelecimentos de saúde a partir da página do CNES

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

    df_extraido = renomear_colunas(df_extraido)
    df_extraido = excluir_colunas(df_extraido)
    df_extraido = status_estabelecimento(df_extraido)
    df_extraido = tratar_valores_codificados(df_extraido)
    df_extraido = tratar_tipos(df_extraido)
    df_extraido["periodo_id"] = periodo_id
    df_extraido["unidade_geografica_id"] = unidade_geografica_id
    df_extraido = df_extraido.reset_index(drop=True)

    logger.info("Dados transformados ...")

    return df_extraido
