# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""ETL dos profissionais dos estabelecimentos de saúde do CNES por município."""

import warnings
from datetime import date

warnings.filterwarnings("ignore")
from prefect import flow
from sqlalchemy.orm import Session

from impulsoetl import __VERSION__
from impulsoetl.bd import Sessao
from impulsoetl.scnes.estabelecimentos_profissionais_com_ine.extracao import (
    extrair_profissionais_com_ine,
)
from impulsoetl.scnes.estabelecimentos_profissionais_com_ine.tratamento import (
    tratamento_dados,
)
from impulsoetl.scnes.estabelecimentos_profissionais_com_ine.verificacao import (
    verificar_dados,
)
from impulsoetl.scnes.extracao_lista_cnes import extrair_lista_cnes
from impulsoetl.utilitarios.bd import carregar_dataframe

from impulsoetl.loggers import habilitar_suporte_loguru, logger

@flow(
    name="Obter dados dos profissionais com INE por estabelecimento",
    description=(
        "Extrai, transforma e carrega os dados dos profissionais de saúde "
        + "a partir da página do CNES"
    ),
    retries=0,
    retry_delay_seconds=None,
    version=__VERSION__,
    validate_parameters=False,
)
def obter_profissionais_cnes_com_ine(
    sessao: Session,
    tabela_destino: str,
    codigo_municipio: str,
    periodo_id: str,
    unidade_geografica_id: str,
    periodo_data_inicio: date,
) -> None:
    """
    Extrai, transforma e carrega os dados dos profissionais dos estabelecimentos de saúde identificados no CNES
     Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite acessar a base de dados da ImpulsoGov.
        tabela_destino: Nome da tabela de destino a ser carregada com os dados extraidos e tratados.
        codigo_municipio: Id sus do municipio.
        periodo_id: Código de identificação do período .
        unidade_geografica_id: Código de identificação da unidade geográfica.
    """

    lista_cnes = extrair_lista_cnes(codigo_municipio=codigo_municipio)

    df_extraido = extrair_profissionais_com_ine(
        codigo_municipio=codigo_municipio,
        lista_codigos=lista_cnes,
        periodo_data_inicio=periodo_data_inicio,
    )

    df_tratado = tratamento_dados(
        df_extraido=df_extraido,
        periodo_id=periodo_id,
        unidade_geografica_id=unidade_geografica_id,
    )

    verificar_dados(df_extraido=df_extraido, df_tratado=df_tratado)
    carregar_dataframe(
        sessao=sessao, df=df_tratado, tabela_destino=tabela_destino
    )

    return df_extraido
