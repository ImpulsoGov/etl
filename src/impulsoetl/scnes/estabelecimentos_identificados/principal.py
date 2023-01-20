# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""ETL dos estabelecimentos de saúde identificados na página do CNES por município."""

import warnings

warnings.filterwarnings("ignore")
from datetime import date
from prefect import flow
from sqlalchemy.orm import Session

from impulsoetl import __VERSION__
from impulsoetl.scnes.estabelecimentos_identificados.carregamento import (
    carregar_dados,
)
from impulsoetl.scnes.estabelecimentos_identificados.extracao import (
    extrair_informacoes_estabelecimentos,
)
from impulsoetl.scnes.estabelecimentos_identificados.tratamento import (
    tratamento_dados,
)
from impulsoetl.scnes.estabelecimentos_identificados.verificacao import (
    verificar_informacoes_estabelecimentos_identicados,
)
from impulsoetl.scnes.extracao_lista_cnes import extrair_lista_cnes


@flow(
    name="Obter dados dos Estabelecimentos Identificados",
    description=(
        "Extrai, transforma e carrega os dados dos estabelecimentos de saúde "
        + "a partir da página do CNES"
    ),
    retries=0,
    retry_delay_seconds=None,
    version=__VERSION__,
    validate_parameters=False,
)
def obter_informacoes_estabelecimentos_identificados(
    sessao: Session,
    tabela_destino: str,
    codigo_municipio: str,
    periodo_id: str,
    unidade_geografica_id: str,
    periodo_data_inicio:date,
) -> None:
    """
    Extrai, transforma e carrega os dados dos estabelecimentos de saúde identificados na página do CNES

     Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite acessar a base de dados da ImpulsoGov.
        tabela_destino: Nome da tabela de destino a ser carregada com os dados extraidos e tratados.
        codigo_municipio: Id sus do municipio.
        periodo_id: Código de identificação do período .
        unidade_geografica_id: Código de identificação da unidade geográfica.
    """

    lista_cnes = extrair_lista_cnes(codigo_municipio=codigo_municipio)

    df_extraido = extrair_informacoes_estabelecimentos(
        codigo_municipio=codigo_municipio, lista_cnes=lista_cnes,periodo_data_inicio=periodo_data_inicio,
    )

    df_tratado = tratamento_dados(
        df_extraido=df_extraido,
        periodo_id=periodo_id,
        unidade_geografica_id=unidade_geografica_id,
    )

    verificar_informacoes_estabelecimentos_identicados(
        df_extraido=df_extraido, df_tratado=df_tratado
    )
    carregar_dados(
        sessao=sessao, df_tratado=df_tratado, tabela_destino=tabela_destino
    )

    return df_tratado
