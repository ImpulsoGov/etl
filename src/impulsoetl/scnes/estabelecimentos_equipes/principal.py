# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""ETL das equipes dos estabelecimentos de saúde do CNES por município."""

import warnings

warnings.filterwarnings("ignore")
from sqlalchemy.orm import Session
from prefect import flow

from impulsoetl import __VERSION__
from impulsoetl.bd import Sessao
from impulsoetl.loggers import logger

from impulsoetl.scnes.carregamento_etls_scnes import (
    carregar_dados,
)
from impulsoetl.scnes.estabelecimentos_equipes.extracao import (
    extrair_equipes
)
from impulsoetl.scnes.estabelecimentos_equipes.tratamento import (
    tratamento_dados,
)
from impulsoetl.scnes.verificacao_etls_scnes import (
    verificar_dados,
)
from impulsoetl.scnes.extracao_lista_cnes import extrair_lista_cnes

@flow(
    name="Obter dados da Ficha de Equipes de Saúde por Estabelecimento",
    description=(
        "Extrai, transforma e carrega os dados dos equipes de saúde "
        + "a partir da página do CNES"
    ),
    retries=0,
    retry_delay_seconds=None,
    version=__VERSION__,
    validate_parameters=False,
)
def obter_equipes_cnes(
    sessao: Session,
    tabela_destino: str,
    codigo_municipio: str,
    periodo_id: str,
    unidade_geografica_id: str,
) -> None:
    """
    Extrai, transforma e carrega os dados das equipes dos estabelecimentos de saúde identificados no CNES
     Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite acessar a base de dados da ImpulsoGov.
        tabela_destino: Nome da tabela de destino a ser carregada com os dados extraidos e tratados.
        codigo_municipio: Id sus do municipio.
        periodo_id: Código de identificação do período .
        unidade_geografica_id: Código de identificação da unidade geográfica.
    """
   
    lista_cnes = extrair_lista_cnes(codigo_municipio=codigo_municipio)

    df_extraido = extrair_equipes(
        codigo_municipio=codigo_municipio, lista_cnes=lista_cnes
    )

    df_tratado = tratamento_dados(
        df_extraido=df_extraido,
        periodo_id=periodo_id,
        unidade_geografica_id=unidade_geografica_id,
    )

    verificar_dados(
        df_extraido=df_extraido, df_tratado=df_tratado
    )
    
    carregar_dados(
        sessao=sessao, df_tratado=df_tratado, tabela_destino=tabela_destino
    )
   

    return df_tratado