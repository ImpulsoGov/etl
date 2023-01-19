# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""ETL dos profissionais dos estabelecimentos de saúde do CNES por município."""

import warnings

warnings.filterwarnings("ignore")
#from prefect import task
from sqlalchemy.orm import Session

from impulsoetl import __VERSION__
from impulsoetl.bd import Sessao
from impulsoetl.scnes.carregamento_etls_scnes import (
    carregar_dados,
)

from impulsoetl.scnes.carregamento_etls_scnes import (
    carregar_dados,
)
from impulsoetl.scnes.estabelecimentos_profissionais.extracao import (
    extrair_profissionais,
)
from impulsoetl.scnes.estabelecimentos_profissionais.tratamento import (
    tratamento_dados,
)
from impulsoetl.scnes.estabelecimentos_profissionais.verificacao import (
    verificar_dados,
)
from impulsoetl.scnes.extracao_lista_cnes import extrair_lista_cnes
#from impulsoetl.loggers import habilitar_suporte_loguru, logger


def obter_profissionais_cnes(
    sessao: Session,
    tabela_destino: str,
    codigo_municipio: str,
    periodo_id: str,
    unidade_geografica_id: str,
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

    df_extraido = extrair_profissionais(
        codigo_municipio=codigo_municipio, lista_codigos=lista_cnes
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

    return df_extraido