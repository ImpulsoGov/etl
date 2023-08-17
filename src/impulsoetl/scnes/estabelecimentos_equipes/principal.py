# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""ETL das equipes dos estabelecimentos de saúde do CNES por município."""

import warnings
import pandas as pd

from datetime import date

warnings.filterwarnings("ignore")
from prefect import flow
from sqlalchemy.orm import Session

from impulsoetl import __VERSION__
from impulsoetl.bd import Sessao
from impulsoetl.loggers import logger
from impulsoetl.sisab.excecoes import SisabErroCompetenciaInexistente
from impulsoetl.scnes.estabelecimentos_equipes.extracao import extrair_equipes
from impulsoetl.scnes.estabelecimentos_equipes.tratamento import (
    tratamento_dados,
)
from impulsoetl.scnes.extracao_lista_cnes import extrair_lista_cnes
from impulsoetl.scnes.verificacao_etls_scnes import verificar_dados
from impulsoetl.utilitarios.bd import carregar_dataframe


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
    periodo_data_inicio: date,
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

    operacao_id = "063c6b40-ab9a-7459-b59c-6ebaa34f1bfd"

    try:

        lista_cnes = extrair_lista_cnes(codigo_municipio=codigo_municipio)

        df_extraido = extrair_equipes(
            codigo_municipio=codigo_municipio,
            lista_cnes=lista_cnes,
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

    except (KeyError, pd.errors.ParserError):
        mensagem_erro = "Data da competência do relatório não está disponível"
        traceback_str = "A extração retornou um dataframe vazio devido à indisponibilidade dos dados da competência solicitada no CNES, não sendo possível seguir com as outrsa etapas do ETL"
        enviar_erro = SisabErroCompetenciaInexistente(mensagem_erro)
        enviar_erro.insere_erro_database(sessao=sessao,traceback_str=traceback_str,operacao_id=operacao_id,periodo_id=periodo_id, unidade_geografica_id = unidade_geografica_id)

        logger.error("Data da competência do relatório não está disponível")

        return 0

