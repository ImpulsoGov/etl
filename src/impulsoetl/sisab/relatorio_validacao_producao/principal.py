# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Junta etapas do fluxo de ETL de validação por ficha por aplicação dos municípios."""

from datetime import date
from typing import Final

import pandas as pd
import traceback
from prefect import flow
from sqlalchemy.orm import Session

from impulsoetl import __VERSION__
from impulsoetl.loggers import habilitar_suporte_loguru, logger
from impulsoetl.sisab.excecoes import SisabExcecao
from impulsoetl.sisab.relatorio_validacao_producao.carregamento import (
    carregar_dados,
)
from impulsoetl.sisab.relatorio_validacao_producao.extracao import (
    extrair_dados,
)
from impulsoetl.sisab.relatorio_validacao_producao.tratamento import (
    tratamento_dados,
)
from impulsoetl.sisab.relatorio_validacao_producao.verificacao import (
    verificar_relatorio_validacao_producao,
)

FICHA_CODIGOS: Final[dict[str, str]] = {
    "Cadastro Individual": "2",
    "Atendimento Individual": "4",
    "Procedimentos": "7",
    "Visita Domiciliar": "8",
}

APLICACAO_CODIGOS: Final[dict[str, str]] = {
    "CDS-offline": "0",
    "CDS-online": "1",
    "PEC": "2",
    "Sistema proprio": "3",
    "Android ACS": "4",
}
ENVIO_PRAZO = [False, True]


@flow(
    name="Obter Relatórios de Validação da Produção",
    description=(
        "Extrai, transforma e carrega os dados dos relatórios de validação da "
        + "produção do portal público do Sistema de Informação em Saúde para "
        + "a Atenção Básica do SUS."
    ),
    retries=0,
    retry_delay_seconds=None,
    version=__VERSION__,
    validate_parameters=False,
)
def obter_validacao_producao(
    sessao: Session,
    periodo_competencia: date,
    periodo_id: str,
    periodo_codigo: str,
    tabela_destino: str,
    operacao_id: str,
) -> None:

    """Extrai, transforma e carrega os dados do relatório de validação [por produção] do SISAB.
    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite acessar a base de dados da ImpulsoGov.
        periodo_competencia: Data do mês em referência.
    """
    habilitar_suporte_loguru()
    for envio_prazo in ENVIO_PRAZO:
        df_consolidado = pd.DataFrame()
        for ficha in FICHA_CODIGOS:
            for aplicacao in APLICACAO_CODIGOS:
                try: 
                    if (
                        (ficha == "Cadastro Individual" and aplicacao == "PEC")
                        or (ficha == "Visita Domiciliar" and aplicacao == "PEC")
                        or (
                            ficha == "Atendimento Individual"
                            and aplicacao == "Android ACS"
                        )
                    ):
                        continue
                    df_extraido = extrair_dados(
                        periodo_competencia=periodo_competencia,
                        envio_prazo=envio_prazo,
                        ficha=ficha,
                        aplicacao=aplicacao,
                    )
                    df_tratado = tratamento_dados(
                        sessao=sessao,
                        df_extraido=df_extraido,
                        periodo_id=periodo_id,
                        periodo_codigo=periodo_codigo,
                        envio_prazo=envio_prazo,
                        ficha=ficha,
                        aplicacao=aplicacao,
                    )
                    df_consolidado = pd.concat(
                        [df_consolidado, df_tratado], ignore_index=True
                    )

                    logger.info(
                        "Captura realizada e tratada para a ficha de `{ficha}`"
                        + "com aplicação '{aplicacao}' no período '{periodo_codigo}'"
                        + "e envio no prazo = {envio_prazo}",
                        ficha=ficha,
                        aplicacao=aplicacao,
                        periodo_codigo=periodo_codigo,
                        envio_prazo=envio_prazo,
                    )
                except pd.errors.ParserError:
                    traceback_str = traceback.format_exc()
                    enviar_erro = SisabExcecao("Competência indisponível no SISAB")
                    enviar_erro.insere_erro_database(sessao=sessao,traceback_str=traceback_str,operacao_id=operacao_id,periodo_id=periodo_id)

                    logger.error("Data da competência do relatório não está disponível")
                    break
            
                except Exception as mensagem_erro:
                    traceback_str = traceback.format_exc()
                    enviar_erro = SisabExcecao(mensagem_erro)
                    enviar_erro.insere_erro_database(sessao=sessao,traceback_str=traceback_str,operacao_id=operacao_id,periodo_id=periodo_id)

                    logger.error(mensagem_erro)
                    break

        carregar_dados(
            sessao=sessao,
            df_tratado=df_consolidado,
            tabela_destino=tabela_destino,
            no_prazo=envio_prazo,
            periodo_id=periodo_id,
        )