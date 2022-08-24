# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Junta etapas do fluxo de ETL de validação por ficha por aplicação dos municípios."""

from __future__ import annotations

from typing import Final
from datetime import date

from sqlalchemy.orm import Session

from impulsoetl.sisab.relatorio_validacao_producao.extracao import (
    extrair_dados,
    )
from impulsoetl.sisab.relatorio_validacao_producao.tratamento import (
    tratamento_dados,
    )
from impulsoetl.sisab.relatorio_validacao_producao.teste_validacao import (
    teste_validacao,
    )
from impulsoetl.sisab.relatorio_validacao_producao.carregamento import (
    carregar_dados,
    )

FICHA_CODIGOS : Final[dict[str, str]] = {
    "Cadastro Individual":"2",
    "Atendimento Individual":"4",
    "Procedimentos":"7",
    "Visita Domiciliar":"8",
    }

APLICACAO_CODIGOS : Final[dict[str, str]] = {
    "CDS-offline": "0",
    "CDS-online": "1",
    "PEC": "2",
    "Sistema proprio": "3",
    "Android ACS": "4",
    }
ENVIO_PRAZO = [True,False]

def obter_validacao_producao(
    sessao: Session,
    periodo_competencia: date,
    periodo_id:str,
    periodo_codigo:str,
    tabela_destino:str,
) -> None:

        """ Extrai, transforma e carrega os dados do relatório de validação [por produção] do SISAB.
            Argumentos:
                sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite acessar a base de dados da ImpulsoGov.
                periodo_competencia: Data do mês em referência.
        """ 
        for envio_prazo in ENVIO_PRAZO:
            for ficha in FICHA_CODIGOS: 
                for aplicacao in APLICACAO_CODIGOS:
                    if (
                        (
                            ficha == "Cadastro Individual" and aplicacao == "PEC"
                            ) or 
                            (
                                ficha == "Visita Domiciliar" and aplicacao == "PEC"
                                ) or (ficha == "Atendimento Individual" and aplicacao == "Android ACS")):
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
                    teste_validacao(df_extraido=df_extraido,df_tratado=df_tratado)
                    carregar_dados(
                        sessao = sessao, 
                        df_tratado = df_tratado,
                        tabela_destino = tabela_destino,
                        periodo_id = periodo_id,
                        no_prazo = envio_prazo,
                        ficha_tipo = ficha,
                        aplicacao_tipo = aplicacao)
