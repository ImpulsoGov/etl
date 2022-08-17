# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Junta etapas do fluxo de ETL de validação por produção dos municípios."""

from __future__ import annotations
from typing import Final
from sqlalchemy.orm import Session
from datetime import date
from impulsoetl.sisab.relatorio_validacao.extracao import (
    extrair_dados,
    )
from impulsoetl.sisab.relatorio_validacao.tratamento import (
    tratamento_dados,
    )
from impulsoetl.sisab.relatorio_validacao.teste_validacao import (
    teste_validacao,
    )
from impulsoetl.sisab.relatorio_validacao.carregamento import (
    carregar_dados,
    )

ENVIO_PRAZO = [True,False]

def obter_validacao_por_producao(
    sessao: Session,
    periodo_competencia: date,
    ) -> None:

        """ Extrai, transforma e carrega os dados do relatório de validação [por produção] do SISAB.
            Argumentos:
                sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite acessar a base de dados da ImpulsoGov.
                periodo_competencia: Data do mês em referência.
        """
        for envio_prazo in ENVIO_PRAZO:
            df_extraido = extrair_dados(
                periodo_competencia=periodo_competencia,
                envio_prazo=envio_prazo,
                )
            df_tratado = tratamento_dados(
                sessao=sessao,
                df_extraido=df_extraido,
                periodo_competencia=periodo_competencia,
                envio_prazo=envio_prazo,
                )
            teste_validacao(
                df_extraido=df_extraido,
                df_tratado=df_tratado,
                )
            carregar_dados(
                sessao=sessao,
                df_tratado=df_tratado,
                )
