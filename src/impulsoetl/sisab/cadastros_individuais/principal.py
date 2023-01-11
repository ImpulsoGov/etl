# SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


from datetime import date

from prefect import flow
from sqlalchemy.orm import Session

from impulsoetl import __VERSION__
from impulsoetl.loggers import habilitar_suporte_loguru, logger
from impulsoetl.sisab.cadastros_individuais.carregamento import (
    carregar_cadastros,
)
from impulsoetl.sisab.cadastros_individuais.extracao import (
    extrair_cadastros_individuais,
)
from impulsoetl.sisab.cadastros_individuais.tratamento import tratamento_dados
from impulsoetl.sisab.cadastros_individuais.verificacao import (
    verificar_cadastros_individuais,
)


@flow(
    name="Obter Cadastros Individuais",
    description=(
        "Extrai, transforma e carrega os dados de cadastros individuais "
        + "do portal público do Sistema de Informação em Saúde para a Atenção "
        + "Básica do SUS."
    ),
    retries=0,
    retry_delay_seconds=None,
    version=__VERSION__,
    validate_parameters=False,
)
def obter_cadastros_individuais(
    sessao: Session,
    visao_equipe: str,
    periodo: date,
    com_ponderacao: list[bool] = [True, False],
    teste: bool = True,
) -> None:
    """Extrai, transforma e carrega dados de cadastros de equipes pelo SISAB.

    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov.
        visao_equipe: Indica a situação da equipe considerada para a contagem
            dos cadastros.
        periodo: Referente ao mês/ano de disponibilização do relatório.
        com_ponderacao: Lista de booleanos indicando quais tipos de população
            devem ser filtradas no cadastro - onde `True` indica apenas as
            populações com critério de ponderação e `False` indica todos os
            cadastros. Por padrão, o valor é `[True, False]`, indicando que
            ambas as possibilidades são extraídas do SISAB e carregadas para a
            mesma tabela de destino.
        teste: Indica se as modificações devem ser de fato escritas no banco de
            dados (`False`, padrão). Caso seja `True`, as modificações são
            adicionadas à uma transação, e podem ser revertidas com uma chamada
            posterior ao método [`Session.rollback()`][] da sessão gerada com o
            SQLAlchemy.
    """
    habilitar_suporte_loguru()

    for status_ponderacao in com_ponderacao:
        df = extrair_cadastros_individuais(
            visao_equipe=visao_equipe,
            com_ponderacao=status_ponderacao,
            competencia=periodo,
        )
        logger.info("Extração dos dados realizada...")
        df_tratado = tratamento_dados(
            sessao=sessao,
            dados_sisab_cadastros=df,
            com_ponderacao=status_ponderacao,
            periodo=periodo,
        )
        verificar_cadastros_individuais(df=df, df_tratado=df_tratado)
        carregar_cadastros(
            sessao=sessao,
            cadastros_transformada=df_tratado,
            visao_equipe=visao_equipe,
        )
        if not teste:
            sessao.commit()
