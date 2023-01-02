# SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


from datetime import date

from prefect import flow
from sqlalchemy.orm import Session

from impulsoetl import __VERSION__
from impulsoetl.loggers import habilitar_suporte_loguru
from impulsoetl.sisab.parametros_cadastro.tratamento import tratamento_dados
from impulsoetl.sisab.parametros_cadastro.carregamento import (
    carregar_parametros,
)
from impulsoetl.sisab.parametros_cadastro.extracao import extrair_parametros
from impulsoetl.sisab.parametros_cadastro.verificacao import (
    verificar_parametros_cadastro,
)


@flow(
    name="Obter Parâmetros de Cadastro",
    description=(
        "Extrai, transforma e carrega os dados de parâmetros de cadastro "
        + "do portal público do Sistema de Informação em Saúde para a Atenção "
        + "Básica do SUS."
    ),
    retries=0,
    retry_delay_seconds=None,
    version=__VERSION__,
    validate_parameters=False,
)
def obter_parametros(
    sessao: Session,
    visao_equipe: str,
    periodo: date,
    nivel_agregacao: str,
    teste: bool = True,
) -> None:
    """Extrai, transforma e carrega dados de parâmetros cadastros de equipes.

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

    df = extrair_parametros(
        visao_equipe=visao_equipe,
        competencia=periodo,
        nivel_agregacao=nivel_agregacao,
    )
    df_tratado = tratamento_dados(
        sessao=sessao,
        dados_sisab_cadastros=df,
        periodo=periodo,
        nivel_agregacao=nivel_agregacao,
    )
    verificar_parametros_cadastro(
        df=df,
        df_tratado=df_tratado,
        nivel_agregacao=nivel_agregacao,
    )
    carregar_parametros(
        sessao=sessao,
        parametros_transformada=df_tratado,
        visao_equipe=visao_equipe,
        nivel_agregacao=nivel_agregacao,
    )
    if not teste:
        sessao.commit()
