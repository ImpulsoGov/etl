# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


from __future__ import annotations

from sqlalchemy.orm import Session
<<<<<<< HEAD
from datetime import date
=======

>>>>>>> f19009cf7eaf7e0e5cf4c3b1035b8d06a05c0a6d
from impulsoetl.sisab.parametros_cadastro.carregamento import (
    carregar_parametros,
)
from impulsoetl.sisab.parametros_cadastro.extracao import extrair_parametros
from impulsoetl.sisab.parametros_cadastro.teste_validacao import (
    teste_validacao,
)
<<<<<<< HEAD
from sisab.parametros_cadastro.tratamento import tratamento_dados
from impulsoetl.bd import Sessao
=======
from impulsoetl.sisab.parametros_cadastro.tratamento import tratamento_dados
from impulsoetl.tipos import DatetimeLike
>>>>>>> f19009cf7eaf7e0e5cf4c3b1035b8d06a05c0a6d

def obter_parametros(
    sessao: Session,
    visao_equipe: str,
    periodo: date,
    nivel_agregacao: str,
<<<<<<< HEAD
    teste: bool = True
=======
    teste: bool = False,
>>>>>>> f19009cf7eaf7e0e5cf4c3b1035b8d06a05c0a6d
) -> None:
    """Extrai, transforma e carrega dados de parâmetros cadastros de equipes pelo SISAB.
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
            SQLAlchemy."""

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
    teste_validacao(df, df_tratado, nivel_agregacao=nivel_agregacao)
    carregar_parametros(
        sessao=sessao,
        parametros_transformada=df_tratado,
        visao_equipe=visao_equipe,
        nivel_agregacao=nivel_agregacao,
    )
    if not teste:
        sessao.commit()
<<<<<<< HEAD


=======
>>>>>>> f19009cf7eaf7e0e5cf4c3b1035b8d06a05c0a6d
