from __future__ import annotations
from sqlalchemy.orm import Session
from datetime import date
from impulsoetl.sisab.cadastros_individuais.carregamento import (
    carregar_cadastros,
)
from impulsoetl.sisab.cadastros_individuais.extracao import (
    extrair_cadastros_individuais,
)
from impulsoetl.sisab.cadastros_individuais.teste_validacao import (
    teste_validacao,
)
from impulsoetl.sisab.cadastros_individuais.tratamento import tratamento_dados


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
            SQLAlchemy."""

    for status_ponderacao in com_ponderacao:
        df = extrair_cadastros_individuais(
            visao_equipe=visao_equipe,
            com_ponderacao=status_ponderacao,
            competencia=periodo,
        )
        df_tratado = tratamento_dados(
            sessao=sessao,
            dados_sisab_cadastros=df,
            com_ponderacao=status_ponderacao,
            periodo=periodo,
        )
        teste_validacao(df, df_tratado)
        carregar_cadastros(
            sessao=sessao,
            cadastros_transformada=df_tratado,
            visao_equipe=visao_equipe,
        )
        if not teste:
            sessao.commit()