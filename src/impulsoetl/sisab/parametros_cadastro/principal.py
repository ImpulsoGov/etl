from __future__ import annotations
from sqlalchemy.orm import Session
from datetime import date
from impulsoetl.sisab.parametros_cadastro.carregamento import (
    carregar_parametros ,
)
from impulsoetl.sisab.parametros_cadastro.extracao import (
    extrair_parametros,
)
from impulsoetl.sisab.parametros_cadastro.teste_validacao import (
    teste_validacao,
)
from impulsoetl.sisab.parametros_cadastro.tratamento import tratamento_dados


def obter_parametros(
    sessao: Session,
    visao_equipe: str,
    periodo: date,
    nivel_agregacao: str,
    teste: bool = True
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
            
    df = extrair_parametros(visao_equipe=visao_equipe,competencia=periodo,nivel_agregacao=nivel_agregacao)
    logger.info("Extração dos dados realizada...")
    df_tratado = tratamento_dados(sessao=sessao,dados_sisab_cadastros=df,periodo=periodo,nivel_agregacao=nivel_agregacao)
    logger.info("Transformação dos dados realizada...")
    teste_validacao(df, df_tratado,nivel_agregacao=nivel_agregacao)
    logger.info("Validação dos dados realizada...")
    carregar_parametros(sessao=sessao,parametros_transformada=df_tratado,visao_equipe=visao_equipe,nivel_agregacao=nivel_agregacao)



