from extracao import _extrair_cadastros_individuais,extrair_cadastros_individuais
from tratamento import tratamento_dados
from sqlalchemy.orm import Session
from carregamento import carregar_cadastros
from impulsoetl.tipos import DatetimeLike
from impulsoetl.bd import Sessao

com_ponderacao = [True,False]

def obter_cadastros_individuais(sessao: Session,visao_equipe:list,periodo:DatetimeLike,teste: bool = False)->None:
  
  """Extrai, transforma e carrega dados de cadastros de equipes de todos os municípios a partir do Sisab.

    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov.
        visao_equipe: Indica a situação da equipe considerada para a contagem dos cadastros.
        periodo: Referente ao mês/ano de disponibilização do relatório.
        teste: Indica se as modificações devem ser de fato escritas no banco de
            dados (`False`, padrão). Caso seja `True`, as modificações são
            adicionadas à uma transação, e podem ser revertidas com uma chamada
            posterior ao método [`Session.rollback()`][] da sessão gerada com o
            SQLAlchemy. """

  for k in range(len(com_ponderacao)):
      df = _extrair_cadastros_individuais(extrair_cadastros_individuais(visao_equipe[0][1],com_ponderacao[k], periodo), visao_equipe[0][0], com_ponderacao[k]) 
      df_tratado = tratamento_dados(sessao=sessao,dados_sisab_cadastros=df, com_ponderacao=com_ponderacao[k],periodo=periodo)
      carregar_cadastros(sessao=sessao,cadastros_transformada=df_tratado,visao_equipe=visao_equipe[0][0])
      if not teste:
          sessao.commit()
  
  

