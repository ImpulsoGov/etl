from sqlalchemy.orm import Session
from impulsoetl.sisab.parametros_equipes.extracao import (
_extrair_parametros_equipes,
extrair_parametros_equipes
)
from impulsoetl.sisab.parametros_equipes.tratamento import tratamento_dados
from impulsoetl.sisab.parametros_equipes.teste_validacao import teste_validacao
from impulsoetl.sisab.parametros_equipes.carregamento import carregar_parametros_equipes
from impulsoetl.bd import Sessao
from impulsoetl.tipos import DatetimeLike
#
import sys
sys.path.append("/Users/Walter Matheus/Impulso/etl/src/impulsoetl")
from tipos import DatetimeLike
from bd import Sessao
#

def obter_parametros_equipes(sessao: Session,visao_equipe:list,periodo:DatetimeLike,teste: bool = False)->None:
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

  df = _extrair_parametros_equipes(extrair_parametros_equipes(visao_equipe[0][1], periodo)) 
  df_tratado = tratamento_dados(sessao=sessao,dados_sisab_cadastros=df,periodo=periodo)
  teste_validacao(df,df_tratado)
  carregar_parametros_equipes(sessao=sessao,parametros_equipes_transformada=df_tratado,visao_equipe=visao_equipe[0][0])
  if not teste:
      sessao.commit()

periodo = '2022-01-01'
visao_equipe=[('equipes-validas','|HM|NC|AQ|')] 
teste=False

if __name__ == "__main__":
    with Sessao() as sessao:
        obter_parametros_equipes(sessao=sessao,visao_equipe=visao_equipe,periodo=periodo,teste=teste)