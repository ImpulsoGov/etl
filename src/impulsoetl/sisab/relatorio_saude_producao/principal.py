import warnings

warnings.filterwarnings("ignore")

import pandas as pd

from datetime import date
from sqlalchemy.orm import Session

from impulsoetl import __VERSION__
from impulsoetl.bd import Sessao
from impulsoetl.sisab.relatorio_saude_producao.extracao import extrair_relatorio_saude_producao
from impulsoetl.sisab.relatorio_saude_producao.tratamento import tratamento_dados
from impulsoetl.utilitarios.bd import carregar_dataframe

def obter_relatorio_producao_por_profissional_problema_conduta_atendimento(
    sessao: Session,
    tabela_destino: str,
    periodo_competencia: date,
    periodo_id: str,
    unidade_geografica_id: str,
) -> None:


    df_extraido = extrair_relatorio_saude_producao(
        periodo_competencia = periodo_competencia
    )
    
    print(df_extraido)
    
    df_tratado = tratamento_dados(
        df_extraido=df_extraido,
        periodo_id=periodo_id,
        unidade_geografica_id=unidade_geografica_id,
    )

    carregar_dataframe(
        sessao=sessao, df=df_tratado, tabela_destino=tabela_destino
    )

    return df_tratado
