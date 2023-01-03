import pandas as pd
import json

from sqlalchemy.orm import Session

import sys
sys.path.append (r'C:\Users\maira\Impulso\etl\src\impulsoetl')

from impulsoetl.loggers import habilitar_suporte_loguru, logger
from impulsoetl.utilitarios.bd import carregar_dataframe

def carregar_dados(
    sessao: Session, 
    df_tratado: pd.DataFrame, 
    tabela_destino: str
) -> int:
    
    habilitar_suporte_loguru()
    logger.info("Carregando dados em tabela...")

    carregar_dataframe(
        sessao=sessao, df=df_tratado, tabela_destino=tabela_destino
    )

    logger.info(
        "Carregamento concluído para a tabela `{tabela_nome}`: "
        + "adicionadas {linhas_adicionadas} novas linhas.",
        tabela_nome=tabela_destino,
        linhas_adicionadas=len(df_tratado),
    )

    return 0