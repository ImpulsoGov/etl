import warnings
warnings.filterwarnings("ignore")
import pandas as pd

from sqlalchemy.orm import Session

from impulsoetl.loggers import logger
from impulsoetl.utilitarios.bd import carregar_dataframe

def carregar_dados(
    sessao: Session, 
    df_tratado: pd.DataFrame, 
    tabela_destino: str
) -> int:
    
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