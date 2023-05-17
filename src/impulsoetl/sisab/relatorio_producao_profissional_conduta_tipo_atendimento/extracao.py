import warnings
warnings.filterwarnings("ignore")

import pandas as pd
from datetime import date

from impulsoetl.loggers import logger, habilitar_suporte_loguru
from impulsoetl.sisab.utilitarios_sisab_relatorio_producao import extrair_producao_por_municipio
from impulsoetl.sisab.utilitarios_sisab_relatorio_producao import transformar_producao_por_municipio


def extrair_relatorio(
    periodo_competencia: date)-> pd.DataFrame():

    df_consolidado = pd.DataFrame()
    
    try:
        df_parcial = extrair_producao_por_municipio(
            tipo_producao="Atendimento individual",
            competencias=[periodo_competencia],
            selecoes_adicionais={
                "Conduta":"Selecionar Todos",
                "Categoria do Profissional":"Selecionar Todos",
                "Tipo de Atendimento": "Selecionar Todos",
            },

            ).pipe(transformar_producao_por_municipio)
        
        df_consolidado = df_consolidado.append(df_parcial)

    except Exception as e:
        logger.error(e)
        pass

    return df_consolidado