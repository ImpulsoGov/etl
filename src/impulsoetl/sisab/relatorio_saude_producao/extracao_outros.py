import warnings

warnings.filterwarnings("ignore")
from datetime import date

import pandas as pd
import numpy as np

from prefect import task

from impulsoetl.loggers import logger, habilitar_suporte_loguru
from impulsoetl.sisab.utilitarios_sisab_relatorio_producao import extrair_producao_por_municipio
from impulsoetl.sisab.utilitarios_sisab_relatorio_producao import transformar_producao_por_municipio


CATEGORIA_PROFISSIONAL_OUTROS = [
    'Agente de combate a endemias', 
    'Agente de saúde',
    'Assistente Social', 
    'Educador social', 
    'Farmacêutico', 
    'Fonoaudiólogo', 
    'Médico veterinário', 
    'Nutricionista', 
    'Outros prof. de nível médio', 
    'Outros prof. de nível superior', 
    'Profissional de educação física', 
    'Sanitarista', 
    'Terapeuta ocupacional', 
    'Naturólogo', 
    'Musicoterapeuta', 
    'Arteterapeuta', 
    'Terapeuto Holístico', 
    'Recepcionista'
]

def obter_relatorio_outros(
    periodo_competencia: date)-> pd.DataFrame():

    df_consolidado = pd.DataFrame()

    try:
        df_parcial = extrair_producao_por_municipio(
            tipo_producao="Atendimento individual",
            competencias=[periodo_competencia],
            selecoes_adicionais={
                "Problema/Condição Avaliada": "Selecionar Todos", 
                "Conduta":"Selecionar Todos",
                "Categoria do Profissional":CATEGORIA_PROFISSIONAL_OUTROS, 
            },

            ).pipe(transformar_producao_por_municipio)
        
        print(df_parcial)

        df_consolidado = df_consolidado.append(df_parcial)

    except Exception as e:
        logger.error(e)
        pass
                    
    return df_consolidado


@task(
    name="Extrair Relatório de Produção de Saúde - Profissionais Outros ",
    description=(
        "Extrai o relatório de Produção de Saúde a partir da página do SISAB."
    ),
    tags=["sisab", "produção", "extracao"],
    retries=2,
    retry_delay_seconds=120,
)
def extrair_relatorio_outros(
    periodo_competencia: date)-> pd.DataFrame():

    habilitar_suporte_loguru()
    logger.info("Iniciando extraçção do relatório...")

    df_extraido = obter_relatorio_outros(periodo_competencia)
    
    logger.info("Extração concluída")

    return df_extraido
