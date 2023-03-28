import warnings

warnings.filterwarnings("ignore")
from datetime import date

import pandas as pd
import numpy as np

from prefect import task

from impulsoetl.loggers import logger, habilitar_suporte_loguru
from impulsoetl.sisab.utilitarios_sisab_relatorio_producao import extrair_producao_por_municipio
from impulsoetl.sisab.utilitarios_sisab_relatorio_producao import transformar_producao_por_municipio


CATEGORIA_PROFISSIONAL_REDUZIDA = [
    'Cirurgião dentista',
    'Enfermeiro',
    'Fisioterapeuta',
    'Médico',
    'Psicólogo',
    'Técnico e auxiliar de enfermagem',
    'Técnico e auxiliar de saúde bucal',
    ]

def obter_relatorio_reduzido(
    periodo_competencia: date)-> pd.DataFrame():

    df_consolidado = pd.DataFrame()
    
    try:
        df_parcial = extrair_producao_por_municipio(
            tipo_producao="Atendimento individual",
            competencias=[periodo_competencia],
            selecoes_adicionais={
                "Problema/Condição Avaliada": "Selecionar Todos", 
                "Conduta":"Selecionar Todos",
                "Categoria do Profissional":CATEGORIA_PROFISSIONAL_REDUZIDA, 
            },

            ).pipe(transformar_producao_por_municipio)
        
        print(df_parcial)

        df_consolidado = df_consolidado.append(df_parcial)

    except Exception as e:
        logger.error(e)
        logger.info("Erro ao aplicar os seguintes filtros: {} + {} + {}", 
        condicao,
        conduta,
        profissional)
        pass

    return df_consolidado

@task(
    name="Extrair Relatório de Produção de Saúde - Profissionais Selecionados ",
    description=(
        "Extrai o relatório de Produção de Saúde a partir da página do SISAB."
    ),
    tags=["sisab", "produção", "extracao"],
    retries=2,
    retry_delay_seconds=120,
)
def extrair_relatorio(
    periodo_competencia: date)-> pd.DataFrame():

    habilitar_suporte_loguru()
    logger.info("Iniciando extraçção do relatório...")

    df_extraido = obter_relatorio_reduzido(periodo_competencia)
    
    logger.info("Extração concluída")

    return df_extraido
