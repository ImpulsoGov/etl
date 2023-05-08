import warnings

warnings.filterwarnings("ignore")
from datetime import date

import pandas as pd
import numpy as np

from prefect import task

from impulsoetl.loggers import logger, habilitar_suporte_loguru
from impulsoetl.sisab.utilitarios_sisab_relatorio_producao import extrair_producao_por_municipio
from impulsoetl.sisab.utilitarios_sisab_relatorio_producao import transformar_producao_por_municipio
from impulsoetl.sisab.relatorio_saude_producao.extracao import extrair_relatorio

periodo_competencia = date(2023,1,1)


CATEGORIA_PROFISSIONAL_REDUZIDA = [
    'Psicólogo',
    'Profissional de educação física',
    'Nutricionista',
    'Médico',
    'Fonoaudiólogo',
    'Fisioterapeuta',
    'Farmacêutico',
    'Enfermeiro',
    'Assistente Social'
    ]


def obter_relatorio_reduzido_bloco_consolidado(
    periodo_competencia: date)-> pd.DataFrame():

    df_consolidado = pd.DataFrame()
    
    try:
        df_parcial = extrair_producao_por_municipio(
            tipo_producao="Atendimento individual",
            competencias=[periodo_competencia],
            selecoes_adicionais={
                "Problema/Condição Avaliada": "Selecionar Todos", 
                #"Problema/Condição Avaliada": ["Diabetes"], 
                "Conduta":"Selecionar Todos",
                #"Conduta":["Alta do episódio"],
                "Categoria do Profissional":CATEGORIA_PROFISSIONAL_REDUZIDA,
                #"Categoria do Profissional":["Médico"], 
                "Tipo de Atendimento": "Selecionar Todos",
                #"Tipo de Atendimento":["Consulta agendada"]
            },

            ).pipe(transformar_producao_por_municipio)
        
        df_consolidado = df_consolidado.append(df_parcial)

    except Exception as e:
        logger.error(e)
        pass

    return df_consolidado



def obter_relatorio_reduzido_bloco_6_tela1(
    periodo_competencia: date)-> pd.DataFrame():

    df_consolidado = pd.DataFrame()
    
    try:
        df_parcial = extrair_producao_por_municipio(
            tipo_producao="Atendimento individual",
            competencias=[periodo_competencia],
            selecoes_adicionais={
                #"Conduta":"Selecionar Todos",
                "Conduta":["Alta do episódio"],
                #"Categoria do Profissional":CATEGORIA_PROFISSIONAL_REDUZIDA,
                "Categoria do Profissional": ['Médico'], 
                #"Tipo de Atendimento": "Selecionar Todos",
                "Tipo de Atendimento":["Consulta agendada"]
            },

            ).pipe(transformar_producao_por_municipio)
        
        df_consolidado = df_consolidado.append(df_parcial)

    except Exception as e:
        logger.error(e)
        pass

    return df_consolidado

df = obter_relatorio_reduzido_bloco_6_tela1(periodo_competencia)
print(df)
print(df.columns)
