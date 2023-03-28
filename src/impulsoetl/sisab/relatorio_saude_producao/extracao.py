import warnings

warnings.filterwarnings("ignore")
from datetime import date

import pandas as pd
import numpy as np

from prefect import task

from impulsoetl.loggers import logger, habilitar_suporte_loguru
from impulsoetl.sisab.utilitarios_sisab_relatorio_producao import extrair_producao_por_municipio
from impulsoetl.sisab.utilitarios_sisab_relatorio_producao import transformar_producao_por_municipio


CONDICAO_AVALIADA = [
    'Asma',
    'Desnutrição',
    'Diabetes',
    'DPOC',
    'Hipertensão arterial',
    'Obesidade',
    'Pré-natal',
    'Puericultura',
    'Puerpério (até 42 dias)',
    'Saúde sexual e reprodutiva',
    'Tabagismo',
    'Usuário de álcool',
    'Usuário de outras drogas',
    'Saúde mental',
    'Reabilitação',
    'D.Transmissíveis - Dengue',
    'Doenças transmissíveis - DST',
    'D.Transmissíveis - Hanseníase',
    'D.Transmissíveis - Tuberculose',
    'Rast. câncer de mama',
    'Rast. câncer do colo do útero',
    'Rast. risco cardiovascular'
    ]

CONDUTAS = [
    'Agendamento p/ NASF',
    'Agendamento para grupos',
    'Alta do episódio',
    'Encaminhamento interno no dia',
    'Encaminhamento intersetorial',
    'Encaminhamento p/ CAPS',
    'Encaminhamento p/ internação hospitalar',
    'Encaminhamento p/ serviço de atenção domiciliar',
    'Encaminhamento p/ serviço especializado',
    'Encaminhamento p/ urgência',
    'Retorno p/ cuidado continuado/programado',
    'Retorno para consulta agendada'
    ]   

CATEGORIA_PROFISSIONAL_REDUZIDA = [
    'Cirurgião dentista',
    'Enfermeiro',
    'Fisioterapeuta',
    'Médico',
    'Psicólogo',
    'Técnico e auxiliar de enfermagem',
    'Técnico e auxiliar de saúde bucal',
    'Outros'
    ]

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

def obter_relatorio_profissionais_reduzidos(
    periodo_competencia: date)-> pd.DataFrame():

    df_consolidado = pd.DataFrame()

    for condicao in CONDICAO_AVALIADA:
        for conduta in CONDUTAS:
            for profissional in CATEGORIA_PROFISSIONAL_REDUZIDA:
                    try:
                        df_parcial = extrair_producao_por_municipio(
                            tipo_producao="Atendimento individual",
                            competencias=[periodo_competencia],
                            selecoes_adicionais={
                                "Problema/Condição Avaliada": [condicao], 
                                "Conduta":[conduta],
                                "Categoria do Profissional":[profissional], 
                            },

                            ).pipe(transformar_producao_por_municipio)

                        df_consolidado = df_consolidado.append(df_parcial)

                    except Exception as e:
                        logger.error(e)
                        #logger.info("Erro ao aplicar os seguintes filtros: {} + {} + {}", 
                        #condicao,
                        #conduta,
                        #profissional)
                        pass
                    
    return df_consolidado


def obter_relatorio_profissionais_outros(
    periodo_competencia: date)-> pd.DataFrame():

    df_consolidado = pd.DataFrame()

    for condicao in CONDICAO_AVALIADA:
        for conduta in CONDUTAS:
                try:
                    df_parcial = extrair_producao_por_municipio(
                        tipo_producao="Atendimento individual",
                        competencias=[periodo_competencia],
                        selecoes_adicionais={
                            "Problema/Condição Avaliada": [condicao], 
                            "Conduta":[conduta],
                            "Categoria do Profissional": CATEGORIA_PROFISSIONAL_OUTROS
                        },

                        ).pipe(transformar_producao_por_municipio)

                    df_consolidado = df_consolidado.append(df_parcial)

                except Exception as e:
                    logger.error(e)
                    pass
                
    return df_consolidado


@task(
    name="Extrair Relatório de Produção de Saúde ",
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

    df_extraido_profissionais_reduzidos = obter_relatorio_profissionais_reduzidos(periodo_competencia)
    df_extraido_profissionais_outros = obter_relatorio_profissionais_outros(periodo_competencia)
    df_extraido = pd.concat([df_extraido_profissionais_reduzidos, df_extraido_profissionais_outros], ignore_index = True)

    logger.info("Extração concluída")

    return df_extraido




