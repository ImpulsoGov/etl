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

CATEGORIA_PROFISSIONAL = [
    'Agente comunitário de saúde',
    'Agente de combate a endemias',
    'Agente de saúde',
    'Assistente Social',
    'Cirurgião dentista',
    'Educador social',
    'Enfermeiro',
    'Farmacêutico',
    'Fisioterapeuta',
    'Fonoaudiólogo',
    'Médico',
    'Médico veterinário',
    'Nutricionista',
    'Outros prof. de nível médio',
    'Outros prof. de nível superior',
    'Profissional de educação física',
    'Psicólogo',
    'Sanitarista',
    'Técnico e auxiliar de enfermagem',
    'Técnico e auxiliar de saúde bucal',
    'Terapeuta ocupacional',
    'Naturólogo',
    'Musicoterapeuta',
    'Arteterapeuta',
    'Terapeuto Holístico',
    'Recepcionista',
    ]

TIPO_ATENDIMENTO = [
    'Cons. agen. prog/cuid. cont.',
    'Consulta agendada',
    'Dem. esp. esc. inicial/orient.',
    'Dem. esp. consulta no dia',
    'Dem. esp. atendimento urgência'
    ]


def obter_relatorio_saude_producao_com_equipes (
    periodo_competencia:date)-> pd.DataFrame:
    
    """
    Captura dados do Relatório de Produção do SISAB selecionando todos os filtros para Tipo de equipe
    Argumentos:
        periodo_competencia: Mês da competência em referência
    
    Retorna:
        Objeto [`pandas.DataFrame`] com os dados extraídos.
    """

    df_consolidado = pd.DataFrame()

    for condicao in CONDICAO_AVALIADA:
        for conduta in CONDUTAS:
            for profissional in CATEGORIA_PROFISSIONAL:
                for atendimento in TIPO_ATENDIMENTO:
                    try:
                        df_parcial = extrair_producao_por_municipio(
                            tipo_producao="Atendimento individual",
                            competencias=[periodo_competencia],
                            selecoes_adicionais={
                                "Problema/Condição Avaliada": [condicao], 
                                "Conduta":[conduta],
                                "Categoria do Profissional":[profissional], 
                                "Tipo de Atendimento":[atendimento], 
                                "Tipo de Equipe":"Selecionar Todos"
                            },
                            
                            ).pipe(transformar_producao_por_municipio)
                  
                        df_consolidado = df_consolidado.append(df_parcial)

                    except pd.errors.ParserError as e:
                        logger.error(e)
                        logger.info("Erro ao aplicar os seguintes filtros: {} + {} + {} + {}", 
                        condicao,
                        conduta,
                        profissional,
                        atendimento)
                        pass
                    
                    except TypeError as e:
                        logger.error(e)
                        logger.info("Erro ao aplicar os seguintes filtros: {} + {} + {} + {}", 
                        condicao,
                        conduta,
                        profissional,
                        atendimento)
                        pass
    return df_consolidado

def obter_relatorio_saude_producao_sem_equipes (
    periodo_competencia:date)-> pd.DataFrame:
    
    """
    Captura dados do Relatório de Produção do SISAB sem filtros para Tipo de equipe
    Argumentos:
        periodo_competencia: Mês da competência em referência
    
    Retorna:
        Objeto [`pandas.DataFrame`] com os dados extraídos.
    """
    df_consolidado = pd.DataFrame()

    for condicao in CONDICAO_AVALIADA:
        for conduta in CONDUTAS:
            for profissional in CATEGORIA_PROFISSIONAL:
                for atendimento in TIPO_ATENDIMENTO:
                    try:
                        df_parcial = extrair_producao_por_municipio(
                            tipo_producao="Atendimento individual",
                            competencias=[periodo_competencia],
                            selecoes_adicionais={
                                "Problema/Condição Avaliada": [condicao], 
                                "Conduta":[conduta],
                                "Categoria do Profissional":[profissional], 
                                "Tipo de Atendimento":[atendimento], 
                            },
                            
                            ).pipe(transformar_producao_por_municipio)
                       
                        df_consolidado = df_consolidado.append(df_parcial)

                    except pd.errors.ParserError as e:
                        logger.error(e)
                        logger.info("Erro ao aplicar os seguintes filtros: {} + {} + {} + {}", 
                        condicao,
                        conduta,
                        profissional,
                        atendimento)
                        pass
                    
                    except TypeError as e:
                        logger.error(e)
                        logger.info("Erro ao aplicar os seguintes filtros: {} + {} + {} + {}", 
                        condicao,
                        conduta,
                        profissional,
                        atendimento)
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

    """
    Captura dados do Relatório de Produção do SISAB considerando os relatórios obtidos com e sem os filtros para Tipo de Equipe
    Argumentos:
        periodo_competencia: Mês da competência em referência
    
    Retorna:
        Objeto [`pandas.DataFrame`] com os dados extraídos.
    """

    habilitar_suporte_loguru()
    logger.info("Iniciando extraçção do relatório...")

    df_extraido_com_equipes = obter_relatorio_saude_producao_com_equipes(periodo_competencia)
    df_extraido_com_equipes = df_extraido_com_equipes.rename(columns = {'quantidade_aprovada':'total_com_equipes'})

    df_extraido_sem_equipes = obter_relatorio_saude_producao_sem_equipes(periodo_competencia)
    df_extraido_sem_equipes = df_extraido_sem_equipes.rename(columns = {'quantidade_aprovada':'total_sem_equipes'})

    agrupado_com_equipe = df_extraido_com_equipes.groupby(['uf_sigla','municipio_id_sus','municipio_nome','Problema/Condição Avaliada','Conduta','Categoria do Profissional','Tipo de Atendimento'])['total_com_equipes'].sum().to_frame().reset_index()
    agrupado_sem_equipe = df_extraido_sem_equipes.groupby(['uf_sigla','municipio_id_sus','municipio_nome','Problema/Condição Avaliada','Conduta','Categoria do Profissional','Tipo de Atendimento'])['total_sem_equipes'].sum().to_frame().reset_index()
   
    df_parcial = agrupado_com_equipe.merge(agrupado_sem_equipe, how='right',on = ['uf_sigla','municipio_id_sus','municipio_nome','Problema/Condição Avaliada','Conduta','Categoria do Profissional','Tipo de Atendimento'])
    df_parcial['total_com_equipes'] = df_parcial['total_com_equipes'].replace(np.nan, 0)
    df_parcial['quantidade_aprovada'] = (df_parcial['total_sem_equipes'] - df_parcial['total_com_equipes']).round(0)
    df_parcial['Tipo de Equipe'] = 'Equipe não identificada'
    df_parcial['periodo_data_inicio'] = periodo_competencia
    df_parcial = df_parcial[['uf_sigla','municipio_id_sus','municipio_nome','periodo_data_inicio','Tipo de Equipe','Problema/Condição Avaliada','Conduta','Categoria do Profissional','Tipo de Atendimento','quantidade_aprovada']]
    df_extraido_com_equipes = df_extraido_com_equipes.rename(columns = {'total_com_equipes':'quantidade_aprovada'})

    df_extraido = pd.concat([df_extraido_com_equipes, df_parcial], ignore_index = True)

    logger.info("Extração concluída")

    return df_extraido



