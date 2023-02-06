from datetime import date

import pandas as pd
import requests

from impulsoetl.sisab.funcoes_obter_relatorio_producao import extrair_producao_por_municipio
from impulsoetl.sisab.funcoes_obter_relatorio_producao import transformar_producao_por_municipio
from impulsoetl.loggers import logger

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

df_consolidado = pd.DataFrame()

for condicao in CONDICAO_AVALIADA:
    for conduta in CONDUTAS:
        for profissional in CATEGORIA_PROFISSIONAL:
            for atendimento in TIPO_ATENDIMENTO:
                try:
                    df_parcial = extrair_producao_por_municipio(
                        tipo_producao="Atendimento individual",
                        competencias=[date(2022, 12, 1)],
                        selecoes_adicionais={
                            "Problema/Condição Avaliada": [condicao], 
                            "Conduta":[conduta],
                            "Categoria do Profissional":[profissional], 
                            "Tipo de Atendimento":[atendimento], 
                        },
                        ).pipe(transformar_producao_por_municipio)
                except ParserError:
                    logger.info("Erro ao aplicar os seguintes filtros: " 
                    +condicao
                    +conduta
                    +profissional
                    +atendimento)

                    df_consolidado = df_consolidado.append(df_parcial)

print(df_consolidado)