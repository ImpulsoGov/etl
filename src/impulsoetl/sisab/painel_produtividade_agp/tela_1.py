import warnings

warnings.filterwarnings("ignore")
from datetime import date

import pandas as pd
import numpy as np

from typing import Final
from frozendict import frozendict


from sqlalchemy.orm import Session

from impulsoetl import __VERSION__
from impulsoetl.bd import Sessao, tabelas

from impulsoetl.loggers import logger, habilitar_suporte_loguru
from impulsoetl.sisab.utilitarios_sisab_relatorio_producao import extrair_producao_por_municipio
from impulsoetl.sisab.utilitarios_sisab_relatorio_producao import transformar_producao_por_municipio
from impulsoetl.utilitarios.bd import carregar_dataframe




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


def extrair_relatorio_reduzido_bloco_6_tela1(
    periodo_competencia: date)-> pd.DataFrame():

    df_consolidado = pd.DataFrame()
    
    try:
        df_parcial = extrair_producao_por_municipio(
            tipo_producao="Atendimento individual",
            competencias=[periodo_competencia],
            selecoes_adicionais={
                "Conduta":"Selecionar Todos",
                #"Conduta":["Alta do episódio"],
                "Categoria do Profissional":CATEGORIA_PROFISSIONAL_REDUZIDA,
                #"Categoria do Profissional": ['Médico'], 
                "Tipo de Atendimento": "Selecionar Todos",
                #"Tipo de Atendimento":["Consulta agendada"]
            },

            ).pipe(transformar_producao_por_municipio)
        
        df_consolidado = df_consolidado.append(df_parcial)

    except Exception as e:
        logger.error(e)
        pass

    return df_consolidado

COLUNAS_RENOMEAR: Final[dict[str, str]] = {
    "uf_sigla":"municipio_uf",
    "periodo_data_inicio":"periodo_data",
    "Conduta":"conduta",
    "Categoria do Profissional":"categoria_profissional",
    "Tipo de Atendimento":"tipo_atendimento",
    "quantidade_aprovada":"quantidade"
}

COLUNAS_TIPOS: Final[frozendict] = frozendict(
    {
    "municipio_uf":"str",
    "periodo_data":"str",
    "municipio_nome":"str",
    "periodo_data":"str",
    "conduta":"str",
    "categoria_profissional":"str",
    "tipo_atendimento":"str",
    "quantidade":"int"
    }
)

def tratar_tipos(df_extraido: pd.DataFrame) -> pd.DataFrame:
    df_extraido = df_extraido.astype(COLUNAS_TIPOS, errors="ignore").where(
        df_extraido.notna(), None
    )
    return df_extraido

def renomear_colunas(df_extraido: pd.DataFrame) -> pd.DataFrame:
    df_extraido.rename(columns=COLUNAS_RENOMEAR, inplace=True)
    return df_extraido



def tratamento_dados(
        df_extraido: pd.DataFrame, periodo_id: str, unidade_geografica_id: str
)-> pd.DataFrame:
    df_extraido = renomear_colunas(df_extraido)
    tratar_tipos(df_extraido)
    df_extraido["periodo_id"] = periodo_id
    df_extraido["unidade_geografica_id"] = unidade_geografica_id
    df_extraido = df_extraido.reset_index(drop=True)

    return df_extraido

def relatorio_tela1(
    sessao: Session,
    tabela_destino: str,
    periodo_competencia: date,
    periodo_id: str,
    unidade_geografica_id: str
)-> None:

    logger.info("Extraindo relatório da competencia {}, ...".format(periodo_competencia))

    df_extraido = extrair_relatorio_reduzido_bloco_6_tela1(
        periodo_competencia = periodo_competencia
    )
    
    df_tratado = tratamento_dados(
        df_extraido=df_extraido,
        periodo_id=periodo_id,
        unidade_geografica_id=unidade_geografica_id,
    )

    carregar_dataframe(
        sessao=sessao, df=df_tratado, tabela_destino=tabela_destino
    )

agendamentos = tabelas["configuracoes.capturas_agendamentos"]
capturas_historico = tabelas["configuracoes.capturas_historico"]

def obter_relatorio_painel_agp_tela1 (
    teste:bool = False
) -> None:

    operacao_id = "064590e2-c57a-72cf-bd6a-e680279eeb39"

    agendamentos = tabelas["configuracoes.capturas_agendamentos"]
    with Sessao() as sessao:
        agendamentos_relatorio_producao_saude = (
            sessao.query(agendamentos)
            .filter(agendamentos.c.operacao_id == operacao_id)
            .all()
        )
        sessao.commit()

        logger.info("Leitura dos Agendamentos ok!")
        for agendamento in agendamentos_relatorio_producao_saude:
            relatorio_tela1(
                sessao=sessao,
                tabela_destino=agendamento.tabela_destino,
                periodo_competencia=agendamento.periodo_data_inicio,
                periodo_id=agendamento.periodo_id,
                unidade_geografica_id=agendamento.unidade_geografica_id
            )

            if teste:  
                sessao.rollback()
                break

            logger.info("Registrando captura bem-sucedida...")
            requisicao_inserir_historico = capturas_historico.insert(
                {
                    "operacao_id": operacao_id,
                    "periodo_id": agendamento.periodo_id,
                    "unidade_geografica_id": agendamento.unidade_geografica_id,
                }
            )
            conector = sessao.connection()
            conector.execute(requisicao_inserir_historico)
            sessao.commit()
            
            logger.info("OK.")

obter_relatorio_painel_agp_tela1()
"""

periodo_competencia = date(2023,1,1)
df_extraido = obter_relatorio_reduzido_bloco_6_tela1(periodo_competencia)
print(df_extraido)
df_tratado = tratamento_dados(df_extraido)
print(df_extraido)
"""