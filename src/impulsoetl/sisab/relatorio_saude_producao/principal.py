import warnings
warnings.filterwarnings("ignore")

import pandas as pd
import time
import traceback

from datetime import date
from sqlalchemy.orm import Session
from prefect import flow 

from impulsoetl import __VERSION__
from impulsoetl.bd import Sessao
from impulsoetl.sisab.excecoes import SisabExcecao
from impulsoetl.sisab.relatorio_saude_producao.extracao import extrair_relatorio
from impulsoetl.sisab.relatorio_saude_producao.tratamento import tratamento_dados
from impulsoetl.sisab.relatorio_saude_producao.verificacao import verificar_informacoes_relatorio_producao

from impulsoetl.utilitarios.bd import carregar_dataframe
from impulsoetl.loggers import logger


@flow(
    name="Obter Relatório de Produção de Saúde",
    description=(
        "Extrai, transforma e carrega os dados do relatório de Produção de Saúde extraído a partir da página do SISAB."
    ),
    retries=0,
    retry_delay_seconds=None,
    version=__VERSION__,
    validate_parameters=False,
)
def obter_relatorio_producao_por_profissionais_reduzidos(
    sessao: Session,
    tabela_destino: str,
    periodo_competencia: date,
    periodo_id: str,
    unidade_geografica_id: str,
) -> None:

    """
    Extrai, transforma e carrega os dados do Relatório de Produção do SISAB
     Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite acessar a base de dados da ImpulsoGov.
        tabela_destino: Nome da tabela de destino a ser carregada com os dados extraidos e tratados.
        periodo_competencia: Data do mês de referência da extração.
        periodo_id: Código de identificação do período.
        unidade_geografica_id: Código de identificação da unidade geográfica.
    """
    tempo_inicio_etl = time.time()

    operacao_id = '063e2878-3247-78a7-83dd-1d291156cdf6'

    logger.info("Extraindo relatório da competencia {}, ...".format(periodo_competencia))

    try:

        df_extraido = extrair_relatorio(
            periodo_competencia = periodo_competencia
        )

        df_tratado = tratamento_dados(
            df_extraido=df_extraido,
            periodo_id=periodo_id,
            unidade_geografica_id=unidade_geografica_id,
        )

        verificar_informacoes_relatorio_producao(df_tratado)

        carregar_dataframe(
            sessao=sessao, df=df_tratado, tabela_destino=tabela_destino
        )
    
    except (KeyError,pd.errors.ParserError):
        traceback_str = traceback.format_exc()
        enviar_erro = SisabExcecao("Competência indisponível no SISAB")
        enviar_erro.insere_erro_database(sessao=sessao,traceback_str=traceback_str,operacao_id=operacao_id,periodo_id=periodo_id)

        logger.error("Data da competência do relatório não está disponível")
        return 0
        
    except Exception as mensagem_erro:
        traceback_str = traceback.format_exc()
        enviar_erro = SisabExcecao(mensagem_erro)
        enviar_erro.insere_erro_database(sessao=sessao,traceback_str=traceback_str,operacao_id=operacao_id,periodo_id=periodo_id)

        logger.error(mensagem_erro)
        return 0

    tempo_final_etl = time.time() - tempo_inicio_etl

    logger.info(
        "Terminou ETL para "
        + "da comepetência`{periodo_competencia}` "
        + "em {tempo_final_etl}.",
        periodo_competencia=periodo_competencia,
        tempo_final_etl=tempo_final_etl,
    )

