import warnings
warnings.filterwarnings("ignore")
import pandas as pd
from impulsoetl.bd import Sessao
from sqlalchemy.orm import Session


#from impulsoetl.comum.geografias import id_sus_para_id_impulso
from impulsoetl.loggers import logger

from impulsoetl.cnes.extracao_lista_cnes import extrair_lista_cnes
from impulsoetl.cnes.estabelecimentos_identificados.extracao import extrair_informacoes_estabelecimentos
from impulsoetl.cnes.estabelecimentos_identificados.tratamento import tratamento_dados
from impulsoetl.cnes.estabelecimentos_identificados.carregamento import carregar_dados


def obter_informacoes_estabelecimentos_identificados(
    sessao: Session,
    tabela_destino:str,
    codigo_municipio:str
):
    lista_cnes = extrair_lista_cnes(
        codigo_municipio=codigo_municipio)

    df_extraido = extrair_informacoes_estabelecimentos(
        codigo_municipio = codigo_municipio,
        lista_cnes = lista_cnes)

    df_tratado = tratamento_dados(
        df_extraido = df_extraido,
        sessao=sessao
    )

    carregar_dados(
        sessao=sessao, 
        df_tratado=df_tratado, 
        tabela_destino=tabela_destino)

    return df_tratado

#with Sessao() as sessao:
#    codigo_municipio = '120001' #acrelandia
#    data = obter_informacoes_estabelecimentos_identificados(sessao,codigo_municipio)
#    print(data)