import pandas as pd
from sqlalchemy.orm import Session

import sys
sys.path.append (r'C:\Users\maira\Impulso\etl\src\impulsoetl')

#from impulsoetl.comum.geografias import id_sus_para_id_impulso
#from impulsoetl.loggers import logger

from cnes.extracao_lista_cnes import extrair_lista_cnes
from cnes.estabelecimentos_identificados.extracao import extrair_informacoes_estabelecimentos
from cnes.estabelecimentos_identificados.tratamento import tratamento_dados


def obter_informacoes_estabelecimentos(
    #sessao: Session,
    #tabela_destino:str,
    codigo_municipio:str
):
    lista_cnes = extrair_lista_cnes(
        codigo_municipio=codigo_municipio)

    df_extraido = extrair_informacoes_estabelecimentos(
        codigo_municipio = codigo_municipio,
        lista_cnes = lista_cnes)

    df_tratado = tratamento_dados(
        df_extraido = df_extraido)

    #carregar_dados(sessao=sessao, df_tratado=df_tratado, tabela_destino=tabela_destino)

    return df_tratado

codigo_municipio = '120001' #acrelandia
data = obter_informacoes_estabelecimentos(codigo_municipio)
print(data)