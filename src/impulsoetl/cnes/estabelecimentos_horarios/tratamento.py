import requests
import pandas as pd
from typing import Final
from frozendict import frozendict

from impulsoetl.cnes.extracao_lista_cnes import extrair_lista_cnes
from impulsoetl.cnes.estabelecimentos_horarios.extracao import extrair_horario_atendimento_estabelecimentos


COLUNAS_EXCLUIR = [
    'diaSemana',
    'hrInicioAtendimento',
    'hrFimAtendimento'
]

COLUNAS_TIPOS: Final[frozendict] = frozendict({
    'municipio_id_sus':'str',
    'estabelecimento_cnes_id':'str',
    'horario_funcionamento':'str'
})


def excluir_colunas(df: pd.DataFrame) -> pd.DataFrame:
    df.drop(columns=COLUNAS_EXCLUIR, inplace=True)

def tratar_tipos(df:pd.DataFrame) -> pd.DataFrame:
    df = df.astype(COLUNAS_TIPOS, errors = 'ignore').where(df.notna(), None)

def tratamento_dados(df:pd.DataFrame) -> pd.DataFrame:
    df['horario_funcionamento'] = df['diaSemana'] + " " + df['hrInicioAtendimento'] + "-" + df['hrFimAtendimento']
    df_tratado = df.groupby(['municipio_id_sus','estabelecimento_cnes_id'], as_index=False).agg({'horario_funcionamento': ' '.join})
    tratar_tipos(df_tratado)

    return df_tratado


coMun = '120001'
lista_codigos = extrair_lista_cnes(coMun)
df_extraido = extrair_horario_atendimento_estabelecimentos(coMun,lista_codigos)
df_tratado = tratamento_dados(df_extraido)
print(len(lista_codigos))
print(df_tratado)

