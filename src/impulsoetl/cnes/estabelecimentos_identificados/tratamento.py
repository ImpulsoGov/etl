import warnings
warnings.filterwarnings("ignore")


import pandas as pd
import numpy as np
from typing import Final
from frozendict import frozendict
from sqlalchemy.orm import Session
from datetime import date
from impulsoetl.bd import Sessao


from impulsoetl.cnes.extracao_lista_cnes import extrair_lista_cnes
from impulsoetl.cnes.estabelecimentos_identificados.extracao import extrair_informacoes_estabelecimentos
from impulsoetl.loggers import logger


COLUNAS_RENOMEAR: Final[dict[str, str]]= {
    'municipio':'municipio_id_sus',
    'cnes' : 'estabelecimento_cnes_id',
    'noFantasia':'estabelecimento_nome',
    'noEmpresarial':'estabelecimento_nome_empresarial',
    'natJuridica':'estabelecimento_natureza_juridica',
    'cnpj':'estabelecimento_cnpj',
    'noLogradouro':'estabelecimento_logradouro',
    'nuEndereco':'estabelecimento_logradouro_numero',
    'bairro':'estabelecimento_bairro',
    'cep':'estabelecimento_cep',
    'regionalSaude':'estabelecimento_regional_saude',
    'dsTpUnidade':'estabelecimento_tipo',
    'dsStpUnidade':'estabelecimento_subtipo',
    'nvDependencia':'estabelecimento_dependencia',
    'tpGestao':'estabelecimento_gestao_tipo',
    'nuTelefone':'estabelecimento_telefone',
    'tpSempreAberto':'sempre_aberto',
    'dtCarga':'estabelecimento_data_cadastro',
    'coMotivoDesab':'codigo_motivo_desativacao', 
    'dsMotivoDesab':'descricao_motivo_desativacao',
    'dtAtualizacaoOrigem':'estabelecimento_data_atualizacao_base_local', 
    'dtAtualizacao':'estabelecimento_data_atualizacao_base_nacional'
}

COLUNAS_EXCLUIR = [
    'id',
    'natJuridicaMant',
    'tpPessoa',
    'nuAlvara',
    'dtExpAlvara', 
    'orgExpAlvara',
    'uf',
    'noComplemento',
    'noMunicipio',
    'cpfDiretorCln', 
    'stContratoFormalizado', 
    'nuCompDesab', 
    ]

ESTABELECIMENTO_NATUREZA_JURIDICA: Final[dict[str, str]]= {
    '1':'ADMINISTRAÇÃO PÚBLICA',
    '2':'ENTIDADES EMPRESARIAIS',
    '3':'ENTIDADES SEM FINS LUCRATIVOS',
    '4':'PESSOAS FÍSICAS'
}

ESTABELECIMENTO_GESTAO_TIPO: Final[dict[str, str]]= {
    'D':'DUPLA',
    'E':'ESTADUAL',
    'M':'MUNICIPAL'
}

ESTABELECIMENTO_DEPENDENCIA: Final[dict[str,str]]={
    '1':'INDIVIDUAL',
    '3':'MANTIDA'
}

COLUNAS_TIPOS: Final[frozendict] = frozendict(
    {
    'estabelecimento_cnes_id':'str', 
    'estabelecimento_nome':'str',
    'estabelecimento_nome_empresarial':'str',
    'estabelecimento_natureza_juridica':'str',
    'estabelecimento_cnpj':'str',
    'estabelecimento_dependencia':'str',
    'estabelecimento_tipo':'str', 
    'estabelecimento_subtipo':'str',
    'estabelecimento_logradouro':'str',
    'estabelecimento_logradouro_numero':'str',      
    'estabelecimento_cep':'str',
    'estabelecimento_regional_saude':'str',
    'estabelecimento_bairro':'str',
    'municipio_id_sus':'str',
    'estabelecimento_gestao_tipo':'str',
    'estabelecimento_telefone':'str',
    'sempre_aberto':'boolean',
    'codigo_motivo_desativacao':'str',
    'descricao_motivo_desativacao':'str',
    'estabelecimento_data_cadastro':'str',
    'estabelecimento_data_atualizacao_base_local':'str',
    'estabelecimento_data_atualizacao_base_nacional':'str',
    'status_estabelecimento':'str',
    
    }
)

COLUNAS_DATA = ['estabelecimento_data_cadastro','estabelecimento_data_atualizacao_base_local','estabelecimento_data_atualizacao_base_nacional']

def status_estabelecimento(df_extraido:pd.DataFrame)->pd.DataFrame:
    df_extraido['status_estabelecimento'] = np.where(df_extraido['codigo_motivo_desativacao'].isnull(),'ATIVO','DESATIVADO')
    return df_extraido

def renomear_colunas(df_extraido: pd.DataFrame) -> pd.DataFrame:
    df_extraido.rename(columns=COLUNAS_RENOMEAR, inplace=True)
    return df_extraido


def excluir_colunas(df_extraido: pd.DataFrame) -> pd.DataFrame:
    df_extraido.drop(columns=COLUNAS_EXCLUIR, inplace=True)
    return df_extraido


def tratar_valores_codificados(df_extraido: pd.DataFrame) -> pd.DataFrame:
    df_extraido['sempre_aberto'] = df_extraido['sempre_aberto'].map({'S':True,'N':False})
    df_extraido['estabelecimento_natureza_juridica'] = df_extraido['estabelecimento_natureza_juridica'].map(ESTABELECIMENTO_NATUREZA_JURIDICA)
    df_extraido['estabelecimento_gestao_tipo'] = df_extraido['estabelecimento_gestao_tipo'].map(ESTABELECIMENTO_GESTAO_TIPO)
    df_extraido['estabelecimento_dependencia'] = df_extraido['estabelecimento_dependencia'].map(ESTABELECIMENTO_DEPENDENCIA)
    return df_extraido


def tratar_tipos(df_extraido:pd.DataFrame) -> pd.DataFrame:
    for coluna in COLUNAS_DATA:
        df_extraido[coluna] = pd.to_datetime(df_extraido[coluna],infer_datetime_format=True)

    df_extraido = df_extraido.astype(COLUNAS_TIPOS, errors = 'ignore').where(df_extraido.notna(), None)
    #rint(df_extraido.info())

    return df_extraido


def tratamento_dados(
    df_extraido:pd.DataFrame,
    sessao:Session
) -> pd.DataFrame:

    logger.info("Iniciando o tratamento dos dados ...")

    df_extraido = renomear_colunas(df_extraido)
    df_extraido = excluir_colunas(df_extraido)
    df_extraido = status_estabelecimento(df_extraido)
    df_extraido = tratar_valores_codificados(df_extraido)
    df_extraido = tratar_tipos(df_extraido)
    df_extraido = df_extraido.reset_index(drop=True)

    logger.info("Dados transformados ...")

    return df_extraido

#coMun = '120001'

#with Sessao() as sessao:
 #   lista_cnes = extrair_lista_cnes(coMun)
  #  df_extraido= extrair_informacoes_estabelecimentos(coMun,lista_cnes)
   # #df_extraido.info()
    #df_tratado = tratamento_dados(df_extraido, sessao)
    p#rint(list(df_tratado.columns))
#df_extraido_tratado.to_csv(r'C:\Users\maira\Impulso\etl_cnes\acrelandia.csv', index=False)