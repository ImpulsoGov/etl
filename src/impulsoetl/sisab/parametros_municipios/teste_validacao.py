import pandas as pd

""" 
Amostra 01: Verifica se a quantidade de municípios é superior a 5000
Amostra 02: Verifica se há diferença na contagem de municípios 
Amostra 03: Verifica se há diferença parametro considerando apenas o município de Betim-MG
Amostra 04: Verifica se a quantidade de unidades federativas é igual a 26
Amostra 05: Verifica se há diferença no somatório de parâmetros 

"""

def amostra_01 (df:pd.DataFrame,df_tratado:pd.DataFrame) -> int:
    return df['IBGE'].nunique()

def amostra_02 (df:pd.DataFrame,df_tratado:pd.DataFrame) -> int:
    return df['IBGE'].nunique() - df_tratado['municipio_id_sus'].nunique()

def amostra_03 (df:pd.DataFrame,df_tratado:pd.DataFrame) -> int:
    return df.query("IBGE == '310670'")["parametro"].astype(int).sum() - df_tratado.query("municipio_id_sus == '310670'")["parametro"].sum() 

def amostra_04 (df:pd.DataFrame,df_tratado:pd.DataFrame) -> int:
	return df_tratado['unidade_geografica_id'].nunique()

def amostra_05 (df:pd.DataFrame,df_tratado:pd.DataFrame) -> int:
    return df['parametro'].astype(int).sum() - df_tratado['parametro'].sum()


def teste_validacao(df,df_tratado):
	assert amostra_01(df,df_tratado) > 5000
	assert amostra_02(df,df_tratado) == 0
	assert amostra_03(df,df_tratado) == 0
	assert amostra_04(df,df_tratado) >= 26
	assert amostra_05(df,df_tratado) == 0
