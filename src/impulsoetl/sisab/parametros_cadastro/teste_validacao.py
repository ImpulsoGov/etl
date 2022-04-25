import pandas as pd

""" 
verifica_qtd_municipios: Verifica se a quantidade de municípios é superior a 5000
verifica_diferenca_ctg_municpios: Verifica se há diferença na contagem de municípios 
verifica_diferenca_mun_betim: Verifica se há diferença parametro considerando apenas o município de Betim-MG
verifica_qtd_uf: Verifica se a quantidade de unidades federativas é igual a 26
verifica_diferenca_qtd_parametro: Verifica se há diferença no somatório de parâmetros 
verifica_diferenca_ctg_cnes: Verifica se há diferença na contagem de estabelecimentos 
verifica_diferenca_ctg_ine: Verifica se há diferença na contagem de equipes 

"""

def verifica_qtd_municipios (df:pd.DataFrame,df_tratado:pd.DataFrame) -> int:
    return df['IBGE'].nunique()

def verifica_diferenca_ctg_municpios (df:pd.DataFrame,df_tratado:pd.DataFrame) -> int:
    return df['IBGE'].nunique() - df_tratado['municipio_id_sus'].nunique()

def verifica_diferenca_mun_betim (df:pd.DataFrame,df_tratado:pd.DataFrame) -> int:
    return df.query("IBGE == '310670'")["parametro"].astype(int).sum() - df_tratado.query("municipio_id_sus == '310670'")["parametro"].sum() 

def verifica_qtd_uf (df:pd.DataFrame,df_tratado:pd.DataFrame) -> int:
	return df_tratado['unidade_geografica_id'].nunique()

def verifica_diferenca_qtd_parametro (df:pd.DataFrame,df_tratado:pd.DataFrame) -> int:
    return df['parametro'].astype(int).sum() - df_tratado['parametro'].sum()

def verifica_diferenca_ctg_cnes (df:pd.DataFrame,df_tratado:pd.DataFrame) -> int:
	return df_tratado['cnes_id'].nunique() - df_tratado['cnes_id'].nunique()

def verifica_diferenca_ctg_ine (df:pd.DataFrame,df_tratado:pd.DataFrame) -> int:
	return df_tratado['ine_id'].nunique() - df_tratado['ine_id'].nunique()


def teste_validacao(df:pd.DataFrame,df_tratado:pd.DataFrame,nivel_agregacao:str):
	assert verifica_qtd_municipios(df,df_tratado) > 5000
	assert verifica_diferenca_ctg_municpios(df,df_tratado) == 0
	assert verifica_diferenca_mun_betim(df,df_tratado) == 0
	assert verifica_qtd_uf(df,df_tratado) >= 26
	assert verifica_diferenca_qtd_parametro(df,df_tratado) == 0
	if nivel_agregacao == 'estabelecimentos_equipes':	
		assert verifica_diferenca_ctg_cnes(df,df_tratado) == 0
		assert verifica_diferenca_ctg_ine(df,df_tratado) == 0
		
