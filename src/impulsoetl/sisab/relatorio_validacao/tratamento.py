#%%
import pandas as pd
from sqlalchemy import false


#%%
df = pd.read_csv ('rel_Validacao032022.csv',sep=';',encoding = 'ISO-8859-1', skiprows=range(0,4))


#%%
df.head(10)



#%%Exclusão coluna NaN
df.drop('Unnamed: 8', axis=1, inplace=True)
print(df)


#%% 
df.iloc[range(48264,48269)]


#%% Exclusao rodapé
df.drop(range(48265,48269))


#%% aplicando a exclusão
df.drop(range(48265,48269),inplace=True)
#%%
print(df)


#%%
'''
    Dados excluídos por falta de necessidade: Região, Uf, Município
    Coluna IBGE e demais colunas mudarão de nome 
'''

df.drop(['Região', 'Uf','Municipio'], axis=1, inplace=True)


#%%
df.head()


#%%
df.columns


#%%
df.columns = ['municipio_id_sus', 'cnes_id', 'id_ine', 'validacao_nome', 'validacao_quantidade']
df.head(2)

#%%
df.isnull().sum()
#%%
df.info()


















# %% Rascunho

df1 = pd.DataFrame(columns=['unidade_geografica_id_sus', 'municipio_id_sus', 'periodo_codigo', 'estabelecimento_id_cnes', 'equipe_id_ine', 'validacao_descricao', 'validacao_quantidade', 'no_prazo'])


#%%

dados = pd.read_csv(path, sep=';', dtype={'Total': str})
df_partial[['municipio_id_sus', 'estabelecimento_id_cnes', 'equipe_id_ine', 'validacao_descricao', 'validacao_quantidade']] = dados[[',IBGE', 'CNES', 'INE', 'Validação', 'Total']]
df_partial['municipio_id_sus'] = df_partial['municipio_id_sus'].str.split(pat=",", expand=True)[1]
df_partial['unidade_geografica_id_sus'] = df_partial['municipio_id_sus']
df_partial.periodo_codigo = periodo[1]
df_partial.no_prazo = prazo[1]
df = df.append(df_partial)











