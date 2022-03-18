#%%
import pandas as pd
from sqlalchemy import false


#%% leitura do arquivo pulando o cabeçalho
df = pd.read_csv ('rel_Validacao032022.csv',sep=';',encoding = 'ISO-8859-1', skiprows=range(0,4))


#%%
df.head(10)

#%%
df.isnull().sum()

#%%
df.info()


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
'''
    Dados excluídos por falta de necessidade: Região, Uf, Município
    Coluna IBGE e demais colunas mudarão de nome 
'''

df.drop(['Região', 'Uf','Municipio'], axis=1, inplace=True)


#%%
df.columns = ['municipio_id_sus', 'cnes_id', 'id_ine', 'validacao_nome', 'validacao_quantidade']


#%%
df.insert(0,"id", value= '')

#%%
df.insert(2,"periodo_id", value='')



#%%
df = df.assign(criacao_data = pd.Timestamp.now(),
            atualizacao_data = pd.Timestamp.now(), 
            no_prazo = '',
            periodo_codigo = '')

#%%
df.head()











