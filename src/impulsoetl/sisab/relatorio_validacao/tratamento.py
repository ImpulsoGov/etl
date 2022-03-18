#%%
import pandas as pd
from sqlalchemy import false


#%% leitura do arquivo pulando o cabeçalho e últimas linhas
df = pd.read_csv ('rel_Validacao032022.csv',sep=';',encoding = 'ISO-8859-1', skiprows=range(0,4), skipfooter=4)


#%%

# #%% Análise dos dados
# df.head(10)
# #%%
# df.tail(10)
# #%%
# print(df)

# #%%
# df.isnull().sum()

# #%%
# df.info()


#%%Exclusão coluna NaN
df.drop('Unnamed: 8', axis=1, inplace=True)
print(df)



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












# %%
