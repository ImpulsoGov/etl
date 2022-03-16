#%%
import pandas as pd
from sqlalchemy import false


#%%
df = pd.read_csv ('rel_Validacao032022.csv',encoding = 'ISO-8859-1')
print(df)



#%%
df = pd.read_csv ('rel_Validacao032022.csv',sep=';',encoding = 'ISO-8859-1', skiprows=range(0,4))
print(df)


#%%
df.head(10)


#%%
df['Unnamed: 8']


#%%
df.drop('Unnamed: 8', axis=1, inplace=True)
print(df)


#%%
df.iloc[range(48264,48269)]


#%%
df.drop(range(48265,48269))

#%% aplicando a exclusão
df.drop(range(48265,48269),inplace=True)
#%%
print(df)
#%%
df.head(10)

# %%















