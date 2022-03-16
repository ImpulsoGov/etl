
#%%
from sqlalchemy import create_engine
import psycopg2
import pandas as pd

#%%
# conexão com banco de dados                 #postgres://usuario:senha@host:porta(5432)/database
engine = create_engine('postgresql+psycopg2://silas:Sessions-Angrily9-Sandlot-Acuteness-Around-Scoundrel-Baboon-Poison@35.239.239.250:5432/postgres')
conn = engine.raw_connection()
cur = conn.cursor()
#teste de conexão - (tentar mais uma vez após 30 segundos caso dê erro)
try:
    conn 
    print("Success!!")
except Exception as e:
	print("connect fail : "+str(e))

#%%
#Parâmetro if_exists='replace': Se a tabela existir será reescrita. Por padrão é fail. Alterada em razão do exercício
#Parâmetro index=False: Não inclui o índice do dataframe como uma coluna na tabela


#df.to_sql('cnes_equipes_1', engine, if_exists='replace', index=False)

#%%
pd.read_sql_query("""select * from dadospublicos.populacao limit 1;""", engine)
# %%
