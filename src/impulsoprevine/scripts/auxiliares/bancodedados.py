from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, relationship
from pandas.io import sql

import os
from dotenv import load_dotenv
from pathlib import Path

env_path = Path('.')/'.env'
load_dotenv(dotenv_path=env_path)

credencial_ = {
   "local":[
       {
          "USERNAME":os.getenv("USERNAME_LOCAL"),
          "PASSWORD":os.getenv("PASSWORD_LOCAL"),
          "HOSTNAME":os.getenv("HOSTNAME_LOCAL"),
          "PORT":os.getenv("PORT_LOCAL"),
          "DATABASE":os.getenv("DATABASE_LOCAL")
       }
   ],
   "impulsogov-analitico":[
      {
        "USERNAME":os.getenv("USERNAME_ANALITICO"),
        "PASSWORD":os.getenv("PASSWORD_ANALITICO"),
        "HOSTNAME":os.getenv("HOSTNAME_ANALITICO"),
        "PORT":os.getenv("PORT_ANALITICO"),
        "DATABASE":os.getenv("DATABASE_ANALITICO")
     }
  ],
  "impulso-producao":[
      {
        "USERNAME":os.getenv("USERNAME_PROD"),
        "PASSWORD":os.getenv("PASSWORD_PROD"),
        "HOSTNAME":os.getenv("HOSTNAME_PROD"),
        "PORT":os.getenv("PORT_PROD"),
        "DATABASE":os.getenv("DATABASE_PROD")
     }
   ]
}


def readQuery(query, database):
   # if os.getenv("IS_LOCAL"): credencial = credencial_["local"][0] 
   # if os.getenv("IS_DEV"): credencial = credencial_["impulsogov-analitico"][0] 
   # if os.getenv("IS_PROD"): credencial = credencial_["impulso-producao"][0] 
   credencial = credencial_["impulsogov-analitico"][0]
   engine = create_engine(
   'postgresql://{}:{}@{}:{}/{}?'.format(credencial['USERNAME'],
                     credencial['PASSWORD'],
                     credencial['HOSTNAME'],
                     credencial['PORT'],
                     credencial['DATABASE']), connect_args={"options": "-c statement_timeout=100000000"}
   )
   conexao = engine.connect()
   resultados = sql.read_sql(query, conexao)
   return resultados

def executeQuery(query, database):
   # if os.getenv("IS_LOCAL"): credencial = credencial_["local"][0] 
   # if os.getenv("IS_DEV"): credencial = credencial_["impulsogov-analitico"][0] 
   # if os.getenv("IS_PROD"): credencial = credencial_["impulso-producao"][0] 
   credencial = credencial_["impulsogov-analitico"][0]
   engine = create_engine(
      'postgresql://{}:{}@{}:{}/{}?'.format(credencial['USERNAME'],
                        credencial['PASSWORD'],
                        credencial['HOSTNAME'],
                        credencial['PORT'],
                        credencial['DATABASE']), connect_args={"options": "-c statement_timeout=200000000"}
      )
   conexao = engine.connect()
   conexao.execute(query)
   conexao.execute('commit;')
   return True