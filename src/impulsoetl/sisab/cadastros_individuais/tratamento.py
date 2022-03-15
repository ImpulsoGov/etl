import uuid
from datetime import datetime
import numpy as np
import pandas as pd
from io import StringIO
from teste_carregamento_cadastro import insereBanco


# Parâmetros #########################
visao_equipe=[
    ('equipes-validas','|HM|NC|AQ|')
]
ponderacao = [True, False]

periodos_dict = {'ABR/2018.Q1':'2018.Q1',
            'AGO/2018.Q2':'2018.Q2',
            'DEZ/2018.Q3':'2018.Q3',
            'ABR/2019.Q1':'2019.Q1',
            'AGO/2019.Q2':'2019.Q2',
            'DEZ/2019.Q3':'2019.Q3',
            'ABR/2020.Q1':'2020.Q1',
            'AGO/2020.Q2':'2020.Q2',
            'DEZ/2020.Q3':'2020.Q3',
            'JAN/2021':'2021.M1',
            'FEV/2021':'2021.M2',
            'MAR/2021':'2021.M3',
            'ABR/2021.Q1':'2021.M4',
            'MAI/2021':'2021.M5',
            'JUN/2021':'2021.M6',
            'JUL/2021':'2021.M7',
            'AGO/2021.Q2':'2021.M8',
            'SET/2021':'2021.M9',
            'OUT/2021':'2021.M10',
            'NOV/2021':'2021.M11',
            'DEZ/2021.Q3':'2021.M12',
            'JAN/2022':'2022.M1'}

################################################################

def formatarTipo(df):
    try:
        df['id'] = df['id'].astype('string')
        df['municipio_id_sus'] = df['municipio_id_sus'].astype('string')
        df['periodo_id'] = df['periodo_id'].astype('string')
        df['periodo_codigo'] = df['periodo_codigo'].astype('string')
        df['estabelecimento_id_cnes'] = df['estabelecimento_id_cnes'].astype('string')
        df['estabelecimento_nome'] = df['estabelecimento_nome'].astype('string')
        df['equipe_id_ine'] = df['equipe_id_ine'].astype('string')
        df['quantidades'] = df['quantidades'].astype(int)
        df['criterio_pontuacao_possui'] = df['criterio_pontuacao_possui'].astype(int)
        df['criacao_data'] = df['criacao_data'].astype('string')
        df['atualizacao_data'] = df['atualizacao_data'].astype('string')
        print(df.info())
        #insereBanco(df)
    except Exception as e:
      print(e) 

###############################################################
def tratamentoDados():
    try:
        sisab_cadastros_municipios = pd.DataFrame(columns=['id','municipio_id_sus','periodo_id','periodo_codigo','estabelecimento_id_cnes','estabelecimento_nome','equipe_id_ine','quantidades','criterio_pontuacao_possui','criacao_data','atualizacao_data'])
        for i in range(len(visao_equipe)):
            for k in range(len(ponderacao)):
                quad = "202201"
                path = 'ponderacao-'+str(ponderacao[k])+"-"+visao_equipe[i][0]+"-"+quad+".csv"
                dados_sisab_cadastros_csv = pd.read_csv(path, sep=';')

                for item in ['JAN/2022']:
                    if visao_equipe[i][0] == 'equipes-validas':
                        sisab_cadastros_municipios_parcial = pd.DataFrame(columns=[
                                                            'municipio_id_sus',
                                                            'periodo_codigo',
                                                            'estabelecimento_id_cnes',
                                                            'estabelecimento_nome',
                                                            'equipe_id_ine',
                                                            'quantidades',
                                                            'criterio_pontuacao_possui'])
                        sisab_cadastros_municipios_parcial[['municipio_id_sus', 'estabelecimento_id_cnes', 'estabelecimento_nome', 'equipe_id_ine', 'quantidades']] = dados_sisab_cadastros_csv[['IBGE', 'CNES', 'Nome UBS', 'INE', item]]  
                        sisab_cadastros_municipios_parcial['criterio_pontuacao_possui'] = ponderacao[k]
                        sisab_cadastros_municipios_parcial['periodo_codigo'] = periodos_dict[item]
                        sisab_cadastros_municipios = pd.concat([sisab_cadastros_municipios,sisab_cadastros_municipios_parcial])
                        

        sisab_cadastros_municipios.reset_index(drop=True, inplace=True)
        sisab_cadastros_municipios['id'] = sisab_cadastros_municipios.apply(lambda row:uuid.uuid4(), axis=1)
        #sisab_cadastros_municipios['periodo_id'] = sisab_cadastros_municipios.apply(lambda row:uuid.uuid4(), axis=1)
        sisab_cadastros_municipios['criacao_data'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sisab_cadastros_municipios['atualizacao_data'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        #sisab_cadastros_municipios.to_csv("sisab_cadastros_municipios_equipe_validas_202112.csv")
        print(sisab_cadastros_municipios.info())

        formatarTipo(sisab_cadastros_municipios)
    except Exception as e:
      print(e)

tratamentoDados()