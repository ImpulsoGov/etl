import uuid
from datetime import datetime
import numpy as np
import pandas as pd
from carregamento import carregar_cadastros
from sqlalchemy.orm import Session
from impulsoetl.comum.datas import periodo_por_data

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



def formatarTipo(tabela_consolidada,sessao: Session):
    try:
        tabela_consolidada['id'] = tabela_consolidada['id'].astype('string')
        tabela_consolidada['municipio_id_sus'] = tabela_consolidada['municipio_id_sus'].astype('string')
        tabela_consolidada['periodo_id'] = tabela_consolidada['periodo_id'].astype('string')
        tabela_consolidada['periodo_codigo'] = tabela_consolidada['periodo_codigo'].astype('string')
        tabela_consolidada['cnes_id'] = tabela_consolidada['cnes_id'].astype('string')
        tabela_consolidada['cnes_nome'] = tabela_consolidada['cnes_nome'].astype('string')
        tabela_consolidada['ine_id'] = tabela_consolidada['ine_id'].astype('string')
        tabela_consolidada['quantidade'] = tabela_consolidada['quantidade'].astype(int)
        tabela_consolidada['criterio_pontuacao'] = tabela_consolidada['criterio_pontuacao'].astype(bool)
        tabela_consolidada['criacao_data'] = tabela_consolidada['criacao_data'].astype('string')
        tabela_consolidada['atualizacao_data'] = tabela_consolidada['atualizacao_data'].astype('string')
        carregar_cadastros(cadastros_transformada=tabela_consolidada,sessao=sessao)
    except Exception as e:
      print(e) 

def tratamentoDados(dados_sisab_cadastros,tipo_equipe,ponderacao,sessao:Session):
    try:
        tabela_consolidada = pd.DataFrame(columns=['id','municipio_id_sus','periodo_id','periodo_codigo','cnes_id','cnes_nome','ine_id','quantidades','criterio_pontuacao','criacao_data','atualizacao_data'])
        for item in ['JAN/2022']:
            
            if tipo_equipe == 'equipes-validas':
                tabela_equipes_validas = pd.DataFrame(columns=[
                                                    'municipio_id_sus',
                                                    'periodo_codigo',
                                                    'cnes_id',
                                                    'cnes_nome',
                                                    'ine_id',
                                                    'quantidade',
                                                    'criterio_pontuacao',
                                                    'periodo_id'])
                tabela_equipes_validas[['municipio_id_sus', 'cnes_id', 'cnes_nome', 'ine_id', 'quantidade']] = dados_sisab_cadastros[['IBGE', 'CNES', 'Nome UBS', 'INE', item]]  
                tabela_equipes_validas['criterio_pontuacao'] = ponderacao
                tabela_equipes_validas['periodo_codigo'] = periodos_dict[item]
                tabela_consolidada=tabela_equipes_validas

        tabela_consolidada.reset_index(drop=True, inplace=True)
        tabela_consolidada['id'] = tabela_consolidada.apply(lambda row:uuid.uuid4(), axis=1)
        tabela_consolidada['criacao_data'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        tabela_consolidada['atualizacao_data'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        tabela_consolidada.transform_column("periodo_id",function=lambda dt: periodo_por_data(sessao=sessao, data=dt).id,
            dest_column_name="periodo_id",
        )
        formatarTipo(tabela_consolidada,sessao=sessao)
        
    except Exception as e:
      print(e)
