import uuid
from datetime import datetime
import pandas as pd
from sqlalchemy.orm import Session
from impulsoetl.comum.datas import periodo_por_codigo,periodo_por_data
from impulsoetl.comum.geografias import id_sus_para_id_impulso
from datetime import date

def tratamento_dados(sessao: Session,dados_sisab_cadastros:pd.DataFrame,periodo:date,nivel_agregacao:str)->pd.DataFrame:

    tabela_consolidada = pd.DataFrame(columns=['municipio_id_sus','periodo_id','periodo_codigo','parametro'])
    periodo_cod = periodo_por_data(sessao=sessao, data=periodo)
    if nivel_agregacao == 'estabelecimentos_equipes':
        tabela_consolidada[['municipio_id_sus', 'cnes_id', 'cnes_nome', 'ine_id', 'parametro']] = dados_sisab_cadastros.loc[:, ['IBGE', 'CNES', 'Nome UBS', 'INE', 'parametro']]
        tabela_consolidada['cnes_id'] = tabela_consolidada['cnes_id'].astype('string')
        tabela_consolidada['cnes_nome'] = tabela_consolidada['cnes_nome'].astype('string')
        tabela_consolidada['ine_id'] = tabela_consolidada['ine_id'].astype('string')
    else:
        tabela_consolidada[['municipio_id_sus','parametro']] = dados_sisab_cadastros.loc[:, ['IBGE', 'parametro']]  
    tabela_consolidada['periodo_codigo'] = periodo_cod[3]
    tabela_consolidada.reset_index(drop=True, inplace=True)
    tabela_consolidada['criacao_data'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    tabela_consolidada['atualizacao_data'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    periodo = periodo_por_codigo(sessao=sessao, codigo=periodo_cod[3])
    tabela_consolidada["periodo_id"] = periodo.id
    tabela_consolidada["unidade_geografica_id"] = (
        tabela_consolidada["municipio_id_sus"].apply(lambda municipio_id_sus: id_sus_para_id_impulso( sessao=sessao,id_sus=municipio_id_sus))
    )


    tabela_consolidada['municipio_id_sus'] = tabela_consolidada['municipio_id_sus'].astype('string')
    tabela_consolidada['periodo_id'] = tabela_consolidada['periodo_id'].astype('string')
    tabela_consolidada['periodo_codigo'] = tabela_consolidada['periodo_codigo'].astype('string')
    tabela_consolidada['unidade_geografica_id'] = tabela_consolidada['unidade_geografica_id'].astype('string')
    tabela_consolidada['parametro'] = tabela_consolidada['parametro'].astype(int)
    tabela_consolidada['criacao_data'] = tabela_consolidada['criacao_data'].astype('string')
    tabela_consolidada['atualizacao_data'] = tabela_consolidada['atualizacao_data'].astype('string')

    return tabela_consolidada
