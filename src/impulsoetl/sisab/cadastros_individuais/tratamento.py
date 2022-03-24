import uuid
from datetime import datetime
import pandas as pd
from sqlalchemy.orm import Session
from impulsoetl.comum.datas import periodo_por_codigo
from impulsoetl.comum.geografias import id_sus_para_id_impulso


periodos_dict = {
            '202201':'2022.M1'}


def formatarTipo(tabela_consolidada):
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
    return tabela_consolidada


def tratamentoDados(dados_sisab_cadastros,visao_equipe,ponderacao,periodo,sessao: Session):
    tabela_consolidada = pd.DataFrame(columns=['id','municipio_id_sus','periodo_id','periodo_codigo','cnes_id','cnes_nome','ine_id','quantidades','criterio_pontuacao','criacao_data','atualizacao_data'])
    for item in periodo:
        if visao_equipe == 'equipes-validas':
            tabela_equipes_validas = pd.DataFrame(columns=[
                                                'municipio_id_sus',
                                                'periodo_codigo',
                                                'cnes_id',
                                                'cnes_nome',
                                                'ine_id',
                                                'quantidade',
                                                'criterio_pontuacao',
                                                'periodo_id'])
            tabela_equipes_validas[['municipio_id_sus', 'cnes_id', 'cnes_nome', 'ine_id', 'quantidade']] = dados_sisab_cadastros.loc[:, ['IBGE', 'CNES', 'Nome UBS', 'INE', periodos_dict[item]]]  
            tabela_equipes_validas['criterio_pontuacao'] = ponderacao
            tabela_equipes_validas['periodo_codigo'] = periodos_dict[item]
            tabela_consolidada=tabela_equipes_validas

    tabela_consolidada.reset_index(drop=True, inplace=True)
    tabela_consolidada['id'] = tabela_consolidada.apply(lambda row:uuid.uuid4(), axis=1)
    tabela_consolidada['criacao_data'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    tabela_consolidada['atualizacao_data'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    periodo = periodo_por_codigo(sessao=sessao, codigo=periodos_dict[item])
    tabela_consolidada["periodo_id"] = periodo.id
    tabela_equipes_validas["unidade_geografica_id"] = (
        tabela_equipes_validas["municipio_id_sus"]
        .apply(
            lambda municipio_id_sus: id_sus_para_id_impulso(
                sessao=sessao,
                id_sus=municipio_id_sus,
            )
        )
    )
    return formatarTipo(tabela_consolidada)
