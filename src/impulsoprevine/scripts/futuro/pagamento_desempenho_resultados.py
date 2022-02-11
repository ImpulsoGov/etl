# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


import pandas as pd
import numpy as np
from selenium import webdriver
from time import sleep
from bs4 import BeautifulSoup
from pathlib import Path
import os
import uuid
import bancodedados
import utilitario
from datetime import datetime
import math


def atualiza_retroativo(resultados, periodo_id):
    for linha in resultados.index:
        query = """
        UPDATE previnebrasil.componente_pagamento_desempenho_resultados
        SET pagamento_desempenho_resultados_id='{}', atualizacao_data='{}'
        WHERE municipio_id_sus={} and periodo_id='{}';
        """.format(resultados.loc[linha].id,datetime.now().strftime("%Y-%m-%d %H:%M:%S"),resultados.loc[linha].municipio_id_sus,periodo_id)
        bancodedados.executeQuery(query,'local')
    return True

def captura(mes):
    return True
    
def calcula(row,pagamento_categoria,resultados):
    resultados = resultados[resultados.municipio_id_sus == int(row)]
    if pagamento_categoria == 'Adicional':
        return resultados[resultados.pagamento_categoria == ('Adicional')].valor_calculado.sum()
    elif pagamento_categoria == 'Pagamento Equipes Novas':
        return resultados[resultados.pagamento_categoria == ('eSF Nova','eAP 20h Nova','eAP 30h Nova')].valor_calculado.sum()
    elif pagamento_categoria == 'Pagamento Desempenho':
        return resultados[resultados.pagamento_categoria == ('eSF','eAP 20h','eAP 30h')].valor_calculado.sum()
    else:
        return resultados.valor_calculado.sum()

def tranforma(periodos):
    pagamento_resultado = pd.DataFrame(columns=['id', 'periodo_id', 'pagamento_valor', 'criacao_data', 'atualizacao_data', 'municipio_id_sus', 'pagamento_valor_calculado', 'pagamento_desempenho_regras_id'])
    pagamento_resultado_parcial = pd.DataFrame(columns=['id', 'periodo_id', 'pagamento_valor', 'criacao_data', 'atualizacao_data', 'municipio_id_sus', 'pagamento_valor_calculado', 'pagamento_desempenho_regras_id'])
    caminho = './temporario/pagamento_aps.xls'
    df = pd.read_excel(caminho, sheet_name='Desempenho ISF')
    df.columns = df.loc[2]
    df = df.iloc[4: , :]
    df = df[[
        'IBGE',
        'Valor do pagamento por desempenho - ISF',
        'Valor  referente a 100% dos indicadores - Portaria nº 166, de 27 de janeiro de 2021',
        'VALOR PAGAMENTO POR DESEMPENHO - EQUIPES NOVAS*',
        'VALOR TOTAL']].rename(columns={"Valor do pagamento por desempenho - ISF":"Pagamento Desempenho",
        "Valor  referente a 100% dos indicadores - Portaria nº 166, de 27 de janeiro de 2021": "Adicional", 
        "VALOR PAGAMENTO POR DESEMPENHO - EQUIPES NOVAS*": "Pagamento Equipes Novas", 
        "VALOR TOTAL": "Valor Total"})
    df['Pagamento Desempenho'] = df['Pagamento Desempenho'].apply(lambda x: float(x.replace('R$ ', '').replace('.', '').replace(',', '.')))
    df['Adicional'] = df['Adicional'].apply(lambda x: float(x.replace('R$ ', '').replace('.', '').replace(',', '.')))
    df['Pagamento Equipes Novas'] = df['Pagamento Equipes Novas'].apply(lambda x: float(x.replace('R$ ', '').replace('.', '').replace(',', '.')))
    df['Valor Total'] = df['Valor Total'].apply(lambda x: float(x.replace('R$ ', '').replace('.', '').replace(',', '.')))
    query = """
    SELECT id, pagamento_categoria, equipes_parametros_id
    FROM previnebrasil.pagamento_desempenho_regras
    WHERE pagamento_categoria in ('Adicional','Pagamento Desempenho','Pagamento Equipes Novas') and versao = (SELECT  MAX(versao) from previnebrasil.pagamento_desempenho_regras);
    """
    pagamento_regras = bancodedados.readQuery(query,'local')
    query = """
    SELECT municipio_id_sus, componente_pagamento_desempenho_regras_id, pagamento_categoria, valor_calculado
    FROM previnebrasil.componente_pagamento_desempenho_resultados
    JOIN previnebrasil.componente_pagamento_desempenho_regras cpdr ON cpdr.id = componente_pagamento_desempenho_regras_id 
    WHERE periodo_id='{}';
    """.format(periodos.id[0])
    resultados = bancodedados.readQuery(query,'local')
    for i in pagamento_regras.index:
        pagamento_resultado_parcial.municipio_id_sus = df['IBGE'].astype(int)
        pagamento_resultado_parcial.pagamento_valor = df[pagamento_regras.loc[i].pagamento_categoria]
        pagamento_resultado_parcial.pagamento_valor_calculado = df['IBGE'].apply(lambda row: calcula(row, pagamento_regras.loc[i].pagamento_categoria, resultados))
        pagamento_resultado_parcial.pagamento_desempenho_regras_id = pagamento_regras.loc[i].id
        pagamento_resultado = pagamento_resultado.append(pagamento_resultado_parcial)
    pagamento_resultado.periodo_id = periodos.id[0]
    pagamento_resultado.criacao_data = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    pagamento_resultado.atualizacao_data = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    pagamento_resultado.id = pagamento_resultado.apply(lambda row : uuid.uuid4(), axis = 1)
    pagamento_resultado = pagamento_resultado.reset_index()
    return pagamento_resultado

def testes(resultados, periodos_antigos_lista):
    TESTS = {
        "DF não é um pd.DataFrame": lambda df: isinstance(resultados, pd.DataFrame),
        "Menos que 5570 cidades": lambda df: len(resultados["municipio_id_sus"].unique()) <= 5570,
        "Mais que 5500 cidades": lambda df: len(resultados["municipio_id_sus"].unique()) > 5500,
        "Valor a receber não negativos": lambda df: len(resultados.query("pagamento_valor < 0")) == 0,
        "Período Novo": lambda df: resultados["periodo_id"].unique() not in periodos_antigos_lista
    }
    results = [v(resultados) for k, v in TESTS.items()]
    if not all(results):
        return False
    return True
    
def insere_atualiza(resultados, periodos):
    resultados = resultados[['id', 'periodo_id', 'pagamento_valor', 'criacao_data', 'atualizacao_data', 'municipio_id_sus', 'pagamento_valor_calculado', 'pagamento_desempenho_regras_id']]
    tuple_list = [tuple(x) for x in resultados.to_records(index=False)]
    chunks = [tuple_list[x:x+800000] for x in range(0, len(tuple_list), 800000)]
    for chunk in chunks:
        query = """
        INSERT INTO previnebrasil.pagamento_desempenho_resultados
        (id, periodo_id, pagamento_valor, criacao_data, atualizacao_data, municipio_id_sus, pagamento_valor_calculado, pagamento_desempenho_regras_id)
        VALUES{};
        """.format(str(chunk).replace('%', '%%').replace("[","").replace("]","").replace(",u'", ",'").replace(", u'", ",'").replace("1L", "1").replace("0L", "0").replace("2L", "2").replace("3L", "3").replace("4L", "4").replace("5L", "5").replace("6L", "6").replace("7L", "7").replace("8L", "8").replace("9L", "9").replace(", nan)", ", NULL)").replace("None,", "NULL,").replace("nan,", "NULL,").replace("(u'","('"))
        # bancodedados.executeQuery(query,'local')
    atualiza_retroativo(resultados, periodos.id[0])
    utilitario.update_mes('pagamento_desempenho_resultados', periodos.data[0])
    # downloadpath = "C:\\Users\\LowCost\\Documents\\IMPULSO\\etl-dados-publicos\\src\\impulso-previne\\temporario"
    # filelist = [f for f in os.listdir(downloadpath) ]
    # for f in filelist:
    #         caminho = './temporario/{}'.format(f)
    #         os.remove(os.path.join(downloadpath, f))
    return True

query = """
SELECT tabela, "data", periodo_codigo, id
FROM previnebrasil.parametro_datacaptura
JOIN dadospublicos.periodos ON periodo_codigo = codigo
WHERE tabela = 'pagamento_desempenho_resultados';
"""
periodos = bancodedados.readQuery(query,'local')
query = """
SELECT DISTINCT periodo_id
FROM previnebrasil.pagamento_desempenho_resultados;
"""
periodos_antigos = bancodedados.readQuery(query,'local')

if captura(periodos.data[0]):
    resultados = tranforma(periodos)
    if testes(resultados, periodos_antigos.periodo_id.to_list()):
        insere_atualiza(resultados,periodos)
