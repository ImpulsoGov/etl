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

def captura(mes):
    return True
    
def EquipeAntiga(row, resultados):
    return resultados[resultados.municipio_id_sus == row.IBGE].valor.to_list()[0]*resultados[resultados.municipio_id_sus == row.IBGE].quantidade.to_list()[0]*resultados[resultados.municipio_id_sus == row.IBGE].isf_declarado.to_list()[0]

def EquipeNova(row, resultados):
    return resultados[resultados.municipio_id_sus == row.IBGE].valor.to_list()[0]*resultados[resultados.municipio_id_sus == row.IBGE].quantidade.to_list()[0]

def Adicional(row, resultados):
    return resultados[resultados.municipio_id_sus == row.IBGE].valor.to_list()[0]*resultados[resultados.municipio_id_sus == row.IBGE].quantidade.to_list()[0]*(10-resultados[resultados.municipio_id_sus == row.IBGE].isf_declarado.to_list()[0])

def IsfID(row, resultados):
    if len(resultados[resultados.municipio_id_sus == row.IBGE].id.to_list())<=0:
        return np.NAN
    return resultados[resultados.municipio_id_sus == row.IBGE].id.to_list()[0]

def calcula(df,resultados,componente_regra_linha):
    if componente_regra_linha['pagamento_categoria'] == 'Adicional':
        df['valor_calculado'] = df.apply(lambda x: Adicional(x, resultados), axis=1)
    elif componente_regra_linha['pagamento_categoria'] in ['eSF', 'eAP 20h', 'eAP 30h']:
        df['valor_calculado'] = df.apply(lambda x: EquipeAntiga(x, resultados[resultados.equipes_tipo == componente_regra_linha['pagamento_categoria']]), axis=1)
    elif componente_regra_linha['pagamento_categoria'] in ['eSF Nova', 'eAP 20h Nova', 'eAP 30h Nova']:
        df['valor_calculado'] = df.apply(lambda x: EquipeNova(x, resultados[resultados.equipes_tipo == componente_regra_linha['pagamento_categoria']]), axis=1)
    df['isf_id'] = df.apply(lambda x: IsfID(x, resultados[resultados.equipes_tipo == componente_regra_linha['pagamento_categoria']]), axis=1)
    return df['IBGE'],df['valor_calculado'],df['isf_id'],componente_regra_linha['id'],componente_regra_linha['equipes_parametros_id']

def tranforma(periodos):
    componente_pagamento_resultado = pd.DataFrame(columns=['id', 'periodo_id', 'valor_calculado', 'equipes_quantidade_id', 'criacao_data', 'atualizacao_data', 'municipio_id_sus', 'isf_id', 'componente_pagamento_desempenho_regras_id', 'pagamento_desempenho_resultados_id'])
    componente_pagamento_resultado_parcial = pd.DataFrame(columns=['id', 'periodo_id', 'valor_calculado', 'equipes_quantidade_id', 'criacao_data', 'atualizacao_data', 'municipio_id_sus', 'isf_id', 'componente_pagamento_desempenho_regras_id', 'pagamento_desempenho_resultados_id'])
    caminho = './temporario/pagamento_aps.xls'
    df = pd.read_excel(caminho, sheet_name='Desempenho ISF')
    df.columns = df.loc[2]
    df = df.iloc[4: , :]
    df = df[['IBGE','Valor do pagamento por desempenho - ISF','Valor  referente a 100% dos indicadores - Portaria nº 166, de 27 de janeiro de 2021','VALOR PAGAMENTO POR DESEMPENHO - EQUIPES NOVAS*','VALOR TOTAL']]
    df['IBGE'] = df['IBGE'].astype(int)
    query = """
    SELECT id, pagamento_categoria, equipes_parametros_id
    FROM previnebrasil.componente_pagamento_desempenho_regras
    WHERE pagamento_categoria in ('eSF', 'eAP 20h', 'eAP 30h', 'eSF Nova', 'eAP 20h Nova','eAP 30h Nova','Adicional')
    and versao = (SELECT MAX(versao) FROM previnebrasil.componente_pagamento_desempenho_regras);
    """
    componente_regras = bancodedados.readQuery(query,'local')
    query = """
    SELECT eqr.municipio_id_sus, periodos.codigo, ep.equipes_tipo, ep.valor, eqr.quantidade, isf.isf_declarado, isf.id
    FROM previnebrasil.equipes_quantidade_resultados as eqr
    JOIN previnebrasil.equipes_parametros AS ep on (ep.id = eqr.equipes_parametro_id)
    JOIN previnebrasil.isf_resultados AS isf on (isf.municipio_id_sus = eqr.municipio_id_sus)
    JOIN dadospublicos.periodos AS periodos on (periodos.id = eqr.periodo_id)
    WHERE eqr.periodo_id='{}';
    """.format(periodos.id[0])
    resultados = bancodedados.readQuery(query,'local')
    for i in componente_regras.index:
        componente_pagamento_resultado_parcial.municipio_id_sus,componente_pagamento_resultado_parcial.valor_calculado,componente_pagamento_resultado_parcial.isf_id,componente_pagamento_resultado_parcial.componente_pagamento_desempenho_regras_id,componente_pagamento_resultado_parcial.equipes_quantidade_id = calcula(df,resultados,componente_regras.loc[i])
        componente_pagamento_resultado = componente_pagamento_resultado.append(componente_pagamento_resultado_parcial)
    componente_pagamento_resultado.pagamento_desempenho_resultados_id = np.NAN
    componente_pagamento_resultado.periodo_id = periodos.id[0]
    componente_pagamento_resultado.id = componente_pagamento_resultado.apply(lambda row : uuid.uuid4(), axis = 1)
    componente_pagamento_resultado.criacao_data = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    componente_pagamento_resultado.atualizacao_data = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return componente_pagamento_resultado

def testes(resultados, periodos_antigos_lista):
    TESTS = {
        "DF não é um pd.DataFrame": lambda df: isinstance(resultados, pd.DataFrame),
        "Menos que 5570 cidades": lambda df: len(resultados["municipio_id_sus"].unique()) <= 5570,
        "Mais que 5500 cidades": lambda df: len(resultados["municipio_id_sus"].unique()) > 5500,
        "Valor a receber não negativos": lambda df: len(resultados.query("valor_calculado < 0")) == 0,
        "Período Novo": lambda df: resultados["periodo_id"].unique() not in periodos_antigos_lista
    }
    results = [v(resultados) for k, v in TESTS.items()]
    if not all(results):
        return False
    return True
    
def insere_atualiza(resultados, periodos):
    resultados = resultados[['id', 'periodo_id', 'valor_calculado', 'equipes_quantidade_id', 'criacao_data', 'atualizacao_data', 'municipio_id_sus', 'isf_id', 'componente_pagamento_desempenho_regras_id', 'pagamento_desempenho_resultados_id']]
    tuple_list = [tuple(x) for x in resultados.to_records(index=False)]
    chunks = [tuple_list[x:x+800000] for x in range(0, len(tuple_list), 800000)]
    for chunk in chunks:
        query = """
        INSERT INTO previnebrasil.componente_pagamento_desempenho_resultados
        (id, periodo_id, valor_calculado, equipes_quantidade_id, criacao_data, atualizacao_data, municipio_id_sus, isf_id, componente_pagamento_desempenho_regras_id, pagamento_desempenho_resultados_id)
        VALUES{};
        """.format(str(chunk).replace('%', '%%').replace("[","").replace("]","").replace(",u'", ",'").replace(", u'", ",'").replace("1L", "1").replace("0L", "0").replace("2L", "2").replace("3L", "3").replace("4L", "4").replace("5L", "5").replace("6L", "6").replace("7L", "7").replace("8L", "8").replace("9L", "9").replace(", nan)", ", NULL)").replace("None,", "NULL,").replace("nan,", "NULL,").replace("(u'","('"))
        bancodedados.executeQuery(query,'local')
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
FROM previnebrasil.componente_pagamento_desempenho_resultados;
"""
periodos_antigos = bancodedados.readQuery(query,'local')

# if captura(periodos.data[0]):
#     resultados = tranforma(periodos)
#     if testes(resultados, periodos_antigos.periodo_id.to_list()):
#         insere_atualiza(resultados,periodos.data[0])

resultados = tranforma(periodos)
if testes(resultados, periodos_antigos.periodo_id.to_list()):
    insere_atualiza(resultados,periodos.data[0])