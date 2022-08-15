# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


import os
from datetime import datetime
from pathlib import Path
from time import sleep

import bancodedados
import numpy as np
import pandas as pd
import utilitario
from bs4 import BeautifulSoup
from selenium import webdriver
from uuid6 import uuid7


def captura(anomes):
    options = webdriver.ChromeOptions()
    options.binary_location = "C:\Program Files\Google\Chrome\Application\chrome.exe"
    downloadpath = "C:\\Users\\LowCost\\Documents\\IMPULSO\\etl-dados-publicos\\src\\impulso-previne\\temporario"
    options.add_experimental_option("prefs", {"download.default_directory": downloadpath})
    chromepath = Path() / "chromedriver.exe"
    driver = webdriver.Chrome(chromepath, chrome_options=options)
    sleep(10)
    driver.get("https://egestorab.saude.gov.br/gestaoaps/relFinanciamento.xhtml")
    sleep(10)
    driver.find_elements_by_xpath("//option[@value='00']")[0].click()
    sleep(10)
    driver.find_elements_by_xpath("//option[@value='{}']".format(anomes[0:4]))[0].click()
    sleep(10)
    driver.find_elements_by_xpath("//option[@value='{}']".format(anomes))[0].click()
    sleep(10)
    driver.find_element_by_class_name('fa-download').click()
    sleep(10)
    return True

def tranforma(periodos):
    equipes_resultados = pd.DataFrame(columns=['id', 'periodo_id', 'quantidade', 'equipes_parametro_id', 'criacao_data', 'atualizacao_data', 'municipio_id_sus'])
    equipes_parcial = pd.DataFrame(columns=['id', 'periodo_id', 'quantidade', 'equipes_parametro_id', 'criacao_data', 'atualizacao_data', 'municipio_id_sus'])
    caminho = './temporario/pagamento_aps.xls'
    df = pd.read_excel(caminho, sheet_name='Desempenho ISF')
    df.columns = df.loc[3]
    df = df.iloc[4: , :]
    df = df.iloc[:, [3,7,8,9,12,13,14]]
    df.columns = ['municipio_id_sus', 'eSF', 'eAP 30h', 'eAP 20h', 'eSF Nova', 'eAP 30h Nova', 'eAP 20h Nova']
    df['municipio_id_sus'] = df['municipio_id_sus'].astype(int)
    query = """
    SELECT id, equipes_tipo, valor
    FROM previnebrasil.equipes_parametros;
    """
    equipes_parametro = bancodedados.readQuery(query,'impulsogov-analitico')
    for i in equipes_parametro.index:
        equipes_parcial.quantidade = df[equipes_parametro[equipes_parametro.id == equipes_parametro.loc[i].id].equipes_tipo[i]]
        equipes_parcial.equipes_parametro_id = equipes_parametro.loc[i].id
        equipes_parcial.municipio_id_sus = df.municipio_id_sus
        equipes_resultados = equipes_resultados.append(equipes_parcial)
    equipes_resultados = equipes_resultados.reset_index()
    equipes_resultados.id = equipes_resultados.apply(lambda row : uuid7(), axis = 1)
    equipes_resultados.periodo_id = periodos.id[0]
    equipes_resultados.criacao_data = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    equipes_resultados.atualizacao_data = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return equipes_resultados

def testes(resultados, periodos_antigos_lista):
    TESTS = {
        "DF não é um pd.DataFrame": lambda df: isinstance(resultados, pd.DataFrame),
        "Menos que 5570 cidades": lambda df: len(resultados["municipio_id_sus"].unique()) <= 5570,
        "Mais que 5500 cidades": lambda df: len(resultados["municipio_id_sus"].unique()) > 5500,
        "Quantidade não negativos": lambda df: len(resultados.query("quantidade < 0")) == 0,
        "Período Novo": lambda df: resultados["periodo_id"].unique() not in periodos_antigos_lista
    }
    results = [v(resultados) for k, v in TESTS.items()]
    if not all(results):
        return False
    return True

def insere_atualiza(resultados,mes,periodos):
    resultados = resultados[['id', 'periodo_id', 'quantidade', 'equipes_parametro_id', 'criacao_data', 'atualizacao_data', 'municipio_id_sus']]
    tuple_list = [tuple(x) for x in resultados.to_records(index=False)]
    chunks = [tuple_list[x:x+800000] for x in range(0, len(tuple_list), 800000)]
    for chunk in chunks:
        query = """
        INSERT INTO previnebrasil.equipes_quantidade_resultados
        (id, periodo_id, quantidade, equipes_parametro_id, criacao_data, atualizacao_data, municipio_id_sus)
        VALUES{};
        """.format(str(chunk).replace('%', '%%').replace("[","").replace("]","").replace(",u'", ",'").replace(", u'", ",'").replace("1L", "1").replace("0L", "0").replace("2L", "2").replace("3L", "3").replace("4L", "4").replace("5L", "5").replace("6L", "6").replace("7L", "7").replace("8L", "8").replace("9L", "9").replace(", nan)", ", NULL)").replace("None,", "NULL,").replace("nan,", "NULL,").replace("(u'","('"))
        bancodedados.executeQuery(query,'impulsogov-analitico')
    # utilitario.update_mes('equipes_quantidade_resultados', mes, 'impulsogov-analitico')
    return True

query = """
SELECT tabela, "data", periodo_codigo, id
FROM previnebrasil.parametro_datacaptura
JOIN dadospublicos.periodos ON periodo_codigo = codigo
WHERE tabela = 'equipes_quantidade_resultados';
"""
periodos = bancodedados.readQuery(query,'impulsogov-analitico')
query = """
SELECT DISTINCT periodo_id
FROM previnebrasil.equipes_quantidade_resultados;
"""
periodos_antigos = bancodedados.readQuery(query,'impulsogov-analitico')

# if captura(periodos.data[0]):
#     resultados = tranforma(periodos)
#     if testes(resultados, periodos_antigos.periodo_id.to_list()):
#         insere_atualiza(resultados,periodos.data[0],periodos)

resultados = tranforma(periodos)
if testes(resultados, periodos_antigos.periodo_id.to_list()):
    insere_atualiza(resultados,periodos.data[0],periodos)