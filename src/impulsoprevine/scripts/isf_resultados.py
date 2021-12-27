import pandas as pd
import numpy as np
from selenium import webdriver
from time import sleep
from bs4 import BeautifulSoup
from pathlib import Path
import os
import uuid
import auxiliares.bancodedados as bancodedados
import auxiliares.utilitario as utilitario
from datetime import datetime

from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By



def calcula_isf(row, id, indicadores_resultados):
    indicadores_resultados = indicadores_resultados[indicadores_resultados.municipio_id_sus == row.municipio_id_sus]
    if sum(indicadores_resultados.peso) == 0:
        return 0.00
    isf = sum(indicadores_resultados.nota_ponderada_calculado)/sum(indicadores_resultados.peso)
    return round(isf, 2)

def atualiza_retroativo(resultados, periodo_id):
    for linha in resultados.index:
        query = """
        UPDATE previnebrasil.indicadores_resultados
        SET isf_id='{}', atualizacao_data='{}'
        WHERE municipio_id_sus={} and periodo_id='{}';
        """.format(resultados.loc[linha].id,datetime.now().strftime("%Y-%m-%d %H:%M:%S"),resultados.loc[linha].municipio_id_sus,periodo_id)
        bancodedados.executeQuery(query,'impulsogov-analitico')
    return True

def captura(quadrimestre):
    ser = Service('auxiliares/chromedriver.exe')
    options = Options()
    #options.headless = True
    dir_path = os.path.dirname(os.path.realpath(__file__))
    downloadpath = dir_path+"\\temporario\\"
    options.add_experimental_option("prefs", {"download.default_directory": downloadpath})
    driver = webdriver.Chrome(service=ser, options=options)
    sleep(10)
    driver.get("https://egestorab.saude.gov.br/gestaoaps/relFinanciamento.xhtml")
    sleep(10)
    driver.find_elements_by_xpath("//option[@value='00']")[0].click()
    sleep(5)
    driver.find_elements_by_xpath("//option[@value='{}']".format(quadrimestre[0:4]))[0].click()
    sleep(5)
    driver.find_elements_by_xpath("//option[@value='{}']".format(quadrimestre))[0].click()
    sleep(5)
    driver.find_element_by_class_name('fa-download').click()
    sleep(5)
    return True

def tranforma(periodos):
    isf_resultados = pd.DataFrame(columns=['id','periodo_id','isf_declarado','isf_calculado','isf_regras_id','criacao_data','atualizacao_data','municipio_id_sus'])
    caminho = './temporario/pagamento_aps.xls'
    df = pd.read_excel(caminho, sheet_name='Desempenho ISF')
    df.columns = df.loc[2]
    df = df.iloc[4: , :]
    df = df[['IBGE','Quadrimestre de Referência','Nota do ISF']]
    df['IBGE'] = df['IBGE'].astype(int)
    query = """
    SELECT id, versao
    FROM previnebrasil.isf_regras;
    """
    isf_regras = bancodedados.readQuery(query,'impulsogov-analitico')
    isf_resultados.municipio_id_sus = df['IBGE']
    isf_resultados.isf_declarado = df['Nota do ISF']
    query = """
    SELECT municipio_id_sus, indicadores_parametros_id, nota_ponderada_calculado, p.peso
    FROM previnebrasil.indicadores_resultados
    JOIN previnebrasil.indicadores_parametros as p ON p.id = indicadores_parametros_id
    WHERE periodo_id='{}';
    """.format(periodos.id[0])
    indicadores_resultados = bancodedados.readQuery(query,'impulsogov-analitico')
    isf_resultados.isf_calculado = isf_resultados.apply(lambda x: calcula_isf(x, periodos.id[0], indicadores_resultados), axis=1)
    isf_resultados.isf_regras_id = (isf_regras.id)[0]
    isf_resultados.id = isf_resultados.apply(lambda row : uuid.uuid4(), axis = 1)
    isf_resultados.periodo_id = periodos.id[0]
    isf_resultados.criacao_data = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    isf_resultados.atualizacao_data = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    isf_resultados = isf_resultados.reset_index()
    return isf_resultados

def testes(resultados, periodos_antigos_lista):
    TESTS = {
        "DF não é um pd.DataFrame": lambda df: isinstance(resultados, pd.DataFrame),
        "Menos que 5570 cidades": lambda df: len(resultados["municipio_id_sus"].unique()) <= 5570,
        "Mais que 5500 cidades": lambda df: len(resultados["municipio_id_sus"].unique()) > 5500,
        "Dados ISF não negativos": lambda df: len(resultados.query("isf_declarado < 0 | isf_calculado < 0")) == 0,
        "Período Novo": lambda df: resultados["periodo_id"].unique() not in periodos_antigos_lista
    }
    results = [v(resultados) for k, v in TESTS.items()]
    if not all(results):
        return False
    return True

def insere_atualiza(resultados,quadrimestre,periodos):
    resultados = resultados[['id','periodo_id','isf_declarado','isf_calculado','isf_regras_id','criacao_data','atualizacao_data','municipio_id_sus']]
    tuple_list = [tuple(x) for x in resultados.to_records(index=False)]
    chunks = [tuple_list[x:x+800000] for x in range(0, len(tuple_list), 800000)]
    for chunk in chunks:
        query = """
        INSERT INTO previnebrasil.isf_resultados
        (id, periodo_id, isf_declarado, isf_calculado, isf_regras_id, criacao_data, atualizacao_data, municipio_id_sus)
        VALUES{};
        """.format(str(chunk).replace('%', '%%').replace("[","").replace("]","").replace(",u'", ",'").replace(", u'", ",'").replace("1L", "1").replace("0L", "0").replace("2L", "2").replace("3L", "3").replace("4L", "4").replace("5L", "5").replace("6L", "6").replace("7L", "7").replace("8L", "8").replace("9L", "9").replace(", nan)", ", NULL)").replace("None,", "NULL,").replace("nan,", "NULL,").replace("(u'","('"))
        bancodedados.executeQuery(query,'impulsogov-analitico')
    # atualiza_retroativo(resultados, periodos.id[0])
    utilitario.update_quadrimestre('isf_resultados', quadrimestre, 'impulsogov-analitico')
    temppath = "./src/impulsoprevine/scripts/temporario"
    utilitario.limpa_temporario(temppath)
    return True


def main():
    query_periodos = """
        SELECT tabela, "data", periodo_codigo, id
        FROM previnebrasil.parametro_datacaptura
        JOIN dadospublicos.periodos ON periodo_codigo = codigo
        WHERE tabela = 'isf_resultados';
        """
    periodos = bancodedados.readQuery(query_periodos,'impulsogov-analitico')
    query_periodos_antigos = """
        SELECT DISTINCT periodo_id
        FROM previnebrasil.isf_resultados;
        """
    periodos_antigos = bancodedados.readQuery(query_periodos_antigos,'impulsogov-analitico')
    if captura(periodos.data[0]):
        resultados = tranforma(periodos)
        if testes(resultados, periodos_antigos.periodo_id.to_list()):
            insere_atualiza(resultados,periodos.data[0],periodos)
    return True
