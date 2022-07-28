# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


import decimal
import math
import os
from datetime import datetime
from decimal import *
from pathlib import Path
from time import sleep

import auxiliares.bancodedados as bancodedados
import auxiliares.log as logging
import auxiliares.utilitario as utilitario
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from uuid6 import uuid7


def captura(quadrimestre):
    if os.getenv("IS_PROD")=='TRUE':
        # chrome_options to dockerfile
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--window-size=1420,1080")
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-gpu")
        dir_path = os.path.dirname(os.path.realpath(__file__))
        downloadpath = dir_path+"\\temporario\\"
        chrome_options.add_experimental_option("prefs", {"download.default_directory": downloadpath})
        driver = webdriver.Chrome(
            chrome_options=chrome_options
        )  # chromedriver é instalado via dockerfile
        sleep(10)
        driver.get("https://www.google.com/")
        print("Sucesso!")
        return True
    if os.getenv("IS_LOCAL")=='TRUE':
        # ser = Service('auxiliares/chromedriver.exe')
        options = Options()
        #options.headless = True
        dir_path = os.path.dirname(os.path.realpath(__file__))
        downloadpath = dir_path+"\\temporario\\"
        options.add_experimental_option("prefs", {"download.default_directory": downloadpath})
        # driver = webdriver.Chrome(service=ser, options=options)
        chromepath = "C:\\Users\\LowCost\\Documents\\IMPULSO\\etl-dados-publicos\\src\\impulsoprevine\\scripts\\auxiliares\\chromedriver.exe"
        driver = webdriver.Chrome(chromepath, options=options)
    sleep(5)
    driver.get("https://sisab.saude.gov.br/paginas/acessoRestrito/relatorio/federal/indicadores/indicadorPainel.xhtml")
    sleep(10)
    driver.find_elements(By.XPATH,"//option[@value='ibge']")[0].click()
    sleep(5)
    driver.find_elements(By.XPATH,"//option[@value='|HM|NC|']")[0].click()
    sleep(5)
    quadrimestre_text = "//option[@value='{}']".format(quadrimestre)
    quadrimestre_text = "//option[@value='202112']"
    if len(driver.find_elements(By.XPATH,quadrimestre_text))==0:
        logging.atualiza_mensagem((tabela+": PERIODO {} AINDA NÃO ENCONTRADO NO SITE, CAPTURA FINALIZADA 🔄 \n").format(quadrimestre))
        return False
    driver.find_elements(By.XPATH,quadrimestre_text)[0].click()
    sleep(5)
    driver.find_element(By.CLASS_NAME,'fa-download').click()
    sleep(5)
    driver.find_element(By.LINK_TEXT,'Csv').click()
    if len(driver.find_elements(By.XPATH,quadrimestre_text))>0:
        for i in [1, 2, 3, 4, 5, 6, 7]:
            text = "//option[@value='{}']".format(i)
            driver.find_elements(By.XPATH,text)[0].click()
            sleep(5)
            driver.find_elements(By.XPATH,quadrimestre_text)[0].click()
            sleep(5)
            driver.find_element(By.CLASS_NAME,'fa-download').click()
            sleep(5)
            driver.find_element(By.LINK_TEXT,'Csv').click()
            sleep(5)
        filelist = [f for f in os.listdir(downloadpath) ]
        for f in filelist:
            caminho = './temporario/{}'.format(f)
            if f != 'pagamento_aps.xls':
                df = pd.read_csv(caminho)
                df.columns = df.loc[9]
                df = df.iloc[10: , :]
                os.remove(os.path.join(downloadpath, f))
                df.to_csv(caminho)   
        return True
    else:
        logging.atualiza_mensagem((tabela+": CAPTURA FALHOU ⛔ \n"))
        return False

def converte_nota(row):
    if row.nota<10:
        return round(row.nota, 1)
    else:
        return 10.0

def tranforma(periodos):
    indicadores_resultados = pd.DataFrame(columns=['id','municipio_id_sus','periodo_id','indicadores_parametros_id','numerador','denominador_estimado','denominador_informado','denominador_utilizado_calculado','resultado_porcentagem','nota_calculado','resultado_porcentagem_calculado','nota_ponderada_calculado','isf_id','criacao_data','atualizacao_data'])
    indicadores_resultados_parcial = pd.DataFrame(columns=['id','municipio_id_sus','periodo_id','indicadores_parametros_id','numerador','denominador_estimado','denominador_informado','denominador_utilizado_calculado','resultado_porcentagem','nota_calculado','resultado_porcentagem_calculado','nota_ponderada_calculado','isf_id','criacao_data','atualizacao_data'])
    query = """
    SELECT id, nome, peso, meta
    FROM previnebrasil.indicadores_parametros
    where versao = (SELECT MAX(versao) FROM previnebrasil.indicadores_parametros);
    """
    indicadores_parametros = bancodedados.readQuery(query,'impulsogov-analitico')
    col = periodos.periodo_codigo[0][0:4] + ' ' + periodos.periodo_codigo[0][5:7] + ' (%)'
    indicadores_parametros_dict = {1:'Pré-Natal (6 consultas)',2:'Pré-Natal (Sífilis e HIV)',3:'Gestantes Saúde Bucal',4:'Cobertura Citopatológico',5:'Cobertura Polio e Penta',6:'Hipertensão (PA Aferida)',7:'Diabetes (Hemoglobina Glicada)'}
    for i in [1, 2, 3, 4, 5, 6, 7]:
        caminho = './temporario/paine-indicador ({}).csv'.format(i)
        df = pd.read_csv(caminho, sep=';', usecols=['IBGE', 'Numerador', 'Denominador Informado', 'Denominador Estimado', col])
        df = df[df['IBGE'].notnull()]
        df['IBGE'] = df['IBGE'].astype(int)
        df['denominador_utilizado_calculado'] = df[["Denominador Informado", "Denominador Estimado"]].max(axis=1)
        df['resultado_porcentagem_calculado'] = (((df['Numerador']/df['denominador_utilizado_calculado'])*100).fillna(0).replace(np.nan, 0).replace(math.inf, 0).round(1))
        df['nota'] = ((df['resultado_porcentagem_calculado']/(indicadores_parametros[indicadores_parametros.nome == indicadores_parametros_dict[i]].meta)[i-1])*10).round(1)
        df['nota_calculado'] = df.apply(lambda x: converte_nota(x), axis=1)
        df['nota_ponderada_calculado'] = (df['nota_calculado']*(indicadores_parametros[indicadores_parametros.nome == indicadores_parametros_dict[i]].peso)[i-1]).round(1)
        indicadores_resultados_parcial[['municipio_id_sus','numerador','denominador_informado','denominador_estimado','resultado_porcentagem', 'denominador_utilizado_calculado', 'resultado_porcentagem_calculado', 'nota_calculado', 'nota_ponderada_calculado']] = df[['IBGE', 'Numerador', 'Denominador Informado', 'Denominador Estimado', col,  'denominador_utilizado_calculado', 'resultado_porcentagem_calculado', 'nota_calculado', 'nota_ponderada_calculado']]
        indicadores_resultados_parcial['indicadores_parametros_id'] = (indicadores_parametros[indicadores_parametros.nome == indicadores_parametros_dict[i]].id)[i-1]
        indicadores_resultados = indicadores_resultados.append(indicadores_resultados_parcial)
    for municipio in indicadores_resultados['municipio_id_sus'].unique():
        for ind_id in indicadores_parametros.id.to_list():
            if not any(indicadores_resultados[indicadores_resultados['municipio_id_sus'] == municipio].indicadores_parametros_id == ind_id):
                indicadores_resultados = indicadores_resultados.append({'municipio_id_sus': municipio,'numerador':0,'denominador_informado':0,'denominador_estimado':0,'resultado_porcentagem':0.0,'denominador_utilizado_calculado':0,'resultado_porcentagem_calculado':0.0,'nota_calculado':0.0,'nota_ponderada_calculado':0.0,'indicadores_parametros_id':ind_id}, ignore_index=True)
    indicadores_resultados.periodo_id = periodos.id[0]
    indicadores_resultados.isf_id = np.NAN
    indicadores_resultados.criacao_data = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    indicadores_resultados.atualizacao_data = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    indicadores_resultados.id = indicadores_resultados.apply(lambda row:uuid7(), axis=1)
    indicadores_resultados = indicadores_resultados.reset_index()
    return indicadores_resultados

def testes(resultados, periodos_antigos_lista):
    TESTS = {
        "DF não é um pd.DataFrame": lambda df: isinstance(resultados, pd.DataFrame),
        "Menos que 5570 cidades": lambda df: len(resultados["municipio_id_sus"].unique()) <= 5570,
        "Mais que 5500 cidades": lambda df: len(resultados["municipio_id_sus"].unique()) > 5500,
        "Dados de numerador, indicador e nota não negativos": lambda df: len(resultados.query("numerador < 0 | denominador_estimado < 0 | denominador_informado < 0 | denominador_utilizado_calculado < 0 | resultado_porcentagem < 0 | nota_calculado < 0 | resultado_porcentagem_calculado < 0")) == 0,
        "Período Novo": lambda df: resultados["periodo_id"].unique() not in periodos_antigos_lista
    }
    results = [v(resultados) for k, v in TESTS.items()]
    if not all(results):
        return False
    return True
    
def insere_atualiza(resultados, quadrimestre):
    resultados = resultados.copy()
    resultados = resultados[['id', 'periodo_id', 'numerador', 'denominador_estimado','denominador_informado','denominador_utilizado_calculado', 'resultado_porcentagem', 'resultado_porcentagem_calculado','nota_ponderada_calculado','isf_id','criacao_data', 'atualizacao_data','indicadores_parametros_id','municipio_id_sus', 'nota_calculado']]
    tuple_list = [tuple(x) for x in resultados.to_records(index=False)]
    chunks = [tuple_list[x:x+800000] for x in range(0, len(tuple_list), 800000)]
    for chunk in chunks:
        query = """
        INSERT INTO previnebrasil.indicadores_resultados
        (id, periodo_id, numerador, denominador_estimado, denominador_informado, denominador_utilizado_calculado, resultado_porcentagem, resultado_porcentagem_calculado, nota_ponderada_calculado, isf_id, criacao_data, atualizacao_data, indicadores_parametros_id, municipio_id_sus, nota_calculado)
        VALUES{};
        """.format(str(chunk).replace('%', '%%').replace("[","").replace("]","").replace(",u'", ",'").replace(", u'", ",'").replace("1L", "1").replace("0L", "0").replace("2L", "2").replace("3L", "3").replace("4L", "4").replace("5L", "5").replace("6L", "6").replace("7L", "7").replace("8L", "8").replace("9L", "9").replace(", nan)", ", NULL)").replace("None,", "NULL,").replace("nan,", "NULL,").replace("(u'","('"))
        bancodedados.executeQuery(query,'impulsogov-analitico')
    utilitario.update_quadrimestre('indicadores_resultados', quadrimestre, 'impulsogov-analitico')
    temppath = "C:\\Users\\LowCost\\Documents\\IMPULSO\\etl-dados-publicos\\src\\impulsoprevine\\scripts\\temporario"
    utilitario.limpa_temporario(temppath)
    return True



tabela = "*CADASTROS_INDIVIDUAIS*"
logging.atualiza_mensagem((tabela+": INICIOU ⌛"))
query_periodos = """
    SELECT tabela, "data", periodo_codigo, id
    FROM previnebrasil.parametro_datacaptura
    JOIN dadospublicos.periodos ON periodo_codigo = codigo
    WHERE tabela = 'indicadores_resultados';
    """
periodos = bancodedados.readQuery(query_periodos,'impulsogov-analitico')
query_periodos_antigos = """
    SELECT DISTINCT periodo_id
    FROM previnebrasil.indicadores_resultados;
    """
periodos_antigos = bancodedados.readQuery(query_periodos_antigos,'impulsogov-analitico')
if captura(periodos.data[0]):
    logging.atualiza_mensagem((tabela+": CAPTUROU DADOS ⌛"))
    resultados = tranforma(periodos)
    logging.atualiza_mensagem((tabela+": TRATAMENTO DE DADOS REALIZADO ⌛"))
    if testes(resultados, periodos_antigos.periodo_id.to_list()):
        logging.atualiza_mensagem((tabela+": TESTES REALIZADOS ⌛"))
        insere_atualiza(resultados,periodos.data[0])
        logging.atualiza_mensagem((tabela+": DADOS INSERIDOS ✔️ \n"))
