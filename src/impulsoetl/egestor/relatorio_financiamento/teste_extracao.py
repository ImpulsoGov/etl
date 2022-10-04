""" Extrai relatório de financiamento a partir do egestor"""
import time
from time import sleep
from asyncio.windows_events import NULL

from webdriver_manager.chrome import ChromeDriverManager
from selenium import webdriver
from selenium.webdriver.support.select import Select
from selenium.webdriver.common.by import By
from selenium_driver_updater import DriverUpdater

import pandas as pd

import sys
sys.path.append(r'C:\Users\maira\Impulso\etl\src')
from impulsoetl.navegadores import listar_downloads

def extracao(periodo):
    # Cria um um WebDriver 
    driver = webdriver.Chrome(ChromeDriverManager().install())
    driver.maximize_window()

    # Atribui o caminho para a pagina do relatorio de financiamento do egestor  
    egestorFinanciamento = 'https://egestorab.saude.gov.br/gestaoaps/relFinanciamentoParcela.xhtml'
    driver.get(egestorFinanciamento)

    # Seletor para UF
    selectUF = driver.find_element(By.CSS_SELECTOR,'#j_idt58\:uf')
    Select(selectUF).select_by_visible_text('** TODOS **')  
    time.sleep(5)  # esperar um pouco pela resposta do servidor

    # Seletor para Ano
    selectAno = driver.find_element(By.CSS_SELECTOR,'#j_idt58\:ano')
    Select(selectAno).select_by_visible_text('2022')
    time.sleep(5)

    #Seletor para Parcela
    selectParcela = driver.find_element(By.CSS_SELECTOR,'#j_idt58\:compInicio')
    Select(selectParcela).select_by_visible_text(periodo)
    time.sleep(5)

    # Baixa o relatório clicando no botão download
    botaoDownload = driver.find_element(By.CLASS_NAME,'btn-app')
    driver.execute_script("arguments[0].click();", botaoDownload)
    time.sleep(40) # espera para concluir o download do relatório

    return True

periodo = 'SET/2022'
df_extraido = extracao(periodo)