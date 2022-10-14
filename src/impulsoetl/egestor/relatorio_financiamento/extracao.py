# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT

""" Extrai relatório de financiamento a partir do egestor"""
import os
from concurrent.futures.process import EXTRA_QUEUED_CALLS
from pathlib import Path
import time
from time import sleep
from asyncio.windows_events import NULL
from typing import Final

from webdriver_manager.chrome import ChromeDriverManager
from selenium import webdriver
from selenium.webdriver.support.select import Select
from selenium.webdriver.common.by import By
from selenium_driver_updater import DriverUpdater

import pandas as pd

import sys
sys.path.append(r'C:\Users\maira\Impulso\etl\src')
from impulsoetl.navegadores import listar_downloads

def extracao(periodo_mes:str)->str:
    # Cria um um WebDriver  com Chrome
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
    Select(selectParcela).select_by_visible_text(periodo_mes)
    time.sleep(5)

    # Baixa o relatório clicando no botão download
    botaoDownload = driver.find_element(By.CLASS_NAME,'btn-app')
    driver.execute_script("arguments[0].click();", botaoDownload)

    # Espera e verifica se o download foi concluído
    contador = 0
    ESPERA_MAX = 300
    while contador < ESPERA_MAX:
            time.sleep(1)            
            if not os.path.exists(r'C:\Users\maira\Downloads\pagamento_aps.xls'):
                contador += 1
            else:
                break

    arquivos_baixados = r'C:\Users\maira\Downloads\pagamento_aps.xls'
    if arquivos_baixados:
        #Renomeia o arquivo baixado
        path = r'C:\Users\maira\Downloads'
        arquivo = 'pagamento_aps.xls'
        new_name = str(periodo_mes[4:8] + '_' + periodo_mes[0:3]) + '_'+ arquivo
        old_name = os.path.join(path, arquivo)
        new_name = os.path.join(path, f"{new_name.split('.')[0]}.{new_name.split('.')[1]}")
        os.rename(old_name, new_name)

        return new_name