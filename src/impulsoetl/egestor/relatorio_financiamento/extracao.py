# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT

""" Extrai relatório de financiamento a partir do egestor"""
import os
import time
from datetime import date, datetime
from typing import Final
from pyparsing import NotAny

from selenium.webdriver.support.select import Select
from selenium.webdriver.common.by import By

import sys
#sys.path.append(r'C:\Users\maira\Impulso\etl\src')
from impulsoetl.navegadores import criar_geckodriver, diretorio_downloads, listar_downloads
from impulsoetl.navegadores import criar_chromedriver
from impulsoetl.loggers import logger

from functools import lru_cache

meses = ['JAN','FEV','MAR','ABR','MAI','JUN','JUL','AGO','SET','OUT','NOV','DEZ']

@lru_cache(1)
def extracao(periodo_mes:date)->str:
    
    mes = meses[periodo_mes.month-1]
    ano = str(periodo_mes.year)
    competencia_por_extenso = mes + '/' + ano

    resultado_caminho = (diretorio_downloads / "pagamento_aps.xls")
    resultado_caminho.unlink(missing_ok=True)

    with criar_geckodriver() as driver:

        egestorFinanciamento = 'https://egestorab.saude.gov.br/gestaoaps/relFinanciamentoParcela.xhtml'
        driver.get(egestorFinanciamento)

        selectUF = driver.find_element(By.CSS_SELECTOR,'#j_idt58\:uf')
        Select(selectUF).select_by_visible_text('** TODOS **')  
        time.sleep(5)  # esperar um pouco pela resposta do servidor

        selectAno = driver.find_element(By.CSS_SELECTOR,'#j_idt58\:ano')
        Select(selectAno).select_by_visible_text(ano)
        time.sleep(5)

        selectParcela = driver.find_element(By.CSS_SELECTOR,'#j_idt58\:compInicio')
        Select(selectParcela).select_by_visible_text(competencia_por_extenso)
        time.sleep(5)

        botaoDownload = driver.find_element(By.CLASS_NAME,'btn-app')
        driver.execute_script("arguments[0].click();", botaoDownload)

        # Espera e verifica se o download foi concluído
        contador = 0
        #ESPERA_MAX: Final[int] = int(os.getenv("IMPULSOETL_ESPERA_MAX", 300))
        ESPERA_MAX = 120
        while contador < ESPERA_MAX:
            downloads = listar_downloads()       
            if resultado_caminho.is_file() and not any(caminho.match('*.part') for caminho in downloads): 
                return resultado_caminho
            else:
                contador += 1
                time.sleep(1)
                if contador>30:
                    breakpoint()
        raise TimeoutError
