# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT

""" Extrai relatório de financiamento a partir do egestor"""
import os
import time
from datetime import date, datetime

from selenium.webdriver.support.select import Select
from selenium.webdriver.common.by import By

import sys
#sys.path.append(r'C:\Users\maira\Impulso\etl\src')
from impulsoetl.navegadores import criar_geckodriver, diretorio_downloads
from impulsoetl.navegadores import criar_chromedriver

from functools import lru_cache

meses = {
        'JAN':'01',
        'FEV':'02',
        'MAR':'03',
        'ABR':'04',
        'MAI':'05',
        'JUN':'06',
        'JUL':'07',
        'AGO':'08',
        'SET':'09',
        'OUT':'10',
        'NOV':'11',
        'DEZ':'12'
    }

@lru_cache(1)
def extracao(periodo_mes:date)->str:
      
    mes = str(periodo_mes.month)
    ano = str(periodo_mes.year)

    for m in meses:
        if mes == meses[m]:
            mes = m

    periodo_mes = mes + '/' + ano

    with criar_chromedriver() as driver:

        egestorFinanciamento = 'https://egestorab.saude.gov.br/gestaoaps/relFinanciamentoParcela.xhtml'
        driver.get(egestorFinanciamento)

        selectUF = driver.find_element(By.CSS_SELECTOR,'#j_idt58\:uf')
        Select(selectUF).select_by_visible_text('** TODOS **')  
        time.sleep(5)  # esperar um pouco pela resposta do servidor

        selectAno = driver.find_element(By.CSS_SELECTOR,'#j_idt58\:ano')
        Select(selectAno).select_by_visible_text('2022')
        time.sleep(5)

        selectParcela = driver.find_element(By.CSS_SELECTOR,'#j_idt58\:compInicio')
        Select(selectParcela).select_by_visible_text(periodo_mes)
        time.sleep(5)

        botaoDownload = driver.find_element(By.CLASS_NAME,'btn-app')
        driver.execute_script("arguments[0].click();", botaoDownload)

        # Espera e verifica se o download foi concluído
        contador = 0
        #ESPERA_MAX: Final[int] = int(os.getenv("IMPULSOETL_ESPERA_MAX", 300))
        ESPERA_MAX = 300
        while contador < ESPERA_MAX:
            time.sleep(1)            
            if not os.path.exists(diretorio_downloads/'pagamento_aps.xls'):
                contador += 1
            else:
                break
       
        arquivos_baixados = diretorio_downloads/'pagamento_aps.xls'
   
        if arquivos_baixados:
            path = diretorio_downloads
            arquivo = 'pagamento_aps.xls'
            new_name = str(periodo_mes[4:8] + '_' + periodo_mes[0:3]) + '_'+ arquivo
            old_name = os.path.join(path, arquivo)
            new_name = os.path.join(path, f"{new_name.split('.')[0]}.{new_name.split('.')[1]}")
            os.rename(old_name, new_name)

        return new_name