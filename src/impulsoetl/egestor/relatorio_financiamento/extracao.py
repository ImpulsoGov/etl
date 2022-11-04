# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT

""" Extrai relatório de financiamento a partir do egestor"""
import os
import time
from datetime import date, datetime
from pathlib import Path
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
    logger.info(
        "Iniciando a captura dos relatórios de financiamento do e-Gestor ",
        "para o mês {:%m/%Y}",
        periodo_mes,
    )

    mes = meses[periodo_mes.month-1]
    ano = str(periodo_mes.year)
    competencia_por_extenso = mes + '/' + ano

    logger.info(
        "Definindo destino para downloads e apagando versões pré-existentes..."
    )
    resultado_caminho = (diretorio_downloads / "pagamento_aps.xls")
    resultado_caminho.unlink(missing_ok=True)

    logger.info("Inicializando instância do navegador web...")
    with criar_geckodriver() as driver:

        logger.info("Acessando o site do e-Gestor...")
        egestorFinanciamento = 'https://egestorab.saude.gov.br/gestaoaps/relFinanciamentoParcela.xhtml'
        driver.get(egestorFinanciamento)
        logger.debug(driver.log_file.read())

        logger.info("Selecionando a unidade federativa de referência...")
        selectUF = driver.find_element(By.CSS_SELECTOR,'#j_idt58\:uf')
        Select(selectUF).select_by_visible_text('** TODOS **')  
        time.sleep(5)  # esperar um pouco pela resposta do servidor
        logger.info("Todas as unidades federativas selecionadas!")
        logger.debug(driver.log_file.read())

        logger.info("Selecionando o ano de referência...")
        selectAno = driver.find_element(By.CSS_SELECTOR,'#j_idt58\:ano')
        Select(selectAno).select_by_visible_text(ano)
        time.sleep(5)
        logger.info("Selecionado o ano de {} como referência!", ano)
        logger.debug(driver.log_file.read())

        logger.info("Selecionando o mês de referência...")
        selectParcela = driver.find_element(By.CSS_SELECTOR,'#j_idt58\:compInicio')
        Select(selectParcela).select_by_visible_text(competencia_por_extenso)
        time.sleep(5)
        logger.info(
            "Selecionado o ano de {} como referência!",
            competencia_por_extenso,
        )
        logger.debug(driver.log_file.read())

        logger.info("Solicitando início do download ao servidor...")
        botaoDownload = driver.find_element(By.CLASS_NAME,'btn-app')
        download_iniciado = False
        driver.execute_script("arguments[0].click();", botaoDownload)
        logger.debug(driver.log_file.read())

        # Espera e verifica se o download foi concluído
        contador = 0
        ESPERA_MAX: Final[int] = int(os.getenv("IMPULSOETL_ESPERA_MAX", 300))
        while contador < ESPERA_MAX:
            downloads = listar_downloads()
            if resultado_caminho.is_file():  # download já começou?
                if download_iniciado:  # informa andamento
                    logger.debug(
                        "Baixados {} bytes...",
                        resultado_caminho.stat.st_size,
                    )
                else:  # avisa que o download acabou de começar
                    download_iniciado = True
                    logger.info("Download iniciado!")
                # checa se não há arquivo indicando que o download ainda é
                # parcial
                if not any(caminho.match('*.part') for caminho in downloads):
                    # se não houver, é porque o download acabou!
                    return resultado_caminho

            # se download ainda não começou ou não finalizou, imprime logs e
            # espera
            logger.debug(driver.log_file.read())
            contador += 1
            time.sleep(1)

        raise TimeoutError(
            "O download demorou mais tempo do que o aceito para ser concluído."
        )
