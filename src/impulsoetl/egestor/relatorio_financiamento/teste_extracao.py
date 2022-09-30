""" Extrai relatório de financiamento a partir do egestor"""

from asyncio.windows_events import NULL
from time import sleep

from webdriver_manager.chrome import ChromeDriverManager
from selenium import webdriver
from selenium.webdriver.support.select import Select
from selenium.webdriver.common.by import By
from selenium_driver_updater import DriverUpdater

import sys
sys.path.append("/Users/maira/Documents/ImpulsoGov/etl/src/impulsoetl")
from impulsoetl.navegadores import listar_downloads

# Atribui o caminho para a pagina do relatorio de financiamento do egestor
egestorFinanciamento = 'https://egestorab.saude.gov.br/gestaoaps/relFinanciamentoParcela.xhtml'

# Cria um objeto para para manipular um WebDriver Chrome com Selenium
driver = webdriver.Chrome(ChromeDriverManager().install())
driver.maximize_window()
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
Select(selectParcela).select_by_visible_text('SET/2022')
time.sleep(5)

# Baixa o relatório clicando no botão download
botaoDownload = driver.find_element(By.CLASS_NAME,'btn-app')
driver.execute_script("arguments[0].click();", botaoDownload)
time.sleep(40) # espera para concluir o download do relatório

# Verificar e garantir que o relatório seja baixado somente uma vez
downloads_antes = listar_downloads()
# checar a cada 1 segundo se o relatório foi baixado
contador = 0
arquivo_baixado = None

while contador < ESPERA_MAX:
# esperar um pouco pela resposta do servidor
    sleep(2)
    contador += 1

# checar se o relatório já foi baixado; interromper laço se sim
    arquivos_baixados = listar_downloads().difference(downloads_antes)
    if arquivos_baixados:
        arquivo_baixado = arquivos_baixados.pop()
        break
