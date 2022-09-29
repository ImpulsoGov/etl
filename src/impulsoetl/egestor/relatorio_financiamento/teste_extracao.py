from asyncio.windows_events import NULL
from time import sleep
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.select import Select
from selenium.webdriver.common.by import By


egestorFinanciamento = 'https://egestorab.saude.gov.br/gestaoaps/relFinanciamentoParcela.xhtml'

driver = webdriver.Chrome(ChromeDriverManager().install())
driver.maximize_window()
driver.get(egestorFinanciamento)

selectUF = driver.find_element(By.CSS_SELECTOR,'#j_idt58\:uf')
Select(selectUF).select_by_visible_text('** TODOS **')

selectMunicipio= driver.find_element(By.CSS_SELECTOR,'#j_idt58\:municipio')
Select(selectMunicipio).select_by_visible_text('** TODOS **')

selectAno= driver.find_element(By.CSS_SELECTOR,'#j_idt58\:ano')
Select(selectAno).select_by_visible_text('2022')

selectParcela= driver.find_element(By.CSS_SELECTOR,'#j_idt58\:compInicio')
Select(selectParcela).select_by_visible_text('SET/2022')

botaoDownload = driver.find_element(By.CSS_SELECTOR,'#j_idt58\:visualizacao > div')

if botaoDownload !=NULL:
    botaoDownload.click()
    print("Botão encontrado")
    driver.implicitly_wait(30000)
else:
    print("Botão não encontrado")
