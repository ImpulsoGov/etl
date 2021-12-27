# SPDX-FileCopyrightText: 2021 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Baixa webdrivers e permite gerenciar janelas de navegadores automatizados.

Atributos:
    diretorio_downloads: Diretório onde são salvos os arquivos baixados com os
        navegadores automatizados.
"""


from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium_driver_updater import DriverUpdater

ambiente = os.getenv("IMPULSOETL_AMBIENTE", "desenvolvimento")
if ambiente == "desenvolvimento":
    _diretorio_raiz = Path(__file__).parents[2]

    _diretorio_binarios = _diretorio_raiz / "bin"
    if not _diretorio_binarios.is_dir():
        _diretorio_binarios.mkdir()

if ambiente != "desenvolvimento":
    _diretorio_raiz = Path.home()
    _diretorio_binarios = Path("/usr/local/bin")

diretorio_downloads = Path(
    os.getenv("IMPULSOETL_DOWNLOADS_CAMINHO") or Path(_diretorio_raiz / "tmp"),
)
if not diretorio_downloads.is_dir():
    diretorio_downloads.mkdir()


_chromedriver_caminho = DriverUpdater.install(
    path=str(_diretorio_binarios),
    driver_name=DriverUpdater.chromedriver,
    upgrade=False,
    check_date=False,
    old_return=False,
)

_geckodriver_caminho = DriverUpdater.install(
    path=str(_diretorio_binarios),
    driver_name=DriverUpdater.geckodriver,
    upgrade=False,
    check_date=False,
    old_return=False,
)


@contextmanager
def criar_chromedriver(**kwargs) -> Generator[webdriver.Chrome, None, None]:
    """Gera um objeto para manipular um WebDriver Chrome com Selenium."""

    servico = ChromeService(executable_path=_chromedriver_caminho)
    opcoes = webdriver.ChromeOptions(**kwargs)
    opcoes.add_experimental_option(
        "prefs",
        {"download.default_directory": str(diretorio_downloads.resolve())},
    )
    if ambiente != "desenvolvimento":
        opcoes.headless = True

    driver = webdriver.Chrome(service=servico, options=opcoes)
    try:
        yield driver
    finally:
        driver.quit()


@contextmanager
def criar_geckodriver(**kwargs) -> Generator[webdriver.Firefox, None, None]:
    """Gera um objeto para manipular um WebDriver Firefox com Selenium."""

    servico = FirefoxService(executable_path=_geckodriver_caminho)
    opcoes = webdriver.FirefoxOptions(**kwargs)
    opcoes.set_preference("browser.download.folderList", 2)
    opcoes.set_preference(
        "browser.download.manager.showWhenStarting",
        value=False,
    )
    opcoes.set_preference(
        "browser.download.dir",
        value=str(diretorio_downloads.resolve()),
    )
    opcoes.set_preference("browser.helperApps.neverAsk.saveToDisk", "text/csv")
    if ambiente != "desenvolvimento":
        opcoes.headless = True

    driver = webdriver.Firefox(service=servico, options=opcoes)
    try:
        yield driver
    finally:
        driver.quit()


def listar_downloads() -> set[Path]:
    """Obtém os caminhos de todos os arquivos no diretório de downloads."""

    return set(diretorio_downloads.iterdir())
