# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Define elementos comuns a diversos formulários do SISAB."""


from __future__ import annotations

import os
from abc import ABC, abstractmethod
from contextlib import contextmanager
from datetime import date
from functools import cached_property
from io import StringIO
from time import sleep
from typing import Final, Iterable

import pandas as pd
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait

from impulsoetl.loggers import logger
from impulsoetl.navegadores import listar_downloads
from impulsoetl.sisab.excecoes import (
    SisabErroCompetenciaInexistente,
    SisabErroPreenchimentoIncorreto,
    SisabErroRotuloOuValorInexistente,
)
from impulsoetl.utilitarios.textos import normalizar_texto

ESPERA_MAX: Final[int] = int(os.getenv("IMPULSOETL_ESPERA_MAX", 300))
SISAB_URL: Final[str] = "https://sisab.saude.gov.br"


class FiltroUnico(object):  # noqa: WPS613
    """Modela filtros do tipo dropdown no SISAB, que assumem um único valor."""

    def __init__(
        self,
        driver: webdriver.Chrome | webdriver.Firefox,
        elemento_id: str,
        rotulo: str | None = None,
        valor: str | None = None,
    ):
        self.driver = driver
        self.elemento_id = elemento_id

        if rotulo:
            self.definir_opcao_por_rotulo(rotulo)
        if valor:
            self.definir_opcao_por_valor(valor)

    def ler_rotulo_ativo(self) -> str | None:
        try:
            return self._selecao.first_selected_option.text
        except AttributeError:
            return None

    def definir_opcao_por_rotulo(self, rotulo: str):
        self._selecao.select_by_visible_text(rotulo)
        sleep(3)

    def ler_valor_ativo(self) -> str | None:
        try:
            return self._selecao.first_selected_option.get_attribute("value")
        except AttributeError:
            return None

    def definir_opcao_por_valor(self, valor: str):
        self._selecao.select_by_value(valor)
        sleep(3)

    @property
    def _selecao(self) -> Select:
        dropdown = self.driver.find_element(By.ID, self.elemento_id)
        return Select(dropdown)


class FiltroMultiplo(object):  # noqa: WPS214  # permitir muitos métodos
    """Modela filtros do tipo botão + checkbox no SISAB, com vários valores."""

    # TODO: dá para refatorar esta classe para usar os mesmos métodos para
    # rotulos e valores

    def __init__(
        self,
        driver: webdriver.Chrome | webdriver.Firefox,
        elemento_precedente_id: str,
        rotulos: Iterable[str] = [],
        valores: Iterable[str] = [],
    ):
        self.driver = driver
        self._elemento_precedente_id = elemento_precedente_id

        self.ultima_consulta: pd.Timestamp | None = None

        if rotulos:
            with self.abrir_menu_opcoes():
                self.definir_opcoes_por_rotulos(rotulos)
        if valores:
            with self.abrir_menu_opcoes():
                self.definir_opcoes_por_valores(valores)

    @contextmanager
    def abrir_menu_opcoes(self, ignorar_se_ja_aberto=True):
        if self._botao_aberto and ignorar_se_ja_aberto:
            yield
        else:
            # clica usando JavaScript, para evitar que algum tooltip intercepte
            # o clique
            self.driver.execute_script(
                "arguments[0].click();",
                self._botao_abre_fecha,
            )
            sleep(2)
            yield
            self.driver.execute_script(
                "arguments[0].click();",
                self._botao_abre_fecha,
            )
            sleep(1)

    @cached_property
    def rotulos_disponiveis(self) -> set[str]:
        """Conjunto com rótulos de todas as opções disponiveis para seleção."""
        return {
            elem.find_element(By.CSS_SELECTOR, "label").text
            for elem in self._opcoes_disponiveis
        }

    @cached_property
    def valores_disponiveis(self) -> set[str]:
        """Conjunto com valores de todas as opções disponiveis para seleção."""
        return {
            elem.find_element(By.CSS_SELECTOR, "input").get_attribute("value")
            for elem in self._opcoes_disponiveis
        }

    def ler_rotulos_ativos(self) -> set[str]:
        """Conjunto com rótulos das opções selecionadas atualmente."""
        with self.abrir_menu_opcoes():
            return {
                elem.find_element(By.CSS_SELECTOR, "label").text
                for elem in self._opcoes_ativas
            }

    def ler_valores_ativos(self) -> set[str]:
        """Conjunto com valores das opções selecionadas atualmente."""
        with self.abrir_menu_opcoes():
            return {
                elem.find_element(By.CSS_SELECTOR, "input").get_attribute(
                    "value",
                )
                for elem in self._opcoes_ativas
            }

    def definir_opcoes_por_valores(self, valores: Iterable[str]) -> None:
        """Marca como ativas as opções em função dos valores desejados."""

        # definir quais valores devem estar selecionados
        valores_desejados = set(valores)
        valores_a_selecionar = valores_desejados.difference(
            self.ler_valores_ativos(),
        )
        valores_a_desselecionar = self.ler_valores_ativos().difference(
            valores_desejados,
        )
        valores_a_clicar = valores_a_selecionar.union(valores_a_desselecionar)

        # avisar caso algum dos valores apontados não exista
        valores_indisponiveis = valores_desejados.difference(
            self.valores_disponiveis,
        )
        if valores_indisponiveis:
            raise SisabErroRotuloOuValorInexistente(
                "Os seguintes valores não estão disponíveis para consulta"
                + " no SISAB: {}".format(", ".join(valores_indisponiveis)),
            )

        for opcao in self._opcoes_disponiveis:
            opcao_valor = opcao.find_element(
                By.CSS_SELECTOR, "input",
            ).get_attribute("value")
            if opcao_valor in valores_a_clicar:
                input_ = opcao.find_element(By.CSS_SELECTOR, "input")
                input_.click()
                sleep(3)

    def definir_opcoes_por_rotulos(self, rotulos: Iterable[str]) -> None:
        """Marca como ativas as opções em função dos rótulos desejados."""

        # definir quais dos rótulos devem estar selecionados
        rotulos_desejados = set(rotulos)
        rotulos_a_selecionar = rotulos_desejados.difference(
            self.ler_rotulos_ativos(),
        )
        rotulos_a_desselecionar = self.ler_rotulos_ativos().difference(
            rotulos_desejados,
        )
        rotulos_a_clicar = {
            normalizar_texto(rotulo)
            for rotulo in rotulos_a_selecionar.union(rotulos_a_desselecionar)
        }

        # avisar caso algum dos rótulos apontados não exista
        rotulos_indisponiveis = rotulos_desejados.difference(
            self.rotulos_disponiveis,
        )
        if rotulos_indisponiveis:
            raise SisabErroRotuloOuValorInexistente(
                "Os seguintes rótulos não estão disponíveis para consulta"
                + " no SISAB: {}".format(", ".join(rotulos_indisponiveis)),
            )

        for opcao in self._opcoes_disponiveis:
            opcao_rotulo = normalizar_texto(
                opcao.find_element(By.CSS_SELECTOR, "label").text,
            )
            if opcao_rotulo in rotulos_a_clicar:
                input_ = opcao.find_element(By.CSS_SELECTOR, "input")
                input_.click()
                sleep(3)

    @property
    def _menu(self) -> WebElement:
        try:
            # tentar como o elemento botão sendo o primeiro irmão após o
            # elemento cujo ID foi fornecido
            seletor_css = "#{} + div.btn-group".format(
                self._elemento_precedente_id,
            )
            return self.driver.find_element(By.CSS_SELECTOR, seletor_css)
        except NoSuchElementException:
            # tentar como o elemento botão sendo o filho do elemento cujo ID
            # foi fornecido
            seletor_css = "#{} > div.btn-group".format(
                self._elemento_precedente_id,
            )
            return self.driver.find_element(By.CSS_SELECTOR, seletor_css)

    @property
    def _botao_abre_fecha(self) -> WebElement:
        return self._menu.find_element(By.CSS_SELECTOR, "button")

    @property
    def _botao_aberto(self) -> bool:
        if self._botao_abre_fecha.get_attribute("aria-expanded") == "true":
            return True
        return False

    @property
    def _opcoes_disponiveis(self) -> list[WebElement]:
        return self._menu.find_elements(By.CSS_SELECTOR, "li")

    @property
    def _opcoes_ativas(self) -> list[WebElement]:
        return self._menu.find_elements(By.CSS_SELECTOR, "li.active")


class FiltroCompetencias(FiltroMultiplo):
    """Modela seleção de múltiplas competências nos relatórios do SISAB."""

    def __init__(self, driver: webdriver.Chrome | webdriver.Firefox, *args):

        super().__init__(driver, elemento_precedente_id="competencia")

        if args:
            try:
                with self.abrir_menu_opcoes():
                    self.definir_competencias(args)
            except NoSuchElementException:
                with self.abrir_menu_opcoes():
                    self.definir_competencias(args)

    def ler_competencias_ativas(self) -> set[pd.Timestamp]:
        valores_ativos = super().ler_valores_ativos()  # noqa: WPS613
        return {pd.to_datetime(dt, format="%Y%m") for dt in valores_ativos}

    def definir_competencias(
        self,
        competencias: list[date | pd.Timestamp] | pd.DatetimeIndex,
    ) -> None:
        try:
            super().definir_opcoes_por_valores(
                {"{:%Y%m}".format(dt) for dt in competencias},  # noqa: WPS613
            )
        except SisabErroRotuloOuValorInexistente as err:
            logger.error("Uma ou mais competências não estão disponíveis.")
            raise SisabErroCompetenciaInexistente from err


class FormularioAbstrato(ABC):
    """Abstração de uma classe que modela um formulário do SISAB."""

    def __init__(
        self,
        relatorio_url: str,
        driver: webdriver.Chrome | webdriver.Firefox,
    ):

        self.driver = driver

        # acessar a página com o relatório
        self.driver.get(relatorio_url)

        # aguardar até 2 minutos até que os elementos sejam carregados
        WebDriverWait(self.driver, ESPERA_MAX).until(
            EC.presence_of_element_located((By.ID, "limpaTela")),
        )
        sleep(3)

    def executar_consulta(self) -> None:
        """Envia ao SISAB uma consulta com os filtros definidos no formulário."""
        downloads_antes = listar_downloads()

        self._botao_download.click()
        sleep(2)
        self._opcao_download_csv.click()

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

            # checar se o relatório não foi gerado por um erro de preenchimento
            # no formulário; se for o caso, interromper e devolver a mensagem
            # de erro do sistema
            if self.mensagem_erro:
                raise SisabErroPreenchimentoIncorreto(self.mensagem_erro)

        # salvar o relatório baixado na propriedade `resultado`
        if arquivo_baixado:
            self.ultima_consulta = pd.Timestamp.now(tz="Etc/GMT+3")
            with open(arquivo_baixado, "r", encoding="ISO-8859-1") as arquivo:
                self._resultado = arquivo.read()
            arquivo_baixado.unlink()
            return
        raise TimeoutError(
            "O servidor demorou mais tempo do que o aceito para responder.",
        )

    def limpar_filtros(self) -> None:
        """Remove todos filtros aplicados ou os reseta para o valor padrão."""
        self._botao_limpar_filtros.submit()
        sleep(5)

    @property
    @abstractmethod
    def resultado(self) -> pd.DataFrame | None:
        """Resultado da consulta."""
        raise NotImplementedError(
            "Modelo de relatório não implementado para o processamento deste "
            + "tipo de relatório.",
        )

    @property
    def mensagem_erro(self) -> str | None:
        """Mensagem retornada pelo sistema em caso de erro."""
        elemento_erro = self.driver.find_elements(
            By.CSS_SELECTOR,
            ".alert-danger",
        )
        if elemento_erro:
            return elemento_erro[0].text
        return None

    @property
    def _botao_download(self):
        return self.driver.find_element(By.CSS_SELECTOR, ".form-group button")

    @property
    def _opcao_download_csv(self):
        return self.driver.find_element(
            By.XPATH,
            "//a[contains(text(), 'Csv')]",
        )

    @property
    def _botao_limpar_filtros(self):
        return self.driver.find_element(By.CSS_SELECTOR, "#limpaTela")


class RelatorioAbstrato(ABC):
    """Abstração de uma classe que modela um relatório do SISAB."""

    def __init__(self, relatorio_csv: str):

        linhas = relatorio_csv.split("\n")

        # tratar como cabeçalho tudo o que vem antes da primeira sequência
        # de duas linhas vazias
        linha_anterior_preenchida = None
        for indice, linha in enumerate(linhas):
            linha_atual_preenchida = bool(linha)
            if not linha_anterior_preenchida and not linha_atual_preenchida:
                linhas_cabecalho = linhas[: indice - 1]
                linhas_pos_cabecalho = linhas[indice + 1 :]
                break
            linha_anterior_preenchida = linha_atual_preenchida

        # tratar como rodapé tudo o que vem depois da segunda sequência de
        # duas linhas vazias
        linha_anterior_preenchida = None
        for indice, linha in enumerate(linhas_pos_cabecalho):
            linha_atual_preenchida = bool(linha)
            if not linha_anterior_preenchida and not linha_atual_preenchida:
                linhas_conteudo = linhas_pos_cabecalho[: indice - 1]
                linhas_rodape = linhas_pos_cabecalho[indice + 1 :]
                break
            linha_anterior_preenchida = linha_atual_preenchida

        self._cabecalho = "\n".join(linhas_cabecalho)
        self._conteudo_csv = "\n".join(linhas_conteudo)
        self._rodape = "\n".join(linhas_rodape)

        # ler arquivo csv
        self.dados = pd.read_csv(
            StringIO(self._conteudo_csv),
            sep=";",
            decimal=",",
            thousands=".",
            encoding="ISO-8859-1",
        )
