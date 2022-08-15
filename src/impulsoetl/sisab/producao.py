# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Obtém quantitativos de contatos assistenciais na atenção primária à saúde.

Atributos:
    RELATORIO_URL: URL do formulário de consulta a relatórios de produção do
        SISAB.
    DE_PARA_RELATORIO_PRODUCAO: Correspondência entre os nomes de campos
        (previamente tratados) provenientes do SISAB e os nomes padronizados
        para uso no banco de dados da ImpulsoGov.
    TIPOS_RELATORIO_PRODUCAO: Definições dos tipos esperados de cada coluna
        dos dados obtidos.
    COLUNAS_MINIMAS: Conjunto fixo de colunas esperadas em todos os relatórios
        de produção enviados para o banco de dados da ImpulsoGov.
"""


from __future__ import annotations

import re
import uuid
from collections.abc import MutableMapping
from functools import cached_property
from typing import Any, Final, Iterable

import janitor  # noqa: F401  # nopycln: import  # janitor é usado indireta/e
import pandas as pd
import requests
import sqlalchemy as sa
from bs4 import BeautifulSoup
from frozendict import frozendict
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
from sqlalchemy.orm import Session
from toolz.functoolz import compose_left

from impulsoetl.bd import Base
from impulsoetl.comum.datas import periodo_por_data
from impulsoetl.comum.geografias import (
    id_impulso_para_id_sus,
    id_sus_para_id_impulso,
    uf_id_ibge_para_sigla,
)
from impulsoetl.loggers import logger
from impulsoetl.navegadores import criar_geckodriver
from impulsoetl.sisab.comum import (
    SISAB_URL,
    FiltroCompetencias,
    FiltroMultiplo,
    FiltroUnico,
    FormularioAbstrato,
    RelatorioAbstrato,
)
from impulsoetl.sisab.excecoes import SisabErroRotuloOuValorInexistente
from impulsoetl.sisab.modelos import TabelaProducao
from impulsoetl.tipos import DatetimeLike
from impulsoetl.utilitarios.repetidores import repetir_por_ano_mes
from impulsoetl.utilitarios.textos import normalizar_texto, tratar_nomes_campos

RELATORIO_URL: Final[str] = (
    SISAB_URL
    + "/paginas/acessoRestrito/relatorio/federal/saude/RelSauProducao.xhtml"
)

DE_PARA_RELATORIO_PRODUCAO: Final[frozendict] = frozendict(
    {
        "municipios": "municipio_id_sus",
    },
)

TIPOS_RELATORIO_PRODUCAO: Final[frozendict] = frozendict(
    {
        "municipio_id_sus": str,
        "quantidade_registrada": "Int64",
    },
)

# campos que devem estar em todas as tabelas
COLUNAS_MINIMAS: Final[frozenset] = frozenset(
    (
        "periodo_id",
        "unidade_geografica_id",
        "tipo_producao",
        "quantidade_registrada",
    ),
)


class FiltroIdade(object):
    """Elemento de definição do intervalo de idade considerada no relatório.

    Parâmetros:
        driver: Objeto [`webdriver.Chrome`][] ou [`webdriver.Firefox`][] do
            Selenium que permita interagir com o navegador automatizado para
            raspagem do formulário de produção do SISAB.
        idade_de: Idade mínima do intervalo de idade considerada no relatório.
        idade_ate: Idade máxima do intervalo de idade considerada no relatório.
        unidade: Unidade de medida da idade considerada no relatório. Deve ser
            um entre `'Ignorar'`, `'Ano'` ou `'Dias'`. Por padrão, o valor é
            `'Ignorar'`, o que equivale a não utilizar o filtro.

    [`webdriver.Chrome`]: https://selenium-python.readthedocs.io
    /api.html#module-selenium.webdriver.chrome.webdriver
    [`webdriver.Firefox`]: https://selenium-python.readthedocs.io
    /api.html#module-selenium.webdriver.firefox.webdriver
    """

    def __init__(
        self,
        driver: webdriver.Chrome | webdriver.Firefox,
        idade_de: int = 0,
        idade_ate: int = 0,
        unidade: str = "Ignorar",
    ) -> None:
        self.driver = driver
        if idade_de > idade_ate:
            raise ValueError(
                "Idade mínima deve ser menor ou igual à idade máxima.",
            )
        if idade_de != 0 or idade_ate != 0:
            self.idade_de = idade_de
            self.idade_ate = idade_ate
            self.unidade = unidade

    @property
    def idade_de(self) -> int:
        """Idade mínima do intervalo de idade considerada no relatório."""
        return int(
            self.driver.find_element(
                By.CSS_SELECTOR,
                "#idadeInicio",
            ).get_attribute("value"),
        )

    @idade_de.setter
    def idade_de(self, idade_de: int) -> None:
        self.driver.find_element(
            By.CSS_SELECTOR,
            "#idadeInicio",
        ).send_keys(str(idade_de))

    @property
    def idade_ate(self) -> int:
        """Idade máxima do intervalo de idade considerada no relatório."""
        return int(
            self.driver.find_element(
                By.CSS_SELECTOR,
                "#idadeInicio",
            ).get_attribute("value"),
        )

    @idade_ate.setter
    def idade_ate(self, idade_ate: int) -> None:
        self.driver.find_element(
            By.CSS_SELECTOR,
            "#idadeFim",
        ).send_keys(str(idade_ate))

    @property
    def unidade(self) -> str:
        """Unidade de medida da idade considerada no relatório."""
        try:
            input_unidade_selecionada = self.driver.find_element(
                By.CSS_SELECTOR,
                "input[name='tpIdade']:checked",
            )
            input_unidade_selecionada_id = (
                input_unidade_selecionada.get_attribute("id")
            )
            label_unidade_selecionada = self.driver.find_element(
                By.CSS_SELECTOR,
                "label[for='{}']".format(input_unidade_selecionada_id),
            )
            return label_unidade_selecionada.text.strip()
        except NoSuchElementException:
            return "Ignorar"

    @unidade.setter
    def unidade(self, unidade: str) -> None:
        inputs_unidade = self.driver.find_elements(
            By.CSS_SELECTOR,
            "input[name='tpIdade']",
        )
        for input_unidade in inputs_unidade:
            input_unidade_id = input_unidade.get_attribute("id")
            label_unidade = self.driver.find_element(
                By.CSS_SELECTOR,
                "label[for='{}']".format(input_unidade_id),
            )
            if label_unidade.text.strip() == unidade:
                input_unidade.click()
                break

    @property
    def unidades_disponiveis(self) -> set[str]:
        return {"Dias", "Ano"}

    @property
    def faixa_etaria(self) -> str | None:
        """Intervalo de idades considerado no relatório como uma faixa etária.

        Descreve o intervalo de idade mínima e máxima filtrados no relatório
        como um texto do tipo '0 a 10 anos', '0 a 180 dias' etc.
        """
        if self.unidade in self.unidades_disponiveis:
            texto_faixa_etaria = "{} a {} {}".format(
                self.idade_de,
                self.idade_ate,
                normalizar_texto(self.unidade),
            )
            if not texto_faixa_etaria.endswith("s"):
                texto_faixa_etaria += "s"
            return texto_faixa_etaria
        return None


class FiltroCiapCid(object):
    """Elemento de definição dos CIAPs/CIDs considerados no relatório.

    Parâmetros:
        driver: Objeto [`webdriver.Chrome`][] ou [`webdriver.Firefox`][] do
            Selenium que permita interagir com o navegador automatizado para
            raspagem do formulário de produção do SISAB.
        ciaps: lista de códigos da Classificação Internacional de Atenção
            Primária (CIAP2) informados como queixa ou condição de saúde
            motivadores do atendimento. Os componentes da lista devem ser os
            códigos alfanuméricos da condição correspondente no CIAP2, sem
            espaços ou caracteres especiais. Por padrão, o valor é uma lista
            vazia, o que corresponde a não usar o filtro.
        cids: lista de códigos da Classificação Internacional de Doenças
            (CID-10) informados como queixa ou condição de saúde motivadores
            do atendimento. Os componentes da lista devem ser os códigos
            alfanuméricos da condição correspondente no CID-10, sem espaços ou
            caracteres especiais. Por padrão, o valor é uma lista vazia, o que
            corresponde a não usar o filtro.

    [`webdriver.Chrome`]: https://selenium-python.readthedocs.io
    /api.html#module-selenium.webdriver.chrome.webdriver
    [`webdriver.Firefox`]: https://selenium-python.readthedocs.io
    /api.html#module-selenium.webdriver.firefox.webdriver
    """

    def __init__(
        self,
        driver: webdriver.Chrome | webdriver.Firefox,
        ciaps: Iterable[str] = [],
        cids: Iterable[str] = [],
    ):
        self.driver = driver

        self.limpar_ciaps_cids()
        for ciap in ciaps:
            self.adicionar_ciap(ciap)
        for cid in cids:
            self.adicionar_cid(cid)

    def limpar_ciaps_cids(self) -> None:
        """Remove todos os filtros ativos de CID-10 e CIAP2."""
        self.driver.execute_script("lsCid = ''")

    def adicionar_ciap(self, ciap: str) -> None:
        """Adiciona um novo código CIAP2 no filtro de CIDs/CIAPs."""
        ciap = normalizar_texto(ciap, separador="", caixa="alta")
        if ciap not in self.ciaps_disponiveis:
            raise SisabErroRotuloOuValorInexistente(
                "O CIAP '{}' não está disponível ".format(ciap)
                + "para consulta no SISAB",
            )
        if ciap not in self.listar_ciaps_ativos():
            self.driver.execute_script("addCid('{}-4')".format(ciap))

    def adicionar_cid(self, cid: str) -> None:
        """Adiciona um novo código CID-10 no filtro de CIDs/CIAPs."""
        cid = normalizar_texto(cid, separador="", caixa="alta")
        if cid not in self.cids_disponiveis:
            raise SisabErroRotuloOuValorInexistente(
                "O CID '{}' não está disponível ".format(cid)
                + "para consulta no SISAB",
            )
        if cid not in self.listar_cids_ativos():
            self.driver.execute_script("addCid('{}-1')".format(cid))

    def listar_valores_ativos(self) -> set[str]:
        """Lista os códigos internos de CID/CIAP selecionados no formulário."""
        return set(self.driver.execute_script("return lsCid").split(","))

    def listar_ciaps_ativos(self) -> set[str]:
        """Lista os códigos CIAP2 selecionados no formulário SISAB."""
        return {
            cod[:-2]
            for cod in self.listar_valores_ativos()
            if cod and cod[-1] == "4"
        }

    def listar_cids_ativos(self) -> set[str]:
        """Lista os códigos CID-10 selecionados no formulário SISAB."""
        return {
            cod[:-2]
            for cod in self.listar_valores_ativos()
            if cod and cod[-1] == "1"
        }

    def listar_rotulos_ativos(self) -> set[str]:
        """Lista os rótulos de CID e CIAP selecionados no formulário SISAB."""
        rotulos_ciaps = {
            self.ciaps_disponiveis[codigo_ciap]
            for codigo_ciap in self.listar_ciaps_ativos()
        }
        rotulos_cids = {
            self.cids_disponiveis[codigo_cid]
            for codigo_cid in self.listar_cids_ativos()
        }
        return rotulos_ciaps.union(rotulos_cids)

    @cached_property
    def ciaps_disponiveis(self) -> dict[str, str]:
        """Conjunto de códigos CIAP2 disponíveis para seleção no SISAB."""
        return self._ciaps_cids_disponiveis[0]

    @cached_property
    def cids_disponiveis(self) -> dict[str, str]:
        """Conjunto de códigos CID-10 disponíveis para seleção no SISAB."""
        return self._ciaps_cids_disponiveis[1]

    @cached_property
    def _ciaps_cids_disponiveis(self) -> tuple[dict[str, str], dict[str, str]]:
        """Conjunto de CIAPs e outro de CIDs disponíveis no SISAB."""
        # HACK: É necessário fazer uma segunda requisição para baixar o XHTML
        # "puro", já que a versão exibida no browser oculta parte dos elementos
        # com os CIDs/CIAPs. Uma vez baixado, o XHTML é processado com o
        # BeautifulSoup para encontrar os itens com ids relativos aos
        # CIAPs/CIDs.
        html_completo = requests.get(RELATORIO_URL).text
        sopa = BeautifulSoup(html_completo, "html.parser")

        padrao_id_ciap = re.compile("^cid-([A-Z]?[0-9]{2})-4")
        padrao_id_cid = re.compile("^cid-([A-Z][0-9]{2,3})-1")

        labels_ciap = sopa.find_all(attrs={"for": padrao_id_ciap})
        labels_cid = sopa.find_all(attrs={"for": padrao_id_cid})

        # dicionários de CIAPs e CIDs no formato {"codigo": "rótulo"}
        ciaps = {
            padrao_id_ciap.search(label.get("for"))[1]: list(  # type: ignore
                label.strings,
            )[-1].strip(" \n\t")
            for label in labels_ciap
        }
        cids = {
            padrao_id_cid.search(label.get("for"))[1]: list(  # type: ignore
                label.strings,
            )[-1].strip(" \n\t")
            for label in labels_cid
        }
        return ciaps, cids


class FiltroTipoProducao(FiltroUnico):
    """Elemento de definição do tipo de produção considerado no relatório.

    Parâmetros:
        driver: Objeto [`webdriver.Chrome`][] ou [`webdriver.Firefox`][] do
            Selenium que permita interagir com o navegador automatizado para
            raspagem do formulário de produção do SISAB.
        rotulo: Rótulo do filtro de tipo de produção desejado, conforme aparece
            na interface do formulário de produção do SISAB.
        valor: Código interno da opção de tipo de produção desejado, conforme
            utilizado no código-fonte para identificar a opção no respectivo
            campo do formulário.

    [`webdriver.Chrome`]: https://selenium-python.readthedocs.io
    /api.html#module-selenium.webdriver.chrome.webdriver
    [`webdriver.Firefox`]: https://selenium-python.readthedocs.io
    /api.html#module-selenium.webdriver.firefox.webdriver
    """

    def __init__(
        self,
        driver: webdriver.Chrome | webdriver.Firefox,
        rotulo: str | None = None,
        valor: str | None = None,
    ):
        super().__init__(driver, "tpProducao", rotulo=rotulo, valor=valor)

    def ler_rotulo_ativo(self) -> str | None:
        """Lê o rótulo ativo do elemento de definição do tipo de produção."""
        return self.driver.find_element(By.ID, "spTipoProducao").text or None

    def ler_valor_ativo(self) -> str | None:
        """Lê o valor ativo do elemento de definição do tipo de produção."""
        return (
            self.driver.execute_script(
                "return document.getElementById('tpProducao').value",
            )
            or None
        )


class FormularioConsulta(FormularioAbstrato):  # noqa: WPS230
    """Representa a página web de consulta dos dados de produção do SISAB.

    Intermedia a interação com um formulário do SISAB responsável pela geração
    de relatórios de produção da atenção primária à saúde. Os parâmetros
    passados na criação das instâncias dessa classe são aplicados ao
    formulário como filtros, permitindo a definição do leiaute do relatório
    gerado. A classe modela a página de consulta disponível
    [aqui][SISAB-Produção], e utiliza os mesmos rótulos e valores para
    especificar os filtros e variáveis que compõem os relatórios.

    Após a definição dos filtros, a solicitação de geração do relatório deve
    ser enviada para o SISAB através do método [`executar_consulta()`][]. A
    resposta é armazenada no atributo `resultado` como um [DataFrame][].

    Parâmetros:
        driver: Instância do driver do Selenium.
        competencias: Lista, [DatetimeIndex][] ou outro iterável de objetos
            dos tipos [`date`][] ou [`Timestamp`][] com o conjunto de competências mensais a serem computadas no relatório.
        unidade_geografica: nível geográfico para agregação da produção. Deve
            ser um entre 'Brasil', 'Macrorregião', 'Estado', 'Região de Saúde'
            ou 'Municípios' (*atualmente, apenas 'Brasil' e 'Municípios' se
            encontram implementados*). Por padrão, o valor é 'Brasil'.
        linha_relatorio: variável a ser utilizada como linha do relatório. Deve
            ser uma das linhas disponíveis no SISAB. Por padrão, o valor é
            'Brasil'.
        coluna_relatorio: variável a ser utilizada como coluna do relatório.
            Deve ser uma das colunas disponíveis no SISAB. Por padrão, o valor
            é 'Tipo de Produção'.
        tipo_equipe: lista de tipos de equipe de atenção primária cujas
            produções serão computadas no relatório. Os componentes da lista devem ser uma das opções disponíveis no SISAB - ou 'Selecionar
            Todos', para aceitar todos os tipos de equipe. Por padrão, o valor
            é uma lista vazia, o que corresponde a não utilizar o filtro.
        categoria_profissional:  lista de categorias de profissionais cujas
            produções serão computadas no relatório. Os componentes da lista devem ser uma das opções disponíveis no SISAB - ou 'Selecionar
            Todos', para aceitar todos os tipos de equipe. Por padrão, o valor
            é uma lista vazia, o que corresponde a não utilizar o filtro.
        idade_intervalo: tupla contendo a idade máxima e mínima dos usuários
            cujos contatos assistenciais serão computados no relatório. Por
            padrão, o filtro é (0,0), o que equivale a não utilizar o filtro.
        idade_unidade: unidade de medida para o filtro de idade dos usuários.
            Se informado, deve ser um valor entre 'Ignorar', 'Dias' e 'Anos'.
            Por padrão, o valor é 'Ignorar', o que corresponde a não utilizar
            o filtro.
        sexo: lista de sexos para filtrar os usuários cujos contatos
            assistenciais serão computados como produção no relatório. Os
            componentes da lista devem ser valores entre 'Masculino' e
            'Feminino' - ou 'Selecionar Todos', para aceitar todos os sexos.
            Por padrão, o valor é uma lista vazia, o que corresponde a não
            utilizar o filtro.
        local_atendimento: lista de locais de atendimento para filtrar as
            produções que serão computadas no relatório. Os componentes da
            lista devem ser valores disponíveis no SISAB - ou 'Selecionar
            Todos', para aceitar todos os locais de atendimento. Por padrão, o
            valor é uma lista vazia, o que corresponde a não utilizar o filtro.
        tipo_atendimento: lista de tipos de atendimento para filtrar as
            produções que serão computadas no relatório. Os componentes da
            lista devem ser valores disponíveis no SISAB - ou 'Selecionar
            Todos', para aceitar todos os tipos de atendimento. Por padrão, o valor é uma lista vazia, o que corresponde a não utilizar o filtro.
        tipo_producao: rótulo do tipo de produção a ser computada no relatório.
            Deve ser um valor entre 'Atendimento Individual', 'Atendimento Odontológico', 'Procedimento' ou 'Visita Domiciliar' (apenas
            'Atendimento Individual' está plenamente implementado). Por padrão,
            o valor é `None`, o que equivale a não definir o filtro (nesse
            caso, um dos parâmetros `linha_relatorio` ou `coluna_relatorio`
            devem assumir o valor 'Tipo de Produção').
        estado: sigla da unidade federativa dos municípios cujas produções
            serão computadas na consulta. Deve ser informado caso o parâmetro `unidade_geografica` seja 'Estado' ou 'Municípios'.
        municipios: lista de códigos de sete dígitos dos municípios cujas
            produções serão computadas na consulta. Deve ser informado caso o parâmetro `unidade_geografica` seja 'Municípios'.
        aleitamento_materno: estatus de aleitamento materno dos usuários cujos
            atendimentos individuais serão computados no relatório. Os
            componentes da lista devem ser opções disponíveis no SISAB. Por
            padrão, o valor é uma lista vazia, o que corresponde a não usar o
            filtro.
        acoes_nasf_academia_saude: lista com tipos de ações desenvolvidas pelo
            NASF ou pelos pólos de academia da saúde. Os componentes da lista
            devem ser opções disponíveis no SISAB. Por padrão, o valor é uma
            lista vazia, o que corresponde a não usar o filtro.
        problema_condicao_avaliada: lista de problemas ou condições informadas como as
            principais avaliadas no atendimento. Os componentes da lista devem
            ser opções disponíveis no SISAB. Por padrão, o valor é uma lista
            vazia, o que corresponde a não usar o filtro.
        vacinacao_em_dia: lista de estatus de vacinação dos usuários atendidos.
            Os componentes da lista devem ser opções disponíveis no SISAB. Por
            padrão, o valor é uma lista vazia, o que corresponde a não usar o
            filtro.
        conduta: lista de condutas informadas como desfecho dos atendimentos.
            Os componentes da lista devem ser opções disponíveis no SISAB. Por
            padrão, o valor é uma lista vazia, o que corresponde a não usar o
            filtro.
        racionalidade_saude: tipo de racionalidade em saúde do atendimento. Os
            componentes da lista devem ser opções disponíveis no SISAB. Por
            padrão, o valor é uma lista vazia, o que corresponde a não usar o
            filtro.
        ciap: lista de códigos da Classificação Internacional de Atenção
            Primária (CIAP2) informados como queixa ou condição de saúde
            motivadores do atendimento. Os componentes da lista devem ser os
            códigos alfanuméricos da condição correspondente no CIAP2, sem
            espaços ou caracteres especiais. Por padrão, o valor é uma lista
            vazia, o que corresponde a não usar o filtro.
        cid: lista de códigos da Classificação Internacional de Doenças
            (CID-10) informados como queixa ou condição de saúde motivadores
            do atendimento. Os componentes da lista devem ser os códigos
            alfanuméricos da condição correspondente no CID-10, sem espaços ou
            caracteres especiais. Por padrão, o valor é uma lista vazia, o que
            corresponde a não usar o filtro.


    [SISAB-Produção]: https://sisab.saude.gov.br/paginas/acessoRestrito
    /relatorio/federal/saude/RelSauProducao.xhtml
    [DatetimeIndex]: https://pandas.pydata.org/pandas-docs/stable/reference
    /api/pandas.DatetimeIndex.html
    [`executar_consulta()`]:
    impulsoetl.sisab.producao.FormularioConsulta.executar_consulta()
    [DataFrame]: https://pandas.pydata.org/pandas-docs/stable/reference/api
    /pandas.DataFrame.html
    [`date`]: https://docs.python.org/3/library/datetime.html#date-objects
    [``Timestamp``]: https://pandas.pydata.org/pandas-docs/stable/reference
    /api/pandas.Timestamp.html
    """

    def __init__(  # noqa: WPS211  # liberar muitos argumentos
        self,
        driver: webdriver.Chrome | webdriver.Firefox,
        # parâmetros para todos os relatórios de produção
        competencias: Iterable[DatetimeLike] | pd.DatetimeIndex = [],
        unidade_geografica: str = "Brasil",
        linha_relatorio: str = "Brasil",
        coluna_relatorio: str = "Tipo de Produção",
        tipo_equipe: Iterable[str] = [],
        categoria_profissional: Iterable[str] = [],
        idade_intervalo: tuple[int, int] = (0, 0),
        idade_unidade: str = "Ignorar",
        sexo: Iterable[str] = [],
        local_atendimento: Iterable[str] = [],
        tipo_atendimento: Iterable[str] = [],
        tipo_producao: str | None = None,
        estado: str | None = None,
        municipios: Iterable[str] | None = [],
        # parâmetros para atendimentos individuais
        aleitamento_materno: Iterable[str] = [],
        acoes_nasf_academia_saude: Iterable[str] = [],
        problema_condicao_avaliada: Iterable[str] = [],
        vacinacao_em_dia: Iterable[str] = [],
        conduta: Iterable[str] = [],
        racionalidade_saude: Iterable[str] = [],
        ciap: Iterable[str] = [],
        cid: Iterable[str] = [],
        # TODO: parâmetros para atendimentos odontológicos
        # TODO: parâmetros para procedimentos
        # TODO: parâmetros para visitas domiciliares
    ):

        super().__init__(RELATORIO_URL, driver)

        self._metadados = ConsultaProducaoMetadados(
            linha_relatorio=linha_relatorio,
            coluna_relatorio=coluna_relatorio,
            unidade_geografica=unidade_geografica,
        )

        if linha_relatorio == "Problema/Condição Avaliada":
            linha_relatorio = "Probl/ Condição Avaliada"

        if coluna_relatorio == "Problema/Condição Avaliada":
            coluna_relatorio = "Probl/ Condição Avaliada"

        self.competencias = FiltroCompetencias(self.driver, *competencias)

        self.unidade_geografica = FiltroUnico(
            self.driver,
            "unidGeo",
            rotulo=(
                None if unidade_geografica == "Brasil" else unidade_geografica
            ),
        )

        if unidade_geografica == "Municípios":
            self.estado = FiltroUnico(
                self.driver,
                "estadoMunicipio",
                rotulo=estado,
            )
            self.municipios = FiltroMultiplo(
                self.driver,
                "municipios",
                valores=municipios,
            )

        # TODO: adicionar outros recortes geográficos
        unidades_geograficas_implementadas = ["Brasil", "Municípios"]
        if (
            unidade_geografica
            and unidade_geografica not in unidades_geograficas_implementadas
        ):
            raise NotImplementedError(
                "Geração de relatórios por outras unidades geográficas que não"
                + " municípios não é suportada pelo ETL no momento.",
            )

        self.linha_relatorio = FiltroUnico(
            self.driver,
            "selectLinha",
            rotulo=(None if linha_relatorio == "Brasil" else linha_relatorio),
        )

        self.coluna_relatorio = FiltroUnico(
            self.driver,
            "selectcoluna",
            rotulo=(
                None
                if coluna_relatorio == "Tipo de Produção"
                else coluna_relatorio
            ),
        )

        self.tipo_producao = FiltroTipoProducao(
            self.driver,
            rotulo=tipo_producao,
        )

        self.tipo_equipe = FiltroMultiplo(
            self.driver,
            "filtroEquipeProf",
            rotulos=tipo_equipe,
        )

        self.categoria_profissional = FiltroMultiplo(
            self.driver,
            "categoriaProfissional",
            rotulos=categoria_profissional,
        )

        self.idade = FiltroIdade(
            self.driver,
            idade_de=idade_intervalo[0],
            idade_ate=idade_intervalo[1],
            unidade=idade_unidade,
        )

        self.sexo = FiltroMultiplo(
            self.driver,
            "sexo",
            rotulos=sexo,
        )

        self.local_atendimento = FiltroMultiplo(
            self.driver,
            "localAtendimento",
            rotulos=local_atendimento,
        )

        self.tipo_atendimento = FiltroMultiplo(
            self.driver,
            "tipoAtendimento",
            rotulos=tipo_atendimento,
        )

        if tipo_producao == "Atendimento Individual":

            self.aleitamento_materno = FiltroMultiplo(
                self.driver,
                "aleitamentoMaterno",
                rotulos=aleitamento_materno,
            )

            self.acoes_nasf_academia_saude = FiltroMultiplo(
                self.driver,
                "acaoNasfPoloAcademiaSaude",
                rotulos=acoes_nasf_academia_saude,
            )

            self.problema_condicao_avaliada = FiltroMultiplo(
                self.driver,
                "condicaoAvaliada",
                rotulos=problema_condicao_avaliada,
            )

            self.vacinacao_em_dia = FiltroMultiplo(
                self.driver,
                "vacina",
                rotulos=vacinacao_em_dia,
            )

            self.conduta = FiltroMultiplo(
                self.driver,
                "conduta",
                rotulos=conduta,
            )

            self.racionalidade_saude = FiltroMultiplo(
                self.driver,
                "racionalidade",
                rotulos=racionalidade_saude,
            )

            self.ciap_cid = FiltroCiapCid(
                self.driver,
                ciaps=ciap,
                cids=cid,
            )

        # TODO
        if tipo_producao == "Atendimento Odontológico":
            raise NotImplementedError(
                "Geração de relatórios de atendimentos odontológicos não é "
                + "suportada pelo ETL no momento.",
            )

        # TODO
        if tipo_producao == "Procedimento":
            raise NotImplementedError(
                "Geração de relatórios de procedimentos não é suportada pelo "
                + "ETL no momento.",
            )

        # TODO
        if tipo_producao == "Visita Domiciliar":
            raise NotImplementedError(
                "Geração de relatórios de visitas domiciliares não é suportada"
                + " pelo ETL no momento.",
            )

    @property
    def metadados(self) -> ConsultaProducaoMetadados:
        """Metadados da consulta, incluindo filtros ativos neste instante.

        Objeto [`ConsultaProducaoMetadados`][] contendo informações das
        variáveis selecionadas como linhas e colunas no formulário, bem como
        os demais filtros ativos no momento.

        [`ConsultaProducaoMetadados`]:
        impulsoetl.sisab.producao.ConsultaProducaoMetadados
        """

        # NOTE: envelopar os metadados em uma propriedade permite devolver
        # sempre os filtros ativos no momento, mesmo que tenham sido alterados
        # após a inicialização do objeto.

        if self._metadados.unidade_geografica == "Municípios":
            self._metadados.filtros_ativos[
                "municipios"
            ] = self.municipios.ler_valores_ativos()

        self._metadados.filtros_ativos.update(
            {
                "competencias": self.competencias.ler_competencias_ativas(),
                "tipo_equipe": self.tipo_equipe.ler_rotulos_ativos(),
                "categoria_profissional": self.categoria_profissional.ler_rotulos_ativos(),
                "idade": [self.idade.faixa_etaria],
                "sexo": self.sexo.ler_rotulos_ativos(),
                "local_atendimento": self.local_atendimento.ler_rotulos_ativos(),
                "tipo_atendimento": self.tipo_atendimento.ler_rotulos_ativos(),
                "tipo_producao": [self.tipo_producao.ler_rotulo_ativo()],
            },
        )

        # caso não haja um filtro de Tipo de Produção, considerar todos tipos
        if not self._metadados.filtros_ativos.get("tipo_producao"):
            # TODO: ler valores disponíveis da página
            self._metadados.filtros_ativos["tipo_producao"] = [
                "Atendimento Individual",
                "Atendimento Odontológico",
                "Procedimento",
                "Visita Domiciliar",
            ]

        if self._metadados.filtros_ativos["tipo_producao"] == [
            "Atendimento Individual",
        ]:
            self._metadados.filtros_ativos.update(
                {
                    "aleitamento_materno": (
                        self.aleitamento_materno.ler_rotulos_ativos()
                    ),
                    "acoes_nasf_academia_saude": (
                        self.acoes_nasf_academia_saude.ler_rotulos_ativos()
                    ),
                    "problema_condicao_avaliada": (
                        self.problema_condicao_avaliada.ler_rotulos_ativos()
                    ),
                    "vacinacao_em_dia": (
                        self.vacinacao_em_dia.ler_rotulos_ativos()
                    ),
                    "conduta": self.conduta.ler_rotulos_ativos(),
                    "racionalidade_saude": (
                        self.racionalidade_saude.ler_rotulos_ativos()
                    ),
                    "ciap_cid": self.ciap_cid.listar_rotulos_ativos(),
                },
            )

        return self._metadados

    @property
    def resultado(self) -> RelatorioProducao | None:
        """Resultado da consulta por produção.

        Objeto [`RelatorioProducao`][] contendo os dados do relatório
        retornados pela execução da consulta no SISAB, bem como os metadados
        da consulta que o gerou. Caso nenhuma consulta tenha sido executada
        com o método [`executar_consulta()`][] ainda, retorna `None`.

        [`RelatorioProducao`]: impulsoetl.sisab.producao.RelatorioProducao
        [`executar_consulta()`]:
        impulsoetl.sisab.producao.FormularioConsulta.executar_consulta
        """
        if self._resultado:
            return RelatorioProducao(
                self._resultado,
                metadados_consulta=self.metadados,
            )
        return None


class FiltrosAtivosDict(MutableMapping):
    """Dicionário que associa nomes de filtros aos seus rótulos ou valores.

    Esta estrutura funciona exatamente como um dicionário Python, mas é
    destinada especificamente a armazenar pares de nome (normalizados) de
    filtros do SISAB com listas ou outros iteráveis contendo ou seus rótulos ou valores ativos.

    Diferentemente de um dicionário comum, esta estrutura realiza uma crítica
    prévia ao se adicionar novos rótulos ou valores, evitando adicionar valores
    vazios ou do tipo 'Selecionar Todos'.
    """

    def __init__(self, *args, **kwargs):
        self.dicionario = dict()
        self.update(dict(*args, **kwargs))

    def __getitem__(self, nome_filtro: str) -> Iterable[str | DatetimeLike]:
        return self.dicionario[nome_filtro]

    def __setitem__(
        self,
        nome_filtro: str,
        rotulos_ou_valores: Iterable[str | DatetimeLike],
    ) -> None:
        # TODO: transformar datas em textos para filtro de competências
        if rotulos_ou_valores and any(rotulos_ou_valores):
            self.dicionario[nome_filtro] = [
                rotulo_ou_valor
                for rotulo_ou_valor in rotulos_ou_valores
                if rotulo_ou_valor != "Selecionar Todos"
            ]

    def __delitem__(self, nome_filtro: str):
        del self.dicionario[nome_filtro]

    def __iter__(self):
        return iter(self.dicionario)

    def __len__(self):
        return len(self.dicionario)


class ConsultaProducaoMetadados(object):
    """Especificações utilizadas para a geração de um relatório de produção.

    Parâmetros:
        linha_relatorio: Variável selecionada como linha do relatório no
            formulário que gerou a consulta.
        coluna: Variável selecionada como coluna do relatório no formulário
            que gerou a consulta.
        unidade_geografica: Unidade geográfica utilizada para a consulta.
    """

    def __init__(
        self,
        linha_relatorio: str,
        coluna_relatorio: str,
        unidade_geografica: str,
    ):
        self.linha_relatorio = linha_relatorio
        self.coluna_relatorio = coluna_relatorio
        self.unidade_geografica = unidade_geografica
        self.filtros_ativos = FiltrosAtivosDict()

    @property
    def rotulos_linhas(self):
        """Rótulos utilizados como categorias da variável linha da consulta."""
        return self.filtros_ativos[tratar_nomes_campos(self.linha_relatorio)]

    @property
    def rotulos_colunas(self):
        """Rótulos utilizados como categorias da variável coluna da consulta."""
        return self.filtros_ativos[tratar_nomes_campos(self.coluna_relatorio)]


class RelatorioProducao(RelatorioAbstrato):
    """Relatório de produção gerado pelo SISAB.

    Contém os dados de um relatório de produção gerado pelo SISAB, e
    informações sobre a consulta que lhe deu origem. Também possui métodos para facilitar a transformação do relatório
    em um formato mais apropriado para exportação.

    Parâmetros:
        relatorio_csv: string contendo o relatório no formato gerado pelo
            SISAB.
        metadados: instância de [FormularioProducao][] que gerou o
            relatório.
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov. Necessário para aplicar
            transformações ao relatório, de acordo com as especificações
            definidas no banco de dados.

    Atributos:
        dados: [DataFrame][] contendo os dados do relatório de produção.
        metadados_consulta: objeto [ConsultaProducaoMetadados][] contendo
            informações sobre a consulta que gerou o relatório.

    [DataFrame]: https://pandas.pydata.org/pandas-docs/stable/reference/api
    /pandas.DataFrame.html
    [ConsultaProducaoMetadados]:
        impulsoetl.sisab.producao.ConsultaProducaoMetadados
    [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
    """

    def __init__(
        self,
        relatorio_csv: str,
        metadados_consulta: ConsultaProducaoMetadados,
        sessao: Session | None = None,
    ):
        super().__init__(relatorio_csv)
        self.metadados_consulta = metadados_consulta
        self.sessao = sessao

    def aplicar_transformacoes(self) -> None:
        """Padroniza dados do relatório de produção.

        Aplica diversas transformações nos dados do relatório de produção,
        preparando-os para a exportação para o banco de dados da ImpulsoGov.
        Mais especificamente, aplica as seguintes transformações:

        - Remove colunas sem nome (sobressalentes) geradas pela existência de
        separadores a mais no final do relatório CSV importado do SISAB.
        - Repõe os nomes completos das categorias da variável utilizada como
        coluna ao gerar o relatório no SISAB (no relatório gerado pelo SISAB,
        essas categorias são truncadas aos primeiros 30 caracteres).
        - Adiciona as variáveis utilizadas como filtros extras (i.e., nem
        como linhas, nem como colunas) como colunas no DataFrame. Atualmente,
        apenas os filtros com um único valor apontado são adicionados.
        - *Verticaliza* os dados do relatório, transformando as categorias da
        variável selecionada como coluna no SISAB em linhas.
        - Normaliza os nomes das colunas geradas pelo SISAB, removendo
        palavras sem significado (de, da, para, etc.), substituindo espaços e
        caracteres especiais por underlines, passando para minúsculas e
        utilizando nomes padronizados para colunas de identificadores
        compartilhadas com outras tabelas do banco de dados da ImpulsoGov.
        - Garante que os tipos de dados das colunas sejam consistentes com os
        tipos esperados pelo banco de dados da ImpulsoGov.
        - Transforma os IDs utilizados pelo SISAB (ex.: código SUS de seis
        dígitos do município) e datas de referência do período nos respectivos
        identificadores únicos utilizados no banco de dados da ImpulsoGov.
        """
        pipeline = compose_left(
            self._remover_colunas_sobressalentes,
            self._repor_nomes_colunas_truncados,
            self._adicionar_filtros_como_colunas,
            self._verticalizar_colunas,
            self._normalizar_nomes_colunas,
            self._impor_tipos,
            self._aplicar_ids_impulso,
        )
        self.dados: pd.DataFrame = pipeline(self.dados)

    @staticmethod
    def _remover_colunas_sobressalentes(df: pd.DataFrame) -> pd.DataFrame:
        """Remove colunas sem nome.

        Remove colunas sem nome (sobressalentes) geradas pela existência de
        separadores a mais no final do relatório CSV importado do SISAB.
        """
        colunas_sem_nome = df.columns.str.match("Unnamed")
        return df.loc[:, ~colunas_sem_nome]

    def _repor_nomes_colunas_truncados(self, df: pd.DataFrame) -> pd.DataFrame:
        """Restaura caracteres truncados em nomes das colunas de resultados.

        Repõe os nomes completos das categorias da variável utilizada como
        coluna ao gerar o relatório no SISAB (no relatório gerado pelo SISAB,
        essas categorias são truncadas aos primeiros 30 caracteres).
        """
        rotulos_abreviados = {
            rotulo[: min(len(rotulo), 30)]: rotulo
            for rotulo in self.metadados_consulta.rotulos_colunas
        }
        return df.rename(
            columns=lambda col: rotulos_abreviados.get(col, col),
        )

    def _adicionar_filtros_como_colunas(
        self,
        df: pd.DataFrame,
    ) -> pd.DataFrame:
        """Insere campos referentes aos filtros utilizados na consulta.

        Adiciona as variáveis utilizadas como filtros extras (i.e., nem
        como linhas, nem como colunas) como colunas no DataFrame. Atualmente,
        apenas os filtros com um único valor apontado são adicionados.
        """

        for (
            nome_filtro,
            rotulos_ativos,
        ) in self.metadados_consulta.filtros_ativos.items():
            if len(rotulos_ativos) == 1:
                df[nome_filtro] = rotulos_ativos[0]

        return df

    def _verticalizar_colunas(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remodela valores em colunas de um DataFrame para linhas.

        *Verticaliza* os dados do relatório, transformando as categorias da
        variável selecionada como coluna no SISAB em linhas.
        """

        # checar quais dos nomes de campos do resultado são referentes à
        # variável linha do relatório
        value_vars = [
            rotulo
            for rotulo in self.metadados_consulta.rotulos_colunas
            if rotulo in df.columns
        ]
        # se nenhum rótulo da variável linha for encontrado, utilizar todos os
        # campos até a última coluna lida como string
        if not value_vars:
            colunas_strings = df.select_dtypes("object").columns
            value_vars = df.loc[:, : colunas_strings[-1]].columns.tolist()

        # manter demais colunas como identificadores
        id_vars = [col for col in df.columns if col not in value_vars]

        # passar variáveis das colunas para formato longo
        return df.melt(
            id_vars=id_vars,
            value_vars=value_vars,
            var_name=self.metadados_consulta.coluna_relatorio,
            value_name="quantidade_registrada",
        )

    @staticmethod
    def _normalizar_nomes_colunas(df: pd.DataFrame) -> pd.DataFrame:
        """Remove caracteres especiais e padroniza caixa e nome das colunas.

        Normaliza os nomes das colunas geradas pelo SISAB, removendo
        palavras sem significado (de, da, para, etc.), substituindo espaços e
        caracteres especiais por underlines, passando para minúsculas e
        utilizando nomes padronizados para colunas de identificadores
        compartilhadas com outras tabelas do banco de dados da ImpulsoGov.
        """

        return df.rename(columns=tratar_nomes_campos).rename(
            columns=lambda col: DE_PARA_RELATORIO_PRODUCAO.get(col, col),
        )

    @staticmethod
    def _impor_tipos(df: pd.DataFrame) -> pd.DataFrame:
        """Impõe tipos de dados para as colunas.

        Garante que os tipos de dados das colunas sejam consistentes com os
        tipos esperados pelo banco de dados da ImpulsoGov.
        """

        return df.astype(
            {
                col: tipo
                for col, tipo in TIPOS_RELATORIO_PRODUCAO.items()
                if col in df.columns
            },
        )

    def _aplicar_ids_impulso(self, df: pd.DataFrame) -> pd.DataFrame:
        """Substitui colunas ID Sus e Competência pelos IDs do BD da Impulso.

        Transforma os IDs utilizados pelo SISAB (ex.: código SUS de seis
        dígitos do município) e datas de referência do período nos respectivos
        identificadores únicos utilizados no banco de dados da ImpulsoGov.
        """
        if not self.sessao:
            raise AttributeError(
                "O atributo `sessao` deve ser definido para que se possa "
                + "consultar os identificadores únicos das categorias no "
                + "banco de dados da Impulso.",
            )
        return (
            df.transform_column(
                "municipio_id_sus",
                function=lambda id_: id_sus_para_id_impulso(
                    sessao=self.sessao,
                    id_sus=id_,
                ),
                dest_column_name="unidade_geografica_id",
            )
            .drop(columns=["municipio_id_sus"])
            .transform_column(
                "competencias",
                function=lambda dt: (
                    periodo_por_data(data=dt, sessao=self.sessao).id
                ),
                dest_column_name="periodo_id",
            )
            .drop(columns=["competencias"])
        )


def gerar_nome_tabela(
    variaveis: Iterable[str],
    unidade_geografica: str = "municipios",
) -> str:
    """Gera o nome da tabela correspondente a um relatório de produção no BD.

    Com base nas variáveis selecionadas para o cruzamento de informações em um
    relatório de produção gerado pelo SISAB, obtém o nome padronizado da tabela
    de destino correspondente no banco de dados da ImpulsoGov.

    Argumentos:
        variaveis: Qualquer iterável contendo os nomes das variáveis utilizadas
            como linhas, colunas ou filtros na geração do relatório no SISAB.
            Os nomes não precisam estar normalizados, já que a função se
            encarrega de remover as palavras vazias e ajustar a caixa e os
            caracteres permitidos.
        unidade_geografica: Tipo de unidade geográfica utilizada na agregação
            dos dados do relatório.

    Retorna:
        O nome da tabela de destino para carga no banco de dados da ImpulsoGov.
    """

    variaveis = map(tratar_nomes_campos, variaveis)  # normalizar nomes
    variaveis = set(variaveis).difference(COLUNAS_MINIMAS)  # excluir comuns
    variaveis = sorted(variaveis)  # colocar em ordem alfabética
    unidade_geografica = tratar_nomes_campos(unidade_geografica)

    nome_tabela = "sisab_producao_{}_por_".format(
        unidade_geografica,
    ) + "_por_".join(variaveis)

    # truncar se tiver mais de 63 caracteres
    nome_tabela = nome_tabela[: min(len(nome_tabela), 63)]

    return "dados_publicos." + nome_tabela


def gerar_modelo_impulso(
    tabela_nome: str,
    variaveis: Iterable[str],
    se_ja_existente: str = "manter",
) -> TabelaProducao:
    """Gera uma classe que modela o relatório de produção no BD da Impulso.

    Devolve uma representação do relatório de produção como tabela
    no banco de dados da ImpulsoGov. A correspondência entre a
    classe resultante e a respectiva tabela no banco de dados é realizada
    utilizando o estilo de [mapeamento declarativo do SQLAlchemy][]. As
    colunas `periodo_id`, `unidade_geografica_id`, `tipo_producao` e
    `quantidade_registrada` estão sempre presentes, enquanto as demais
    colunas encontradas no relatório processado são dinamicamente
    acrescentadas ao modelo.

    Argumentos:
        tabela_nome: Nome da tabela de destino no banco de dados da ImpulsoGov.
        variaveis: Qualquer iterável contendo os nomes das variáveis utilizadas
            como linhas, colunas ou filtros na geração do relatório no SISAB.
            Os nomes não precisam estar normalizados, já que a função se
            encarrega de remover as palavras vazias e ajustar a caixa e os
            caracteres permitidos.
        se_ja_existente: Ação a ser tomada caso um modelo já tenha sido
            definido para uma tabela de mesmo nome. Se for `'manter'` (padrão),
            a tabela já definida é retornada. Se for `'extender'`, as colunas
            geradas a partir das variáveis informadas serão adicionadas à
            tabela original, e a tabela modificada será retornada.

    Retorna:
        Uma classe que modela a tabela correspondente ao relatório de
        produção no banco de dados da ImpulsoGov.

    [mapeamento declarativo do SQLAlchemy]: https://docs.sqlalchemy.org
    /en/14/orm/mapping_styles.html#orm-declarative-mapping
    """

    # NOTE: talvez esta função possa ser útil em outros lugares; pode ser
    # movida eventualmente para outro módulo.

    # definir dinamicamente as colunas restantes ao modelo de tabela
    modelos_colunas_especificas: dict[str, sa.Column] = {}
    variaveis = map(tratar_nomes_campos, variaveis)  # normalizar nomes
    for variavel in variaveis:
        if variavel not in COLUNAS_MINIMAS:
            modelos_colunas_especificas[variavel] = sa.Column(
                variavel,
                sa.Text,
                nullable=False,
            )

    opcoes_tabela: dict[str, dict[str, Any]] = {"__table_args__": {}}

    opcoes_tabela["__table_args__"]["schema"] = "dados_publicos"

    if se_ja_existente.lower() == "manter":
        opcoes_tabela["__table_args__"]["keep_existing"] = True
    elif se_ja_existente.lower() == "estender":
        opcoes_tabela["__table_args__"]["extend_existing"] = True
    else:
        raise ValueError(
            "Valor não reconhecido para o argumento `se_ja_existente`: "
            + "'{}'.".format(se_ja_existente),
        )

    # define dinamicamente uma subclasse de tabela de produção contendo as
    # colunas comuns (município, período)
    modelo = type(
        tabela_nome,
        (TabelaProducao, Base),
        dict(modelos_colunas_especificas, **opcoes_tabela),
    )
    Base.metadata.create_all()
    return modelo


def carregar_relatorio_producao(
    sessao: Session,
    dados_producao: pd.DataFrame,
    modelo_tabela: TabelaProducao,
) -> int:
    """Carrega relatório de produção na base de dados da ImpulsoGov.

    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov.
        dados_producao: [`DataFrame`][] contendo os dados do relatório a
            ser carregado.
        modelo_tabela: objeto de uma subclasse de [`TabelaProducao`][] que
            mapeia a tabela correspondente a esse relatório no banco de dados
            da ImpulsoGov.

    Retorna:
        Código de saída do processo de carregamento. Se o carregamento
        for bem sucedido, o código de saída será `0`.

    [`DataFrame`]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    [`RelatorioProducao`]: impulsoetl.sisab.producao.RelatorioProducao
    [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
    [`TabelaProducao`]: impulsoetl.sisab.modelos.TabelaProducao
    """

    # apontar carregamento de linhas do DataFrame na tabela do banco
    adicionados = 0
    for linha_relatorio in dados_producao.itertuples():
        dicionario_linha = linha_relatorio._asdict()
        del dicionario_linha["Index"]
        dicionario_linha["id"] = uuid.uuid4().hex
        relatorio_orm = modelo_tabela(**dicionario_linha)
        sessao.add(relatorio_orm)
        adicionados += 1

    logger.info(
        "Carregamento concluído para a tabela `{tabela_nome}`: "
        + "adicionadas {linhas_adicionadas} novas linhas.",
        tabela_nome=modelo_tabela.__tablename__,
        linhas_adicionadas=adicionados,
    )
    return 0


@repetir_por_ano_mes(data_inicio_minima="2013-04-01")
def obter_relatorio_producao(
    sessao: Session,
    tabela_destino: str,
    variaveis: tuple[str, str],
    ano: int | None,
    mes: int | None,
    unidades_geograficas_ids: list[str],
    unidade_geografica_tipo: str = "Municípios",
    tipo_producao: str = "Atendimento Individual",
    teste: bool = False,
    atualizar_captura: bool = True,
) -> None:  # TODO: transformar em gerador (??)
    """Extrai, transforma e carrega dados de produção para o BD da Impulso.

    Define uma sequência de operações para extrair, transformar e carregar
    dados de produção do SISAB para o banco de dados da ImpulsoGov. A função
    suporta o caso de uso mais comum de ETL de dados do SISAB: o cruzamento
    de duas variáveis disponíveis no [formulário de consulta][] de dados de produção do SISAB para um mesmo tipo de atendimento (individual,
    odontológico ⃰, procedimento ⃰, visita domiciliar ⃰), agregados por
    município e por mês.

     ⃰ A extração para esses tipos de produção ainda não se encontra
    implementada (nov./2021).

    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov.
        tabela_destino: Nome da tabela no banco de dados da ImpulsoGov para
            onde serão carregados os dados capturados.
        variaveis: tupla com os nomes das duas variáveis que se pretende cruzar
            na obtenção do relatório de produção para cada município e
            competência. Os nomes das variáveis devem ser idênticos a como
            estão escritas no formulário do SISAB.
        ano: ano da competência a ser considerada.
        mes: mês da competência a ser considerada.
        data_inicio: objeto [`datetime.date`][] ou [`pd.Timestamp`][] que pode
            ser especificado no lugar do ano e do mês para extrair, transformar
            e carregar dados de um intervalo de competências, a partir daquela
            data. Deve ser igual ou posterior à data da primeira competência
            disponível no SISAB.
        data_fim = objeto [`datetime.date`][] ou [`pd.Timestamp`][] que pode
            ser especificado em conjunto com o parâmetro `data_inicio` para extrair, transformar e carregar dados de um intervalo de
            competências. Caso seja especificado um valor de `data_inicio` e a
            `data_fim` não seja especificada, o intervalo de competências será
            extraído até a última competência disponível.
        unidades_geograficas_ids: lista de identificadores das unidades
            geográficas cujos dados se pretende capturar, expressas como
            strings com as representações hexadecimais dos UUIDs respectivos
            no banco de dados da ImpulsoGov.
        unidade_geografica_tipo: Tipo de unidade geográfica utilizada na
            agregação dos dados do relatório. Atualmente, o único valor
            suportado é `'Municípios'`.
        tipo_producao: rótulo do tipo de produção a ser computada no relatório.
            Deve ser um valor entre 'Atendimento Individual',
            'Atendimento Odontológico', 'Procedimento' ou 'Visita Domiciliar'
            (apenas 'Atendimento Individual' está plenamente implementado).
            Por padrão, o valor é 'Atendimento Individual'.
        atualizar_captura: Indica se a tabela
            "configuracoes.capturas_agendadas" deve ser atualizada para
            refletir a realização de uma nova captura (*legado*; para novos
            fluxos de captura, utilize `False`). Para compatibilidade com as
            versões antigas, o padrão é `True`.
        teste: Indica se as modificações devem ser de fato escritas no banco de
            dados (`False`, padrão). Caso seja `True`, as modificações são
            adicionadas à uma transação, e podem ser revertidas com uma chamada
            posterior ao método [`Session.rollback()`][] da sessão gerada com o
            SQLAlchemy.

    [formulário de consulta]: https://sisab.saude.gov.br/paginas/acessoRestrito/relatorio/federal/saude/RelSauProducao.xhtml
    [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
    [`datetime.date`]: https://docs.python.org/3/library/datetime.html#datetime.date
    [`pd.Timestamp`]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Timestamp.html
    [`Session.rollback()`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session.rollback
    """
    logger.info(
        "Iniciando captura de relatório de produção com as variáveis "
        + "{variaveis} para {num_unidades_geograficas} "
        + "{unidade_geografica_tipo} na competencia de {mes}/{ano}.",
        variaveis="['" + "', '".join(variaveis) + "']",
        num_unidades_geograficas=len(unidades_geograficas_ids),
        unidade_geografica_tipo=unidade_geografica_tipo.lower(),
        ano=ano,
        mes=mes,
    )
    filtros: dict[str, list[str]] = {
        tratar_nomes_campos(variavel): [
            "Selecionar Todos",
        ]
        for variavel in variaveis
        if variavel != "Tipo de Produção"
    }
    modelo_tabela = gerar_modelo_impulso(
        tabela_nome=tabela_destino,
        variaveis=variaveis,
    )
    competencia = pd.Timestamp(ano, mes, 1)
    with criar_geckodriver() as driver:
        for unidade_geografica_id in unidades_geograficas_ids:
            unidade_geografica_id_sus = id_impulso_para_id_sus(
                sessao=sessao,
                id_impulso=unidade_geografica_id,
            )
            uf_id_ibge = int(unidade_geografica_id_sus[:2])
            logger.info(
                "Preenchendo formulário para unidade geográfica {id_sus}",
                id_sus=unidade_geografica_id_sus,
            )
            formulario = FormularioConsulta(
                driver,
                competencias=[competencia],
                linha_relatorio=variaveis[0],
                coluna_relatorio=variaveis[1],
                tipo_producao=tipo_producao,
                unidade_geografica=unidade_geografica_tipo,
                estado=uf_id_ibge_para_sigla(
                    sessao=sessao,
                    id_ibge=uf_id_ibge,
                ),
                municipios=[unidade_geografica_id_sus],
                **filtros,  # type: ignore
            )
            logger.info("Executando consulta...")
            formulario.executar_consulta()
            resultado = formulario.resultado
            if resultado:
                logger.info("OK.")
                logger.info("Processando resultados...")
                resultado.sessao = sessao
                resultado.aplicar_transformacoes()
                logger.info("OK")
                logger.info(
                    "Carregando dados obtidos para o banco de dados da"
                    + "ImpulsoGov...",
                )
                codigo_saida = carregar_relatorio_producao(
                    sessao=sessao,
                    dados_producao=resultado.dados,
                    modelo_tabela=modelo_tabela,
                )
            if not resultado or codigo_saida != 0:
                logger.error(
                    "Algo deu errado ao tentar obter o cruzamento das "
                    + "variáveis `{variavel_1}` e `{variavel_2}` para o "
                    + "município com ID SUS {id_sus} na competência de "
                    + "{mes}/{ano}.",
                    variavel_1=variaveis[0],
                    variavel_2=variaveis[1],
                    id_sus=unidade_geografica_id_sus,
                    mes=mes,
                    ano=ano,
                )
                raise RuntimeError
            logger.info("OK.")
            if resultado and codigo_saida == 0 and not teste:
                sessao.commit()
