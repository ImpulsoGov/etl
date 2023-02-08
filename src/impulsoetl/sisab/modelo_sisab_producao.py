from __future__ import annotations

import re
import urllib
from copy import copy
from datetime import date
from functools import lru_cache, partial
from io import StringIO
from itertools import product
from typing import Generator, List, Literal, Union
from warnings import warn

import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
from more_itertools import chunked
from pandas.errors import ParserError

from impulsoetl.loggers import logger

TIPOS_IDADE = Literal["Dias", "Ano"]
TIPOS_PRODUCAO = Literal[
    "Atendimento Individual",
    "Atendimento Odontológico",
    "Procedimento",
    "Visita Domiciliar",
]
SELECIONAR_TODOS = Literal["Selecionar Todos"]
SELECAO_OPCOES_LISTA = Union[SELECIONAR_TODOS, List[str]]

MESES: dict[str, str] = {
    "JAN": "01",
    "FEV": "02",
    "MAR": "03",
    "ABR": "04",
    "MAI": "05",
    "JUN": "06",
    "JUL": "07",
    "AGO": "08",
    "SET": "09",
    "OUT": "10",
    "NOV": "11",
    "DEZ": "12",
}


@lru_cache(8)
def _requisitar_url_com_cache(
    url: str,
    metodo: str = "GET",
    **kwargs
) -> requests.Response:
    """Faz requisição com `requests`, com armazenando as respostas em cache.

    Argumentos:
        url: URL do relatório do SISAB cuja interface se deseja consultar.
        metodo: método HTTP para a requisição. O padrão é `'GET'`.
        kwargs: argumentos adicionais a serem repassados para a função
            `requests.request()`.
    Retorna:
        Objeto da classe `requests.Response`.
    """
    resposta = requests.request(method=metodo, url=url)
    resposta.raise_for_status()
    return resposta


class SisabRelatorioProducao(object):
    URL: str = (
        "https://sisab.saude.gov.br/paginas/acessoRestrito/relatorio/federal"
        + "/saude/RelSauProducao.xhtml"
    )
    """URL da tela do SISAB relativa à geração de relatórios de produção."""

    def __init__(
        self,
        competencias: list[date],
        unidade_geografica: str = "Brasil",
        variavel_linha: str = "Brasil",
        variavel_coluna: str = "Tipo de Produção",
        faixa_etaria: tuple[int, int] = (0, 0),
        idade_tipo: TIPOS_IDADE | None = None,
        tipo_producao: TIPOS_PRODUCAO | None = None,
        selecoes_adicionais: dict[str, SELECAO_OPCOES_LISTA] = {},
    ):
        """Representação da interface com relatórios de produção da APS.

        Argumentos:
            competencias: Lista de objetos `datetime.date()` referentes aos
                meses cuja produção se deseja obter.
            unidade_geografica: Nível geográfico ao qual estarão restritos os
                resultados (não se confunde com o nível de detalhamento
                utilizado apresentação dos resultados, que pode ser controlado
                com o argumento `variavel_linha`). O único valor suportado
                atualmente é `'Brasil'`, que é também o padrão.
            variavel_linha: Nome da variável que deve ser apresentada como
                linha do relatório. Deve assumir um dos valores previstos na
                interface do SISAB, na caixa de seleçao `Linha do Relatório:`.
                Por padrão, assume o valor `'Brasil'`.
            variavel_coluna: Nome da variável que deve ser apresentada como
                coluna do relatório. Deve assumir um dos valores previstos na
                interface do SISAB, na caixa de seleçao `Coluna do Relatório:`.
                Por padrão, assume o valor `'Tipo de Produção'`.
            faixa_etaria: tupla com as idades inicial e final da faixa etária
                dos usuários para os quais a produção será contabilizada. Por
                padrão, assume o valor `(0, 0)`, o que corresponde a não usar
                esse filtro.
            idade_tipo: unidade de medida das idades informadas no atributo
                `faixa_etaria`. É obrigatório caso esse argumento seja
                utilizado, e não deve ser utilizado caso contrário. Caso
                informado, deve assumir um valor entre `'Ano'` ou `'Dias'`. Por
                padrão, não é usado.
            tipo_producao: Tipo da produção que deverá ser contabilizada. Deve
                ser informada sempre que nenhum dos argumentos `variavel_linha`
                ou `variavel_coluna` assumirem o valos `'Tipo de Produção'`. Se
                informado, deve assumir um valor entre
                `'Atendimento Individual'`, `'Atendimento Odontológico'`,
                `'Procedimento'` ou `'Visita Domiciliar'`. Por padrão, não é
                utilizado.
            selecoes_adicionais: Dicionário com outros filtros a serem
                configurados para determinar a produção a ser contabilizada. O
                dicionário informado deve ter como chaves os nomes de alguma das
                caixas de seleção existentes na interface do SISAB *(não é
                sensível a maiúsculas e minúsculas, nem a espaços ou sinais de
                pontuação no início ou no final dos rótulos)*. Para cada chave,
                deve ser informada uma lista com os nomes das opções que serão
                marcadas para geração do relatório, também conforme aparecem ao
                abrir as opções nas caixas de seleção do SISAB.
        """
        resposta = _requisitar_url_com_cache(url=self.URL)
        self._cookies = resposta.cookies
        self._interface = bs(resposta.text, "html.parser")
        self._view_state = self._interface.find(
            id="javax.faces.ViewState"
        )["value"]
        self._payload = [
            ("j_idt44", "j_idt44"),
            ("lsCid", ""),
            ("dtBasicExample_length", 10),
            ("lsSigtap", ""),
            ("td-ls-sigtap_length", 10),
            ("javax.faces.ViewState", self._view_state),
            ("j_idt192", "j_idt192"),
        ]
        self.competencias = competencias
        self.unidade_geografica = unidade_geografica
        self.variavel_linha = variavel_linha
        self.variavel_coluna = variavel_coluna
        self.idade_tipo = idade_tipo
        self.faixa_etaria = faixa_etaria
        self.tipo_producao = tipo_producao
        self.selecoes_adicionais = selecoes_adicionais

    @property
    def unidade_geografica(self) -> str:
        return self._unidade_geografica
    
    @unidade_geografica.setter
    def unidade_geografica(self, unidade_geografica):
        if unidade_geografica.lower() != "brasil":
            raise NotImplementedError(
                "Apenas o nível '`Brasil`' é suportado atualmente para o "
                "o argumento `unidade_geografica`."
            )
        unidade_geografica_codigo = self._obter_opcao_codigo(
            "unidGeo",
            unidade_geografica,
        )
        self._payload.append(("unidGeo", unidade_geografica_codigo))
        self._unidade_geografica = unidade_geografica

    @property
    def competencias(self) -> list[date]:
        return self._competencias

    @competencias.setter
    def competencias(self, competencias: list[date]):
        if not competencias or len(competencias) > 12:
            raise ValueError(
                "Informe de 1 a 12 competências! ({} informadas)".format(
                    len(competencias or []),
                )
            )
        if not hasattr(self, "_competencias"):
            self._competencias: list[date] = [] 
        competencias_disponiveis = self._obter_opcoes_disponiveis("j_idt76")
        for competencia in competencias:
            competencia_aaaamm = "{:%Y%m}".format(competencia)
            if competencia_aaaamm in competencias_disponiveis:
                self._payload.append(("j_idt76", competencia_aaaamm))
                self._competencias.append(competencia)
                continue
            mensagem_competencia_indisponivel = (
                "Competência {:%m/%Y} indisponível no sistema. ".format(
                    competencia,
                )
            )
            if len(competencias) == 1:
                raise ValueError(
                    mensagem_competencia_indisponivel + "Interrompendo!",
                )
            warn(mensagem_competencia_indisponivel + "Ignorando...")

    @property
    def variavel_linha(self) -> str:
        return self._variavel_linha

    @variavel_linha.setter
    def variavel_linha(self, variavel: str):
        variavel_codigo = self._obter_opcao_codigo("selectLinha", variavel)
        self._payload.append(("selectLinha", variavel_codigo))
        self._variavel_linha = variavel

    @property
    def variavel_coluna(self) -> str:
        return self._variavel_coluna

    @variavel_coluna.setter
    def variavel_coluna(self, variavel: str):
        variavel_codigo = self._obter_opcao_codigo("selectcoluna", variavel)
        self._payload.append(("selectcoluna", variavel_codigo))
        self._variavel_coluna = variavel

    @property
    def idade_tipo(self) -> str:
        return self._idade_tipo

    @idade_tipo.setter
    def idade_tipo(self, idade_tipo):
        if idade_tipo:
            idade_tipo_rotulo = self._interface.find(
                "label",
                string=re.compile(
                    r"^\W*" + idade_tipo[:3] + r"s?\W*$",
                    re.IGNORECASE,
                ),
                attrs={"for": re.compile("tpIdade.*")},
            )
            idade_tipo_codigo = idade_tipo_rotulo.parent.find("input")["value"]
            self._payload.append(("tpIdade", idade_tipo_codigo))
        self._idade_tipo = idade_tipo

    @property
    def faixa_etaria(self) -> tuple[int, int]:
        return self._faixa_etaria

    @faixa_etaria.setter
    def faixa_etaria(self, faixa_etaria: tuple[int, int]):
        if (faixa_etaria[0] + faixa_etaria[1] > 0) and not self.idade_tipo:
            raise ValueError(
                "Foi inserido um filtro de faixa etária, mas o tipo da idade "
                + "não foi informado."
            )
        self._payload.extend([
            ("idadeInicio", faixa_etaria[0]),
            ("idadeFim", faixa_etaria[1]),
        ])
        self._faixa_etaria = faixa_etaria

    @property
    def tipo_producao(self) -> str:
        return self._tipo_producao

    @tipo_producao.setter
    def tipo_producao(self, tipo_producao: str | None):
        if (
            not tipo_producao
            and self.variavel_linha.lower() != "tipo de produção"
            and self.variavel_coluna.lower() != "tipo de produção"
        ):
            raise ValueError(
                "Argumento `tipo_producao` não informado. Informe um valor "
                + "ou defina algum dos argumentos `variavel_linha` ou "
                + "`variavel_coluna` como `'Tipo de Produção'`."
            )
        if tipo_producao:
            tipo_producao_codigo = self._obter_opcao_codigo(
                "tpProducao",
                tipo_producao,
            )
            self._payload.append(("tpProducao", tipo_producao_codigo))
        self._tipo_producao = tipo_producao

    @property
    def selecoes_adicionais(self) -> dict[str, list[str]]:
        return self._selecoes_adicionais

    @selecoes_adicionais.setter
    def selecoes_adicionais(self, selecoes):
        if not hasattr(self, "_selecoes_adicionais"):
            self._selecoes_adicionais: dict[str, list[str]] = {}
        for selecao_rotulo, opcoes_descricoes in selecoes.items():
            if not opcoes_descricoes:
                continue  # Seleções vazias ou com valor None são ignoradas
            selecao_codigo = self._obter_selecao_codigo(selecao_rotulo)
            if (
                isinstance(opcoes_descricoes, str)
                and opcoes_descricoes.lower() == "selecionar todos"
            ):
                opcoes_descricoes = self._obter_opcoes_disponiveis(
                    selecao_codigo,
                    retornar="descricoes",
                )
            for opcao_descricao in opcoes_descricoes:
                opcao_codigo = self._obter_opcao_codigo(
                    selecao_codigo,
                    opcao_descricao,
                )
                self._payload.append((selecao_codigo, opcao_codigo))
            self._selecoes_adicionais[selecao_rotulo] = opcoes_descricoes

    def download(self) -> str:
        """Baixar o relatório com as configurações informadas como um CSV.
        
        Retorna:
            Uma string que contém o relatório baixado do SISAB na forma de um
            arquivo de texto com codificação ISO-8859-1, incluindo um cabeçalho,
            um trecho de dados com valores separados por ponto-e-vírgula, e um
            rodapé.
        """
        resposta = requests.request(
            "POST",
            self.URL + ";jsessionid=" + self._cookies["JSESSIONID"],
            headers=self._cabecalhos_requisicao,
            cookies=self._cookies,
            data=self._payload,
            timeout=120,
        )
        resposta.raise_for_status()
        return resposta.text


    @property
    def _cabecalhos_requisicao(self) -> dict[str, str]:
        """Cabeçalhos necessarios à execução de uma requisição."""
        return {
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;q=0.9,"
                + "image/avif,image/webp,image/apng,*/*;q=0.8"
            ),
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "pt-BR,pt;q=0.9",
            "Cache-Control": "max-age=0",
            "Connection": "keep-alive",
            "Content-Type": "application/x-www-form-urlencoded",
            "Host": "sisab.saude.gov.br",
            "Origin": "https://sisab.saude.gov.br",
            "Referer": self.URL,
        }

    def _obter_selecao_codigo(self, rotulo):
        selecao_rotulo = self._interface.find(
            "label",
            string=re.compile(r"^\W*" + rotulo + "\W*$", re.IGNORECASE)
        )
        return selecao_rotulo.find_next("select")["name"]

    def _obter_opcao_codigo(self, selecao_codigo: str, descricao: str) -> str:
        selecao = self._interface.find(attrs={"name": selecao_codigo})
        return selecao.find(
            "option",
            string=re.compile(r"^\W*" + descricao + r"\W*$", re.IGNORECASE)
        )["value"]

    def _obter_opcoes_disponiveis(
        self,
        selecao_codigo: str,
        retornar: str = "codigos",
    ) -> str:
        selecao = self._interface.find(attrs={"name": selecao_codigo})
        opcoes = selecao.find_all("option")
        if retornar == "codigos":
            return [opcao["value"] for opcao in opcoes]
        elif retornar == "descricoes":
            return [opcao.string for opcao in opcoes]
        else:
            raise ValueError(
                "O argumento `retornar` deve assumir algum dos valores entre "
                + "`'codigos'` ou `'descricoes'` (`'{}'` recebido).".format(
                    retornar,
                ),
            )

