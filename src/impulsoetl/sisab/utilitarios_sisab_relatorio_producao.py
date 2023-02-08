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

from impulsoetl.sisab.modelo_sisab_producao import SisabRelatorioProducao
from impulsoetl.loggers import logger


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


def _resolver_selecionar_todos(
    selecoes: dict[str, SELECIONAR_TODOS | SELECAO_OPCOES_LISTA],
) -> dict[str, SELECAO_OPCOES_LISTA]:
    """Substitui filtros `'Selecionar Todos'` pelos valores respectivos."""

    # separa filtros que estão com valor `'Selecionar Todos'`
    selecoes_selecionar_todos = {}
    _selecoes = copy(selecoes)
    for selecao_rotulo, opcoes_valores in selecoes.items():
        if opcoes_valores == "Selecionar Todos":
            selecoes_selecionar_todos[selecao_rotulo] = _selecoes.pop(
                selecao_rotulo,
            )

    # gera relatório dummy que resolve os `'Selecionar Todos'`
    relatorio_dummy = SisabRelatorioProducao(
        competencias=[date(2021, 1, 1)],
        selecoes_adicionais=selecoes_selecionar_todos,
    )

    # apensa de novo as seleções resolvidas com os demais filtros inalterados 
    return dict(_selecoes, **relatorio_dummy.selecoes_adicionais)


def _gerar_relatorios_producao_por_municipio(
    competencias: list[date],
    faixas_etarias: list[tuple[int, int]] = [(0, 0)],
    idade_tipo: TIPOS_IDADE | None = None,
    tipo_producao: TIPOS_PRODUCAO | None = None,
    selecoes_adicionais: dict[
        str, SELECIONAR_TODOS | SELECAO_OPCOES_LISTA
    ] = {},
    incluir_subtotais: list[str] = [],
) -> Generator[SisabRelatorioProducao, None, None]:
    """Gera relatórios de produção por município combinando vários critérios."""

    # Para filtros com "Selecionar Todos" no primeiro nível 
    # (`{'Nome do filtro': 'Selecionar Todos'}`), serão feitas as combinações
    # para cada uma das opções disponíveis.
    selecoes_adicionais = _resolver_selecionar_todos(selecoes_adicionais)

    # Se pedido para inserir subtotais, adiciona uma opção "None" em cada
    # filtro, que será ignorado pelo gerador de relatórios - o que equivale a
    # rodar o relatório desconsiderando esse filtro 
    if incluir_subtotais:
        selecoes_adicionais = {
            selecao_rotulo: (
                opcoes_valores + [None]
                if selecao_rotulo in incluir_subtotais
                else opcoes_valores  # não fazer nada
            )
            for selecao_rotulo, opcoes_valores
            in selecoes_adicionais.items()
        }

    # determina automagicamente a melhor variável para ser usada como coluna
    variavel_mais_opcoes = "Competência"
    variavel_quantidade_opcoes = min(len(competencias), 12)
    for selecao_rotulo, opcoes_valores in selecoes_adicionais.items():
        if (
            (selecao_rotulo not in incluir_subtotais)
            and (len(opcoes_valores) > variavel_quantidade_opcoes)
            and all(isinstance(opcao, str) for opcao in opcoes_valores)
        ):
            variavel_mais_opcoes = selecao_rotulo
    if tipo_producao:
        variavel_coluna = variavel_mais_opcoes
    else:
        variavel_coluna = "Tipo de Produção"

    # garante que no máximo 12 competências sejam solicitadas de cada vez
    if variavel_coluna == "Competência":
        # listas com 12 competências, se for ser usada como variável da coluna
        intervalos_competencias = list(chunked(competencias, 12))
    else:
        # se não, solicitar uma competência de cada vez
        intervalos_competencias = [
            [competencia]
            for competencia
            in competencias
        ]

    # obtém todas as combinações possíveis de opções a serem requisitadas
    # NOTE: para a variável coluna, sempre são usados todos os valores
    # marcados. Para os demais, é gerada uma requisição para cada valor possível
    selecoes_exceto_variavel_coluna = {
        selecao_rotulo: opcoes_valores
        for selecao_rotulo, opcoes_valores
        in selecoes_adicionais.items()
        if selecao_rotulo != variavel_coluna
    }
    combinacoes_parametros = list(product(
        intervalos_competencias,
        faixas_etarias,
        *selecoes_exceto_variavel_coluna.values(),
    ))

    # executa a requisição do relatório para cada combinação de opções
    total_requisicoes = len(combinacoes_parametros)
    requisicao_num = 0
    for parametros in combinacoes_parametros:
        requisicao_num += 1
        (intervalo_competencias, faixa_etaria, *combinacao_opcoes) = parametros
        logger.info("Iniciando requisição {} de {}...".format(
            requisicao_num,
            total_requisicoes,
        ))
        selecoes_combinacao = {
            # só um valor usado p/ o filtro nessa combinação
            selecao_rotulo: [combinacao_opcoes[indice]]  # envelopa em lista
            if isinstance(combinacao_opcoes[indice], str)
            # OU filtro considera vários valores juntos nessa combinação
            else combinacao_opcoes[indice]  # já é uma lista; ignora
            # iterar para cada filtro usado nessa combinação
            for indice, selecao_rotulo
            in enumerate(selecoes_exceto_variavel_coluna)
        }
        # reincorpora as restrições de valores para a variável coluna
        selecoes_combinacao[variavel_coluna] = selecoes_adicionais.get(
            variavel_coluna,
        )

        logger.debug(
            "Competências: {}\nFaixa etária: {} {}\nFiltros:{}".format(
                ", ".join(
                    ["{:%m/%Y}".format(c) for c in intervalo_competencias]
                ),
                faixa_etaria.__repr__,
                idade_tipo or "",
                "\n\t".join([
                    "{}: {}".format(k, v.__repr__)
                    for k, v in selecoes_combinacao.items()
                ]),
            ),
        )
        relatorio = SisabRelatorioProducao(
            competencias=intervalo_competencias,
            unidade_geografica="Brasil",
            variavel_linha="Municipio",
            variavel_coluna=variavel_coluna,
            faixa_etaria=faixa_etaria,
            idade_tipo = idade_tipo,
            tipo_producao = tipo_producao,
            selecoes_adicionais=selecoes_combinacao,
        )

        yield relatorio


def _ler_producao_por_municipio(csv: str) -> pd.DataFrame:
    """Lê um arquivo de texto com relatório de produção e devolve um DataFrame.

    Argumentos:
        csv: relatório baixado do SISAB na forma de um arquivo de texto com
        codificação ISO-8859-1, incluindo um cabeçalho, um trecho de dados com
        valores separados por ponto-e-vírgula, e um rodapé.

    Retorna:
        Um objeto `pandas.DataFrame()` contendo apenas os dados obtidos do corpo
        principal do relatório.
    """

    # delimita cabeçalho e rodapé no CSV bruto 
    cabecalho_posicao_fim = csv.find("\n\n\n")
    dados_posicao_inicio = cabecalho_posicao_fim + 3
    dados_posicao_fim = csv.find("\n\n\nFonte:")

    # realiza leitura com pandas
    dados = pd.read_csv(
        StringIO(csv[dados_posicao_inicio:dados_posicao_fim]),
        decimal=",",
        dtype={"Ibge": "object"},
        encoding="iso-8859-1",
        engine="python",
        sep=";",
        thousands=".",
    )
    return dados.loc[:, ~dados.columns.str.contains('^Unnamed')]


def extrair_producao_por_municipio(
    competencias: list[date],
    faixas_etarias: list[tuple[int, int]] = [(0, 0)],
    idade_tipo: Literal["ano", "dias"] | None = None,
    tipo_producao: str | None = None,
    selecoes_adicionais: dict[str, str | list[str]] = {},
    incluir_subtotais: list[str] = [],
) -> pd.DataFrame:
    """Extrai do SISAB dados de produção da APS para todos os municípios.

    Note:
        Diferentemente do que ocorre ao utilizar diretamente a classe
        `SisabRelatorioProducao()`, os valores em listas informados para os
        argumentos `competencias`, `faixas_etarias` e cada uma das chaves do
        argumento `selecoes_adicionais` serão combinados entre si para gerar
        diversas requisições de forma a obter o cruzamento entre todas as
        variáveis informadas.

        Para os elementos do dicionário de `selecoes_adicionais`, é aceito o
        valor `'Selecionar Todos'`, que implica *cada uma das as opções*
        disponíveis para aquela variável na hora de gerar as combinações.

    Argumentos:
        competencias: Lista de objetos `datetime.date()` referentes aos
            meses cuja produção se deseja obter.
        faixas_etarias: lista de tupla com idades inicial e final das faixas
            etárias dos usuários para as quais a produção será contabilizada.
            Por padrão, assume o valor `[(0, 0)]`, o que corresponde a não usar
            esse filtro.
        idade_tipo: unidade de medida das idades informadas no atributo
            `faixas_etarias`. É obrigatório caso esse argumento seja
            utilizado, e não deve ser utilizado caso contrário. Caso
            informado, deve assumir um valor entre `'Ano'` ou `'Dias'`. Por
            padrão, não é usado.
        tipo_producao: Tipo da produção que deverá ser contabilizada. Se
            informado, deve assumir um valor entre `'Atendimento Individual'`,
            `'Atendimento Odontológico'`, `'Procedimento'` ou
            `'Visita Domiciliar'`. Por padrão, não é utilizado.
        selecoes_adicionais: Dicionário com outros filtros cujas opções serão
            combinadas para obter os cruzamentos segunto os quais a produção
            será contabilizada. O dicionário informado deve ter como chaves os
            nomes de alguma das caixas de seleção existentes na interface do
            SISAB *(não é sensível a maiúsculas e minúsculas, nem a espaços ou
            sinais de pontuação no início ou no final dos rótulos)*. Para cada
            chave, deve ser informada uma lista com os nomes das opções que
            serão marcadas para geração do relatório, também conforme aparecem
            ao abrir as opções nas caixas de seleção do SISAB.
        incluir_subtotais: Lista com os rótulos de filtros informados no
            argumento `selecoes_adicionais` para os quais se deseja incluir uma
            categoria `'Total'` - equivalente a não se marcar nenhuma categoria
            para aquele filtro. Pode ser útil para filtros como
            `Tipo de Equipe`, em que a produção de profissionais que não estão
            vinculados a nenhuma equipe não é computada considerando-se cada uma
            das categorias possíveis. Por padrão, é uma lista vazia, o que
            equivale a não totalizar nenhum dos filtros.

    Retorna:
        Um objeto `pandas.DataFrame()` com os dados de produção da APS segundo o
        cruzamento de todas as variáveis informadas, incluindo todas as
        combinações dos valores aceitos.
    """

    logger.info("Extraindo relatório de produção do SISAB...")
    producao_por_municipio = pd.DataFrame()
    for relatorio in _gerar_relatorios_producao_por_municipio(
        competencias=competencias,
        faixas_etarias=faixas_etarias,
        idade_tipo=idade_tipo,
        tipo_producao=tipo_producao,
        selecoes_adicionais=selecoes_adicionais,
        incluir_subtotais=incluir_subtotais,
    ):
        csv = relatorio.download()
        producao_por_municipio_parcial = _ler_producao_por_municipio(csv)

        # verticaliza o dataframe (transforma colunas em linhas)
        producao_por_municipio_parcial = producao_por_municipio_parcial.melt(
            id_vars=["Uf", "Ibge", "Municipio"],
            var_name=relatorio.variavel_coluna,
            value_name="quantidade_aprovada",
        )
        producao_por_municipio_parcial["quantidade_aprovada"] = (
            producao_por_municipio_parcial["quantidade_aprovada"].fillna(0)
            .astype("int")
        )

        # registra nos dados os parâmetros usados como filtros na requisição
        if relatorio.variavel_coluna.lower() == "competência":
            # competências estavam nas colunas; vêm no formato MES/AAAA
            producao_por_municipio_parcial["Competência"] = (
                producao_por_municipio_parcial["Competência"].apply(
                    lambda dt: MESES[dt[:3]] + dt[3:]
                )
                .apply(pd.to_datetime, format="%m/%Y", errors="ignore")
            )
        else:
            if len(relatorio.competencias) == 1:
                # só uma competência; registrar na coluna
                producao_por_municipio_parcial["Competência"] = (
                    relatorio.competencias[0]
                )
            else:
                # várias competências; registrar como índice de datas do pandas
                producao_por_municipio_parcial["Competência"] = (
                    pd.DatetimeIndex(relatorio.competencias, freq="MS")
                )

        if relatorio.faixa_etaria != (0, 0):
            producao_por_municipio_parcial["faixa_etaria_descricao"] = (
                "{} a {} {}".format(
                    relatorio.faixa_etaria[0],
                    relatorio.faixa_etaria[1],
                    relatorio.idade_tipo.lower(),
                )
            )
        for selecao_rotulo, opcao_valor in relatorio.selecoes_adicionais.items():
            if selecao_rotulo != relatorio.variavel_coluna:
                producao_por_municipio_parcial[selecao_rotulo] = ",".join(
                    opcao_valor,
                )

        producao_por_municipio = pd.concat(
            [producao_por_municipio, producao_por_municipio_parcial],
            ignore_index=True,
        )

    # se `incluir_subtotais` tiver sido usado, alguns valores nas colunas
    # que descrevem de filtros combinação usada em cada linha estarão em
    # branco, porque são as combinações em que o filtro foi ignorado.
    # Essas células devem ser preenchidas para indicar que esses são o total 
    if incluir_subtotais:
        nomes_colunas_filtros = list(selecoes_adicionais.keys())
        producao_por_municipio[nomes_colunas_filtros] = (
            producao_por_municipio[nomes_colunas_filtros].fillna("Todos")
        )
    
    logger.info(
        "Extração de dados de produção do SISAB finalizada com sucesso!",
    )
    return producao_por_municipio


def transformar_producao_por_municipio(df: pd.DataFrame) -> pd.DataFrame:
    """Renomeia colunas e lê datas de um relatório de produção por municípios."""
    df.rename(
        columns={
            "Uf": "uf_sigla",
            "Ibge": "municipio_id_sus",
            "Municipio": "municipio_nome",
            "Competência": "periodo_data_inicio",
        },
        inplace=True,
    )
    df["quantidade_aprovada"] = df.pop("quantidade_aprovada")  # reordena
    # TODO: normalizar nomes das variáveis
    return df
