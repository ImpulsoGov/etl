# flake8: noqa: WPS232
# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Decoradores para executar múltiplas vezes as funções de ETL."""


from __future__ import annotations

from functools import wraps
from typing import Callable

import pandas as pd

from impulsoetl.comum.geografias import BR_UFS
from impulsoetl.tipos import DatetimeLike

HOJE = pd.Timestamp.today()
COMPETENCIA_ATUAL_INICIO = pd.Timestamp(HOJE.year, HOJE.month, 1)
COMPETENCIA_ULTIMA_FIM = COMPETENCIA_ATUAL_INICIO - pd.Timedelta(days=1)


def repetir_por_uf(  # noqa: WPS231
    _funcao: Callable | None = None,
    ignore_index=True,
) -> Callable:
    """Executa função de obtenção de dados do SUS para múltiplas UFs.

    Define um decorador para as funções com prefixo `repetir_`, garantindo que
    sejam executadas uma vez para cada uma das Unidades da Federação listadas
    no parâmetro `uf` (ou para todas as UFs, caso o parâmetro esteja ausente ou
    seja definido como 'BR').

    As funções decoradas devem retornar um objeto `pandas.DataFrame`, de modo
    que o decorador se encarrega de concatenar os resultados das várias
    execuções.

    Argumentos:
        funcao: Uma função que recebe uma sigla de duas letras por meio do
            parâmetro `uf`, extrai dados para a respectiva Unidade da
            Federação, e os retorna como um objeto `pandas.DataFrame`.
        ignore_index: Define se o índice dos DataFrames retornados devem ser
            respeitados ao concatená-los. Por padrão, os índices são ignorados.

    Retorna:
        Um único objeto `pandas.DataFrame` com os resultados concatenados da
        execução para todas as UFs indicadas.
    """

    def decorador_uf(  # noqa: WPS231, WPS430
        funcao: Callable[..., pd.DataFrame],
    ) -> Callable[..., pd.DataFrame]:
        @wraps(funcao)
        def wrapper(*args, **kwargs):

            uf: list[str] | str = kwargs.pop("uf")

            # se a sigla for 'BR', buscar uma lista de todas as UFs do Brasil
            br = ["BR", "br", ["BR"], ["br"]]
            if uf in br:
                ufs = list(BR_UFS)
            # se for outra sigla única, armazená-la como item de uma lista
            elif isinstance(uf, str):
                ufs = [uf.upper()]
            # se já for uma lista de UFs, usar a lista dada
            else:
                ufs = [sigla.upper() for sigla in uf]

            # checar se não há uma sigla desconhecida
            for _ in ufs:
                if _ not in BR_UFS:
                    raise ValueError(
                        "Unidade da Federação não reconhecida: '{}'. ".format(
                            _,
                        ),
                    )
            return pd.concat(
                [funcao(uf=uf, *args, **kwargs) for uf in ufs],
                ignore_index=ignore_index,
            )

        return wrapper

    if _funcao is None:
        return decorador_uf

    return decorador_uf(_funcao)


def repetir_por_ano_mes(  # noqa: WPS231
    _funcao: Callable | None = None,
    data_inicio_minima: DatetimeLike | None = None,
    ignore_index=True,
) -> Callable:
    """Executa função de obtenção de dados do SUS para múltiplas competências.

    Define um decorador que adiciona os parâmetros `data_inicio` e
    `data_fim` nas funções decoradas, garantindo que sejam executadas uma vez
    para cada uma das competências mensais definidas por esse intervalo.

    As funções decoradas devem receber os parâmetros `ano` e `mes` (inteiros) e
    retornar um objeto `pandas.DataFrame`. O o decorador se encarrega de
    concatenar os resultados das várias execuções em um único DataFrame.

    Argumentos:
        funcao: Uma função que recebe como parâmetros dois valores inteiros,
            `ano` e `mes`, extrai dados para a respectiva competência, e os
            retorna como um objeto `pandas.DataFrame`.
        data_inicio_minima: Uma data mínima opcional, para garantir que a
            data de início fornecida seja posterior à implantação de um sistema
            ou documento de registro.
        ignore_index: Define se o índice dos DataFrames retornados devem ser
            respeitados ao concatená-los. Por padrão, os índices são ignorados.

    Retorna:
        Um único objeto `pandas.DataFrame` com os resultados concatenados da
        execução para todas as competências no intervalo indicado.
    """

    if data_inicio_minima:
        data_inicio_minima = pd.Timestamp(data_inicio_minima)

    def decorador_ano_mes(  # noqa: WPS231 WPS430
        funcao: Callable[..., pd.DataFrame],
    ) -> Callable[..., pd.DataFrame | None]:
        @wraps(funcao)
        def wrapper(*args, **kwargs):

            # lê o ano e mês, se informados, e os transforma em intervalo
            ano: int | None = kwargs.pop("ano", None)
            mes: int | None = kwargs.pop("mes", None)
            data_inicio: DatetimeLike | None = kwargs.pop("data_inicio", None)
            data_fim: DatetimeLike | None = kwargs.pop(
                "data_fim",
                COMPETENCIA_ULTIMA_FIM,
            )

            # ano e mês tomam precedência sobre data de início e de fim.
            # se ambos estiverem presentes, sobrescrevê-los pelo início e
            # fim do mês
            if ano and mes:
                # TODO: aceitar só ano, e repetir para todos os meses do ano
                data_inicio = pd.Timestamp(ano, mes, 1)
                data_fim = pd.Timestamp(ano, mes, 28)  # noqa: WPS 432

            # checa se foi definido pelo menos um tipo de parâmetro de data
            if data_inicio:
                data_inicio = pd.Timestamp(data_inicio)
            else:
                raise ValueError(
                    "Indique um valor como `data_inicio`, ou então o `ano` e "
                    + "`mes` de uma competência.",
                )

            # checar se a data de início é posterior à data mínima
            if data_inicio_minima and data_inicio < data_inicio_minima:
                raise ValueError(
                    "A data de início informada é anterior à data mínima para "
                    + "o documento. Informe uma data de início a partir de "
                    + "'{}'.".format(data_inicio_minima.isoformat()),
                )

            # checar se a data final da consulta é posterior à data de início
            if data_fim:
                data_fim = pd.Timestamp(data_fim)
            if data_fim < data_inicio:
                raise ValueError(
                    "A data de término da consulta é anterior à data de "
                    + "início.",
                )

            # executar a função para cada competência no intervalo,
            # concatenando os resultados
            try:
                return pd.concat(
                    [
                        funcao(ano=dt.year, mes=dt.month, *args, **kwargs)
                        for dt in pd.date_range(
                            data_inicio, data_fim, freq="MS"
                        )
                    ],
                    ignore_index=ignore_index,
                )
            except ValueError:  # todos os dataframes são vazios
                return None

        return wrapper

    if _funcao is None:
        return decorador_ano_mes

    return decorador_ano_mes(_funcao)
