# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Define categorias de datas e períodos utilizadas em vários processos de ETL.
"""


from datetime import datetime, timedelta, timezone
from functools import lru_cache

import pandas as pd
from sqlalchemy.engine import Row
from sqlalchemy.orm import Session

from impulsoetl.bd import tabelas
from impulsoetl.tipos import DatetimeLike

periodos = tabelas["listas_de_codigos.periodos"]


def agora_gmt_menos3():
    """Retorna o valor de data e hora atuais no fuso GMT-03:00."""
    return datetime.now(tz=timezone(-timedelta(hours=3)))


def de_aaaammdd_para_timestamp(
    texto: str,
    erros: str = "raise",
) -> pd.Timestamp:
    """Transforma data no formato AAAAMMDD em um objeto [`pandas.Timestamp`][].

    Dada um string representando uma data em que os 4 primeiros caracteres
    representam o ano, os dois seguintes representam o mês e o último
    representa o dia, gera um objeto [`pandas.Timestamp`][] que representa
    essa mesma data. Essa função é robusta em relação a caracteres de zero
    eventualmente substituídos por espaços em branco.

    Argumentos:
        texto: Uma string contendo a data em formato AAAAMMDD.
        erros: Define a atitude a ser tomada em caso da ocorrência de um erro.
            Por compatibilidade com o pandas, aceita as categorias `'raise'`
            (interrompe a execução e levanta o erro; padrão), `'ignore'`
            (devolve o objeto passado ao atributo `texto` sem qualquer
            alteração) ou `coerce` (devolve um objeto [`pandas.NaT`][]).

    Retorna:
        Um objeto [`pandas.Timestamp`][] representando a data fornecida.
    """
    try:
        ano = int(texto[0:4])
        mes = int(texto[4:6])
        dia = int(texto[6:8])
        return pd.Timestamp(year=ano, month=mes, day=dia)
    except ValueError:
        if erros == "ignore":
            return texto
        elif erros == "coerce":
            return pd.NaT
        else:
            raise


@lru_cache(365)
def periodo_por_data(  # noqa: WPS122 - permite argumento `data`
    sessao: Session,
    data: DatetimeLike,
    tipo_periodo="mensal",
):
    """Busca o período no qual uma data está incluída.

    Argumentos:
        data: Data de referência para a busca do período correspondente.
        tipo_periodo: O nível de agregação do período desejado. Atualmente, são
            suportados os valores `"mensal"` e `"quadrimestral"`. Por padrão,
            o tipo de período buscado é `"mensal"`.

    Retorna:
        Identificador único do município no banco de dados da ImpulsoGov.
    """

    return (
        sessao.query(periodos)
        .filter(
            periodos.c.tipo == tipo_periodo.title(),
            periodos.c.data_inicio <= data,
            periodos.c.data_fim >= data,
        )
        .one()
    )


@lru_cache(365)
def periodo_por_codigo(sessao: Session, codigo: str) -> Row:
    """Busca um período a partir do seu código.

    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov.
        codigo: Código do período que se pretende buscar.

    Retorna:
        Objeto da classe `sqlalchemy.engine.Row` contendo informações do
        período correspondente.

    [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
    [`sqlalchemy.engine.Row`]: https://docs.sqlalchemy.org/en/14/core/connections.html#sqlalchemy.engine.Row
    """

    return sessao.query(periodos).filter(periodos.c.codigo == codigo).one()


@lru_cache(60)
def obter_proximo_periodo(sessao: Session, periodo_id: str):
    """Retorna a representação do período subsequente a um período dado."""

    periodo_atual = (
        sessao.query(periodos).filter(periodos.c.id == periodo_id).one()
    )

    inicio_proximo_periodo = periodo_atual.data_fim + timedelta(days=1)

    return periodo_por_data(
        sessao=sessao,
        data=inicio_proximo_periodo,
        tipo_periodo=periodo_atual.tipo,
    )
