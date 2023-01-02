# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Define categorias geográficas utilizadas em vários processos de ETL.

Atributos:
    BR_UFS: Siglas das Unidades Federativas brasileiras.
"""


from functools import lru_cache

from frozenlist import FrozenList
from sqlalchemy.orm import Session

from impulsoetl.bd import tabelas

BR_UFS: FrozenList[str] = FrozenList(
    [
        "AC",
        "AL",
        "AM",
        "AP",
        "BA",
        "CE",
        "DF",
        "ES",
        "GO",
        "MA",
        "MG",
        "MS",
        "MT",
        "PA",
        "PB",
        "PE",
        "PI",
        "PR",
        "RJ",
        "RN",
        "RO",
        "RR",
        "RS",
        "SC",
        "SE",
        "SP",
        "TO",
    ],
)


ufs = tabelas["listas_de_codigos.ufs"]
unidades_geograficas = tabelas["listas_de_codigos.unidades_geograficas"]


@lru_cache(27)
def uf_id_ibge_para_sigla(sessao: Session, id_ibge: str | int) -> str:
    """Retorna a sigla de uma unidade federativa a partir do código IBGE.

    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov.
        id_ibge: Código de dois dígitos utilizado para identificar a unidade
            federativas no IBGE.

    Retorna:
        Sigla da unidade federativa.

    [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session"""
    return (
        sessao.query(ufs.c.sigla)
        .filter(ufs.c.id_ibge == str(id_ibge))
        .one()[0]
    )


@lru_cache(5570)
def id_sus_para_id_impulso(sessao: Session, id_sus: str | int) -> str:
    """Converte identificador SUS do município para o usado no BD da Impulso.

    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov.
        id_sus: Código de seis dígitos utilizado para identificar o município
            nos sistemas do SUS.

    Retorna:
        Identificador único do município no banco de dados da ImpulsoGov.

    [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
    """
    return (
        sessao.query(unidades_geograficas.c.id)
        .filter(unidades_geograficas.c.id_sus == str(id_sus))
        .one()[0]
    )


@lru_cache(5570)
def id_sim_para_id_impulso(sessao: Session, id_sim: str | int) -> str:
    """Converte identificador SUS do município para o usado no BD da Impulso.

    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov.
        id_sim: Código de seis dígitos utilizado para identificar o município
            no Sistema de Informação sobre Mortalidade do SUS (SIM).

    Retorna:
        Identificador único do município no banco de dados da ImpulsoGov.

    [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
    """
    return (
        sessao.query(unidades_geograficas.c.id)
        .filter(unidades_geograficas.c.id_sim == str(id_sim))
        .one()[0]
    )


@lru_cache(5570)
def id_impulso_para_id_sus(sessao: Session, id_impulso: str) -> str:
    """Obtém o ID SUS a partir do identificador usado no BD da Impulso.

    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov.
        id_impulso: Identificador único do município no banco de dados da
            ImpulsoGov.

    Retorna:
        Código de sete dígitos utilizado para identificar o município nos
        sistemas do SUS.

    [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
    """
    return (
        sessao.query(unidades_geograficas.c.id_sus)
        .filter(unidades_geograficas.c.id == str(id_impulso))
        .one()[0]
    )
