# SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Obtém dados de CEPs de diversas fontes via BrasilAPI."""


from __future__ import annotations

from time import sleep
from typing import Any, Final, Iterable

import janitor  # noqa: F401  # nopycln: import
import pandas as pd
import requests
from frozendict import frozendict
from sqlalchemy.orm import Session

from impulsoetl.brasilapi.modelos import ceps as tabela_destino
from impulsoetl.loggers import logger

DE_PARA_CEP: Final[frozendict] = frozendict(
    {
        "cep": "id_cep",
        "state": "uf_sigla",
        "city": "municipio_nome",
        "neighborhood": "bairro_nome",
        "street": "logradouro_nome_completo",
        "service": "fonte_nome",
        "latitude": "latitude",
        "longitude": "longitude",
    },
)

TIPOS_CEP: Final[frozendict] = frozendict(
    {
        "id_cep": str,
        "uf_sigla": str,
        "municipio_nome": str,
        "bairro_nome": str,
        "logradouro_nome_completo": str,
        "fonte_nome": str,
        "latitude": float,
        "longitude": float,
    },
)

CEP_ENDPOINT_V2: str = "https://brasilapi.com.br/api/cep/v2/{cep}"


def extrair_cep(id_cep: str) -> dict[str, Any] | None:
    response = requests.get(CEP_ENDPOINT_V2.format(cep=id_cep))
    if response.ok:
        return response.json()
    return None


def transformar_cep(cep_dados: dict[str, Any]) -> pd.Dataframe:
    """Transforma um dicionário com dados de um CEP retornado pela BrasilAPI.

    Argumentos:
        cep_dados: Dicionário contendo dados do Código de Endereçamento Postal
            retornados pelo endpoint da [BrasilAPI][].

    [BrasilAPI]: https://brasilapi.com.br/docs#tag/CEP-V2
    """
    logger.debug(
        "Transformando dados para o CEP '{}'.",
        cep_dados["cep"],
    )

    cep_transformado = cep_dados
    cep_transformado["latitude"] = (
        cep_transformado.get("location", {})
        .get("coordinates", {})
        .get("latitude", None)
    )
    cep_transformado["longitude"] = (
        cep_transformado.get("location", {})
        .get("coordinates", {})
        .get("longitude", None)
    )

    for campo_de, campo_para in DE_PARA_CEP.items():
        campo_valor = cep_transformado.pop(campo_de, None)
        if campo_valor:
            campo_valor = TIPOS_CEP[campo_para](campo_valor)
        cep_transformado[campo_para] = campo_valor

    cep_transformado = {
        campo: valor
        for campo, valor in cep_transformado.items()
        if campo in TIPOS_CEP
    }

    logger.debug("OK")
    return cep_transformado


@logger.catch
def carregar_cep(
    sessao: Session,
    cep_transformado: pd.DataFrame,
) -> int:
    """Carrega um CEP no banco de dados da ImpulsoGov.

    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov.
        cep_transformado: DataFrame contendo os dados a serem carregados na
            tabela de destino, já no formato utilizado pelo banco de dados da
            ImpulsoGov (conforme retornado pela função
            [`transformar_cep()`][]).
        passo: Indica quantos registros devem ser enviados para a base de dados
            de cada vez.

    Retorna:
        Código de saída do processo de carregamento. Se o carregamento
        for bem sucedido, o código de saída será `0`.

    [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
    [`transformar_cep()`]: impulsoetl.brasilapi.cep.transformar_cep
    """

    tabela_nome = tabela_destino.key
    logger.debug(
        "Carregando dados do CEP {cep} para a tabela `{tabela_nome}`...",
        cep=cep_transformado["id_cep"],
        tabela_nome=tabela_nome,
    )

    requisicao_insercao = tabela_destino.insert().values(
        # cep_transformado.to_dict(orient="records"),
        cep_transformado,
    )
    sessao.execute(requisicao_insercao)
    logger.debug("OK")
    return 0


def obter_cep(
    sessao: Session,
    ceps_pendentes=Iterable[str],
    teste: bool = False,
    **kwargs,
) -> None:
    """Baixa, transforma e carrega dados de CEPs.

    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov.
        ceps_pendentes: lista de Códigos de Endereçamento Postal a serem
            buscados, escritos como strings sem separadores ou caracteres
            especiais.
        teste: Indica se as modificações devem ser de fato escritas no banco de
            dados (`False`, padrão). Caso seja `True`, as modificações são
            adicionadas à uma transação, e podem ser revertidas com uma chamada
            posterior ao método [`Session.rollback()`][] da sessão gerada com o
            SQLAlchemy.

    [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
    """
    logger.info("Iniciando captura de dados de CEPs.")

    if teste and len(ceps_pendentes) > 10:
        ceps_pendentes = ceps_pendentes[:10]
        logger.warning(
            "Lista de CEPs truncada para 10 registros para fins de teste.",
        )

    # TODO: paralelizar transformação e carregamento
    logger.info("Obtendo informações para {} CEPs...", len(ceps_pendentes))
    ceps_carregados = 0
    for cep in ceps_pendentes:
        cep_dados = extrair_cep(id_cep=cep)
        if cep_dados:
            cep_transformado = transformar_cep(cep_dados=cep_dados)
            codigo_saida = carregar_cep(
                sessao=sessao, cep_transformado=cep_transformado
            )
            if codigo_saida == 0:
                ceps_carregados += 1
        sleep(1)

    logger.info(
        "{} CEPs carregados com sucesso; {} falharam.",
        ceps_carregados,
        len(ceps_pendentes) - ceps_carregados,
    )

    if not teste:
        sessao.commit()
