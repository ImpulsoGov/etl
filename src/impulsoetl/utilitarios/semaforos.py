# SPDX-FileCopyrightText: 2023 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Define funções para restringir e liberar a escrita no banco de dados."""


import socket

from sqlalchemy.orm.session import Session

from impulsoetl.bd import tabelas
from impulsoetl.loggers import logger


tabela_semaforos = tabelas["configuracoes.capturas_semaforos"]


class EscritaBloqueadaExcecao(BlockingIOError):
    """Exceção relativa à tentativa de escrita em uma tabela bloqueada."""

    pass


def bloquear_escrita(
    sessao: Session,
    tabela_destino: str,
    unidade_geografica_id: str,
    periodo_id: str,
) -> int:
    logger.info(
        "Bloqueando escrita simultânea para a tabela `{tabela_destino}` e "
        + "identificadores de unidade geográfica {unidade_geografica_id} e "
        + "de período `{periodo_id}`.",
        tabela_destino=tabela_destino,
        unidade_geografica_id=unidade_geografica_id,
        periodo_id=periodo_id,
    )
    cliente_nome = socket.gethostname()
    cliente_ipv4 = socket.gethostbyname(cliente_nome)

    requisicao_bloqueio = tabela_semaforos.insert().values([{
        "tabela_destino": tabela_destino,
        "unidade_geografica_id": unidade_geografica_id,
        "periodo_id": periodo_id,
        "cliente_nome": cliente_nome,
        "cliente_ipv4": cliente_ipv4,
    }])
    sessao.execute(requisicao_bloqueio)
    sessao.commit()
    return 0


def checar_escrita_liberada(
    sessao: Session,
    tabela_destino: str,
    unidade_geografica_id: str,
    periodo_id: str,
) -> int:
    logger.info(
        "Checando estatus de liberação da escrita para a tabela "
        "`{tabela_destino}` e identificadores de unidade geográfica "
        + "{unidade_geografica_id} e de período `{periodo_id}`.",
        tabela_destino=tabela_destino,
        unidade_geografica_id=unidade_geografica_id,
        periodo_id=periodo_id,
    )
    bloqueios = (
        sessao.query(tabela_semaforos)
        .filter(tabela_semaforos.c.tabela_destino == tabela_destino)
        .filter(
            tabela_semaforos.c.unidade_geografica_id == unidade_geografica_id,
        )
        .filter(tabela_semaforos.c.periodo_id == periodo_id)
        .order_by(tabela_semaforos.c.data_inicio.asc())
    )

    if len(bloqueios.all()) != 0:
        logger.error(
            "Outro processo já está escrevendo na tabela `{tabela_destino}` "
            "para a unidade geográfica `{unidade_geografica_id}` e "
            "período `{periodo_id}` (bloqueio ativo desde "
            "{data_inicio:%d/%m/%Y às %H:%M}, por `{cliente_nome}`).",
            tabela_destino=tabela_destino,
            unidade_geografica_id=unidade_geografica_id,
            periodo_id=periodo_id,
            data_inicio=bloqueios.first()["data_inicio"],
            cliente_nome=bloqueios.first()["cliente_nome"],
        )
        raise EscritaBloqueadaExcecao
    return 0


def liberar_escrita(
    sessao: Session,
    tabela_destino: str,
    unidade_geografica_id: str,
    periodo_id: str,
) -> int:
    logger.info(
        "Liberando escrita para a tabela `{tabela_destino}` e "
        + "identificadores de unidade geográfica {unidade_geografica_id} e "
        + "de período `{periodo_id}`.",
        tabela_destino=tabela_destino,
        unidade_geografica_id=unidade_geografica_id,
        periodo_id=periodo_id,
    )
    requisicao_liberacao = (
        sessao.query(tabela_semaforos)
        .filter(tabela_semaforos.c.tabela_destino == tabela_destino)
        .filter(
            tabela_semaforos.c.unidade_geografica_id == unidade_geografica_id,
        )
        .filter(tabela_semaforos.c.periodo_id == periodo_id)
    )
    requisicao_liberacao.delete()
    sessao.commit()
    return 0
