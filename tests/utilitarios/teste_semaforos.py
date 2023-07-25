# SPDX-FileCopyrightText: 2023 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Testes das funções para restringir e liberar a escrita no banco de dados."""


import pytest

from sqlalchemy.orm.session import Session

from impulsoetl.utilitarios.semaforos import (
    bloquear_escrita,
    checar_escrita_liberada,
    liberar_escrita,
    EscritaBloqueadaExcecao,
    tabela_semaforos,
)


@pytest.fixture()
def tabela_teste(sessao: Session):
    return "tabela_teste"


@pytest.fixture()
def limpar_testes(sessao: Session, tabela_teste: str):
    try:
        yield
    finally:
        sessao.query(tabela_semaforos).filter(
            tabela_semaforos.c.tabela_destino == tabela_teste,
        ).delete()
        sessao.commit()


@pytest.mark.parametrize(
    "unidade_geografica_id,periodo_id",
    [(
        "e8cb5dcc-46d4-45af-a237-4ab683b8ce8e",
        "9883e787-10c9-4de8-af11-9de1df09543b",
    )],
)
def teste_liberar_escrita(
    sessao: Session,
    tabela_teste: str,
    unidade_geografica_id: str,
    periodo_id: str,
    limpar_testes: None,
):

    # Insere uma entrada de bloqueio para excluí-la posteriormente
    sessao.execute(
        """
        INSERT INTO configuracoes.capturas_semaforos (
            tabela_destino, 
            unidade_geografica_id, 
            periodo_id, 
            cliente_nome, 
            cliente_ipv4
        )
        VALUES (
            :tabela_destino, 
            :unidade_geografica_id, 
            :periodo_id, 
            :cliente_nome, 
            :cliente_ipv4
        )
        """,
        {
            "tabela_destino": tabela_teste,
            "unidade_geografica_id": unidade_geografica_id,
            "periodo_id": periodo_id,
            "cliente_nome": "cliente_teste", 
            "cliente_ipv4": "127.0.0.1",
        }
    )

    # Executa a operação de liberação
    liberar_escrita(sessao, tabela_teste, unidade_geografica_id, periodo_id)

    # Verifica se a entrada de bloqueio foi removida do banco de dados
    result = sessao.execute(
        "SELECT * FROM configuracoes.capturas_semaforos",
    ).fetchall()
    assert len(result) == 0


@pytest.mark.parametrize(
    "unidade_geografica_id,periodo_id",
    [(
        "e8cb5dcc-46d4-45af-a237-4ab683b8ce8e",
        "9883e787-10c9-4de8-af11-9de1df09543b",
    )],
)
def teste_bloquear_escrita(
    sessao: Session,
    tabela_teste: str,
    unidade_geografica_id: str,
    periodo_id: str,
    limpar_testes: None,
):
    # Executa a operação de bloqueio
    bloquear_escrita(sessao, tabela_teste, unidade_geografica_id, periodo_id)

    # Verifica se a entrada de bloqueio está no banco de dados
    result = sessao.execute(
        "SELECT * FROM configuracoes.capturas_semaforos",
    ).fetchall()

    assert len(result) == 1
    assert result[0]["tabela_destino"] == tabela_teste
    assert str(result[0]["unidade_geografica_id"]) == unidade_geografica_id
    assert str(result[0]["periodo_id"]) == periodo_id


@pytest.mark.parametrize(
    "unidade_geografica_id,periodo_id",
    [(
        "e8cb5dcc-46d4-45af-a237-4ab683b8ce8e",
        "9883e787-10c9-4de8-af11-9de1df09543b",
    )],
)
def teste_checar_escrita_liberada(
    sessao: Session,
    tabela_teste: str,
    unidade_geografica_id: str,
    periodo_id: str,
    limpar_testes: None,
):

    # Verifica se checar_escrita_liberada gera a exceção apropriada quando a entrada está presente
    sessao.execute(
        """
        INSERT INTO configuracoes.capturas_semaforos (
            tabela_destino,
            unidade_geografica_id,
            periodo_id,
            cliente_nome,
            cliente_ipv4
        )
        VALUES (
            :tabela_destino, 
            :unidade_geografica_id, 
            :periodo_id, 
            :cliente_nome, 
            :cliente_ipv4
        )
        """,
        {
            "tabela_destino": tabela_teste,
            "unidade_geografica_id": unidade_geografica_id,
            "periodo_id": periodo_id,
            "cliente_nome": "cliente_teste", 
            "cliente_ipv4": "127.0.0.1",
        },
    )
    with pytest.raises(EscritaBloqueadaExcecao):
        checar_escrita_liberada(
            sessao,
            tabela_teste,
            unidade_geografica_id,
            periodo_id,
        )
