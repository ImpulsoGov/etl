# SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT

"""Casos de teste para o ETL de autorizações de internação hospitalar."""


import re

import pytest

from impulsoetl.bd import tabelas
from impulsoetl.brasilapi.cep import (
    DE_PARA_CEP,
    TIPOS_CEP,
    carregar_cep,
    extrair_cep,
    obter_cep,
    transformar_cep,
)


@pytest.fixture
def cep_dados():
    # Exemplo da documentação: https://brasilapi.com.br/docs#tag/CEP-V2
    return {
        "cep": "89010025",
        "state": "SC",
        "city": "Blumenau",
        "neighborhood": "Centro",
        "street": "Rua Doutor Luiz de Freitas Melro",
        "service": "viacep",
        "location": {
            "type": "Point",
            "coordinates": {
                "longitude": "-49.0629788",
                "latitude": "-26.9244749",
            },
        },
    }


@pytest.fixture
def cep_transformado():
    return {
        "id_cep": "89010025",
        "uf_sigla": "SC",
        "municipio_nome": "Blumenau",
        "bairro_nome": "Centro",
        "logradouro_nome_completo": "Rua Doutor Luiz de Freitas Melro",
        "fonte_nome": "viacep",
        "longitude": -49.0629788,
        "latitude": -26.9244749,
    }


def teste_de_para():
    for coluna_para in DE_PARA_CEP.values():
        assert (
            coluna_para in TIPOS_CEP
        ), "Coluna definida no De-Para, sem tipo definido: {}".format(
            coluna_para,
        )


def teste_tipos():
    tabela_destino = tabelas["listas_de_codigos.ceps"]
    colunas_destino = tabela_destino.columns

    for col in TIPOS_CEP.keys():
        assert (
            col in colunas_destino
        ), "Coluna inexistente na tabela de destino: '{}'".format(col)
    for col in colunas_destino.keys():
        assert col in TIPOS_CEP, "Coluna sem tipo definido: '{}'".format(col)


@pytest.mark.parametrize("cep", ["89010025"])
def teste_extrair_cep(cep):
    cep_dados = extrair_cep(id_cep=cep)
    assert isinstance(cep_dados, dict)
    for campo in DE_PARA_CEP:
        if campo not in ("latitude", "longitude"):
            assert (
                campo in cep_dados
            ), "Campo faltante na resposta do servidor: {}".format(campo)
    assert "location" in cep_dados
    assert "coordinates" in cep_dados["location"]


def teste_transformar_cep(cep_dados):
    cep_transformado = transformar_cep(cep_dados=cep_dados)

    assert isinstance(cep_transformado, dict)
    for campo in cep_transformado:
        assert campo in TIPOS_CEP, "Campo inesperado: {}".format(campo)
    for campo in TIPOS_CEP:
        assert campo in cep_transformado, "Campo faltante: {}".format(campo)
        assert isinstance(cep_transformado[campo], TIPOS_CEP[campo])


def teste_carregar_cep(sessao, cep_transformado, caplog):
    codigo_saida = carregar_cep(
        sessao=sessao,
        cep_transformado=cep_transformado,
    )
    assert codigo_saida == 0


@pytest.mark.integracao
@pytest.mark.parametrize("ceps_pendentes", [["89010025", "01001000"]])
def teste_obter_cep(sessao, ceps_pendentes, caplog):
    obter_cep(sessao=sessao, ceps_pendentes=ceps_pendentes, teste=True)

    logs = caplog.text
    assert "CEPs carregados com sucesso" in logs
    linhas_adicionadas = int(re.search("([0-9]+) CEPs carregados", logs)[1])
    linhas_com_falha = int(re.search("; ([0-9]+) falharam", logs)[1])
    assert linhas_adicionadas
    assert linhas_com_falha == 0
