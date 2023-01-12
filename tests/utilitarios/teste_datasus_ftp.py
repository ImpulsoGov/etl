# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Casos de teste para funções utilitárias relacionadas ao FTP do DataSUS."""


import re
from ftplib import FTP, error_perm

import pandas as pd
import pytest

from impulsoetl.utilitarios.datasus_ftp import (
    _listar_arquivos,
    extrair_dbc_lotes,
)


@pytest.fixture(scope="function")
def cliente_ftp_siasus():
    try:
        ftp = FTP("ftp.datasus.gov.br")
        ftp.login()
        ftp.cwd("/dissemin/publicos/SIASUS/200801_/Dados")
        yield ftp
    finally:
        ftp.quit()


@pytest.mark.parametrize(
    "arquivo_nome_ou_padrao",
    ["PARR2108.dbc", re.compile("PASP1112[a-z]?.dbc", re.IGNORECASE)],
)
def teste_listar_arquivos_existentes(
    cliente_ftp_siasus,
    arquivo_nome_ou_padrao,
):
    lista_arquivos = _listar_arquivos(
        cliente_ftp=cliente_ftp_siasus,
        arquivo_nome_ou_padrao=arquivo_nome_ou_padrao,
    )
    assert hasattr(lista_arquivos, "__iter__")
    assert all(isinstance(arquivo, str) for arquivo in lista_arquivos)
    assert len(lista_arquivos) > 0


@pytest.mark.parametrize(
    "arquivo_nome_ou_padrao",
    ["PAZZ2108.dbc", re.compile("PAZZ1112[a-z]?.dbc", re.IGNORECASE)],
)
def teste_listar_arquivos_inexistentes(
    cliente_ftp_siasus,
    arquivo_nome_ou_padrao,
):
    with pytest.raises(error_perm):
        lista_arquivos = _listar_arquivos(
            cliente_ftp=cliente_ftp_siasus,
            arquivo_nome_ou_padrao=arquivo_nome_ou_padrao,
        )


@pytest.mark.parametrize(
    "ftp,caminho_diretorio,arquivo_nome",
    [
        (
            "ftp.datasus.gov.br",
            "/dissemin/publicos/SIASUS/200801_/Dados",
            "SADPE1508.dbc",
        )
    ],
)
def teste_extrair_dbc_lotes(ftp, caminho_diretorio, arquivo_nome, passo):
    lotes = extrair_dbc_lotes(
        ftp=ftp,
        caminho_diretorio=caminho_diretorio,
        arquivo_nome=arquivo_nome,
        passo=passo,
    )
    lote_1 = next(lotes)
    assert isinstance(lote_1, pd.DataFrame)
    assert len(lote_1) > 0, "DataFrame vazio."
    assert len(lote_1) == passo, "Número incorreto de linhas lidas."
    assert len(lote_1.columns) > 1, "Número insuficiente de colunas lidas."
    lote_2 = next(lotes)
    assert isinstance(lote_2, pd.DataFrame)
    assert len(lote_2) > 0, "Apenas um DataFrame gerado."
