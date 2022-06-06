# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Casos de teste para funções utilitárias relacionadas ao FTP do DataSUS."""


import pandas as pd
import pytest

from impulsoetl.utilitarios.datasus_ftp import extrair_dbc_lotes


@pytest.mark.parametrize(
    "ftp,caminho_diretorio,arquivo_nome",
    [(
        "ftp.datasus.gov.br",
        "/dissemin/publicos/SIASUS/200801_/Dados",
        "SADPE1508.dbc",
    )]
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
