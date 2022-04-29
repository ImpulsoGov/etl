# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Funções e classes úteis para interagir com os repositórios do DataSUS."""


from __future__ import annotations

from ftplib import FTP  # noqa: B402  # nosec: B402
from pathlib import Path
from tempfile import TemporaryDirectory, NamedTemporaryFile
from typing import Generator

import pandas as pd
from dbfread import DBF
from more_itertools import ichunked
from pysus.utilities.readdbc import dbc2dbf

from impulsoetl.loggers import logger


def extrair_dbc_lotes(
    ftp: str,
    caminho_diretorio: str,
    arquivo_nome: str,
    passo: int = 10000,
) -> Generator[pd.DataFrame, None, None]:
    """Extrai dados de um arquivo .dbc do FTP do DataSUS e retorna DataFrames.

    Dados o endereço de um FTP público do DataSUS e o caminho de um diretório
    e de um arquivo localizados nesse repositório, faz download do arquivo para
    o disco, descompacta-o e itera sobre seus registros, gerando objetos
    [`pandas.DataFrames`][] com lotes de linhas lidas.

    Argumentos:
        ftp: Endereço do repositório FTP público do DataSUS.
        caminho_diretorio: Caminho do diretório onde se encontra o arquivo
            desejado no repositório.
        arquivo_nome: Nome do arquivo no formato `.dbc` desejado, incluindo a
            extensão.
        passo: Número de registros que devem ser convertidos em DataFrame a
            cada iteração.

    Gera:
        A cada iteração, devolve um objeto [`pandas.DataFrames`][] com um
        trecho do arquivo `.dbc` desejado lido e convertido. Quando o fim do
        arquivo é atingido, os registros restantes são convertidos para
        DataFrame e a conexão com o servidor FTP é encerrada.

    [`pandas.DataFrame`]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    """

    logger.info("Conectando-se ao servidor FTP `{}`...", ftp)
    cliente_ftp = FTP(ftp)
    cliente_ftp.login()
    logger.info("Conexão estabelecida com sucesso!")

    if not caminho_diretorio.startswith("/"):
        caminho_diretorio = "/" + caminho_diretorio
    logger.info("Buscando diretório `{}`...", caminho_diretorio)
    cliente_ftp.cwd(caminho_diretorio)
    logger.info("OK!")

    logger.info("Preparando ambiente para o download...")
    arquivo_dbf_nome = arquivo_nome.replace(".dbc", ".dbf")

    with TemporaryDirectory() as diretorio_temporario:
        with NamedTemporaryFile(dir=diretorio_temporario) as arquivo_dbc:
            logger.info("Tudo pronto para o download.")

            logger.info("Iniciando download do arquivo `{}`...", arquivo_nome)
            cliente_ftp.retrbinary(
                "RETR {}".format(arquivo_nome),
                arquivo_dbc.write,
            )
            logger.info("Download concluído.")

            logger.info("Descompactando arquivo DBC...")
            arquivo_dbf_caminho = Path(diretorio_temporario, arquivo_dbf_nome)
            dbc2dbf(arquivo_dbc.name, str(arquivo_dbf_caminho))
            logger.info("Lendo arquivo DBF...")
            arquivo_dbf = DBF(
                arquivo_dbf_caminho,
                encoding="iso-8859-1",
                load=False,
            )
            arquivo_dbf_fatias = ichunked(arquivo_dbf, passo)

            contador = 0
            for fatia in arquivo_dbf_fatias:
                logger.info(
                    "Lendo trecho do arquivo DBF disponibilizado pelo DataSUS "
                    + "e convertendo em DataFrame (linhas {} a {})...",
                    contador,
                    contador + passo,
                )
                yield pd.DataFrame(fatia)
                contador += passo

    logger.debug("Encerrando a conexão com o servidor FTP `{}`...", ftp)
    cliente_ftp.close()
