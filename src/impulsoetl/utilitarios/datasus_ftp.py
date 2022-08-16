# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Funções e classes úteis para interagir com os repositórios do DataSUS."""


from __future__ import annotations

import shutil
from contextlib import closing
from ftplib import FTP  # noqa: B402  # nosec: B402
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Generator, cast
from urllib.request import urlopen

import pandas as pd
from dbfread import DBF
from more_itertools import ichunked
from pysus.utilities.readdbc import dbc2dbf

from impulsoetl.loggers import logger


def _checar_arquivo_corrompido(
    tamanho_arquivo_ftp: int,
    tamanho_arquivo_local: int,
) -> bool:
    """Informa se um arquivo baixado do FTP está corrompido."""

    logger.info("Checando integridade do arquivo baixado...")
    logger.debug(
        "Tamanho declarado do arquivo no FTP: {:n} bytes",
        tamanho_arquivo_ftp,
    )
    logger.debug(
        "Tamanho do arquivo baixado: {:n} bytes",
        tamanho_arquivo_local,
    )
    if tamanho_arquivo_ftp > tamanho_arquivo_local:
        logger.error(
            "Tamanho no servidor é maior do que o do arquivo baixado.",
        )
        return True
    elif tamanho_arquivo_ftp < tamanho_arquivo_local:
        logger.error(
            "Tamanho no servidor é menor do que o do arquivo baixado.",
        )
        return True
    else:
        logger.info("OK!")
        return False


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
        arquivo_dbc = Path(diretorio_temporario, arquivo_nome)
        logger.info("Tudo pronto para o download.")

        logger.info("Iniciando download do arquivo `{}`...", arquivo_nome)

        # baixar do FTP usando urllib
        # ver https://stackoverflow.com/a/11768443/7733563
        with closing(
            urlopen(  # nosec: B310
                "ftp://" + ftp + caminho_diretorio + "/" + arquivo_nome,
            )
        ) as resposta:
            with open(arquivo_dbc, "wb") as arquivo:
                shutil.copyfileobj(resposta, arquivo)
        logger.info("Download concluído.")

        if _checar_arquivo_corrompido(
            tamanho_arquivo_ftp=cast(int, cliente_ftp.size(arquivo_nome)),
            tamanho_arquivo_local=arquivo_dbc.stat().st_size,
        ):
            raise RuntimeError(
                "A extração da fonte `{}{}` ".format(
                    ftp,
                    caminho_diretorio,
                )
                + "falhou porque o arquivo baixado está corrompido."
            )

        logger.info("Descompactando arquivo DBC...")
        arquivo_dbf_caminho = Path(diretorio_temporario, arquivo_dbf_nome)
        dbc2dbf(str(arquivo_dbc), str(arquivo_dbf_caminho))
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
