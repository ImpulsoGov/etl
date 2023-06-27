# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Funções e classes úteis para interagir com os repositórios do DataSUS."""


import re
import shutil
from contextlib import closing
from ftplib import FTP, error_perm  # noqa: B402  # nosec: B402
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Generator, cast
from urllib.request import urlopen
import os
import pandas as pd
from dbfread import DBF, FieldParser
from more_itertools import ichunked
from pyreaddbc import ffi, lib

from impulsoetl.loggers import logger

def dbc2dbf(infile, outfile):
    """
    Converts a DATASUS dbc file to a DBF database saving it to `outfile`.
    :param infile: .dbc file name
    :param outfile: name of the .dbf file to be created.
    """
    if isinstance(infile, str):
        infile = infile.encode()
    if isinstance(outfile, str):
        outfile = outfile.encode()
    p = ffi.new("char[]", os.path.abspath(infile))
    q = ffi.new("char[]", os.path.abspath(outfile))

    lib.dbc2dbf([p], [q])

class LeitorCamposDBF(FieldParser):
    def parseD(self, field, data):
        # lê datas como strings
        # VER: https://dbfread.readthedocs.io/en/latest
        # /introduction.html#custom-field-types
        return self.parseC(field, data)


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


def _listar_arquivos(
    cliente_ftp: FTP,
    arquivo_nome_ou_padrao: str | re.Pattern,
) -> list[str]:
    """Busca em um diretório FTP um ou mais arquivos pelo nome ou padrão.

    Argumentos:
        cliente_ftp: Instância de conexão com o servidor FTP, já no diretório
            onde se deseja buscar os arquivos.
        arquivo_nome_ou_padrao: Nome do arquivo desejado, incluindo a
            extensão; ou expressão regular a ser comparada com os nomes de
            arquivos disponíveis no servidor FTP.

    Retorna:
        Uma lista de nomes de arquivos compatíveis com o nome ou padrão
        informados no diretório FTP.

    Exceções:
        Levanta um erro [`ftplib.error_perm`][] se nenhum arquivo
        correspondente for encontrado.

    [`ftplib.error_perm`]: https://docs.python.org/3/library/ftplib.html#ftplib.error_perm
    """

    logger.info("Listando arquivos compatíveis...")
    arquivos_todos = cliente_ftp.nlst()

    if isinstance(arquivo_nome_ou_padrao, re.Pattern):
        arquivos_compativeis = [
            arquivo
            for arquivo in arquivos_todos
            if arquivo_nome_ou_padrao.match(arquivo)
        ]
    else:
        arquivos_compativeis = [
            arquivo
            for arquivo in arquivos_todos
            if arquivo == arquivo_nome_ou_padrao
        ]

    arquivos_compativeis_num = len(arquivos_compativeis)
    if arquivos_compativeis_num > 0:
        logger.info(
            "Encontrados {numero_arquivos} arquivos.",
            numero_arquivos=arquivos_compativeis_num,
        )
        return arquivos_compativeis
    else:
        logger.error(
            "Nenhum arquivo compatível com o padrão fornecido foi "
            + "encontrado no diretório do servidor FTP."
        )
        raise error_perm


def extrair_dbc_lotes(
    ftp: str,
    caminho_diretorio: str,
    arquivo_nome: str | re.Pattern,
    passo: int = 10000,
    **kwargs,
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
            extensão; ou expressão regular a ser comparada com os nomes de
            arquivos disponíveis no servidor FTP.
        passo: Número de registros que devem ser convertidos em DataFrame a
            cada iteração.
        \*\*kwargs: Argumentos adicionais a serem passados para o construtor
            da classe
            [`dbfread.DBF`](https://dbfread.readthedocs.io/en/latest/dbf_objects.html#dbf-objects)
            ao instanciar a representação do arquivo DBF lido.

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

    arquivos_compativeis = _listar_arquivos(
        cliente_ftp=cliente_ftp,
        arquivo_nome_ou_padrao=arquivo_nome,
    )

    logger.info("Preparando ambiente para o download...")

    with TemporaryDirectory() as diretorio_temporario:
        for arquivo_compativel_nome in arquivos_compativeis:
            arquivo_dbf_nome = arquivo_compativel_nome.replace(".dbc", ".dbf")
            arquivo_dbc = Path(diretorio_temporario, arquivo_compativel_nome)
            logger.info("Tudo pronto para o download.")

            # baixar do FTP usando urllib
            # ver https://stackoverflow.com/a/11768443/7733563
            url = "ftp://{}{}/{}".format(
                ftp,
                caminho_diretorio,
                arquivo_compativel_nome,
            )

            logger.info(
                "Iniciando download do arquivo `{}`...",
                arquivo_compativel_nome,
            )
            with closing(urlopen(url)) as resposta:  # nosec: B310
                with open(arquivo_dbc, "wb") as arquivo:
                    shutil.copyfileobj(resposta, arquivo)
            logger.info("Download concluído.")

            # if _checar_arquivo_corrompido(
            #     tamanho_arquivo_ftp=cast(
            #         int,
            #         cliente_ftp.size(arquivo_compativel_nome),
            #     ),
            #     tamanho_arquivo_local=arquivo_dbc.stat().st_size,
            # ):
            #     raise RuntimeError(
            #         "A extração da fonte `{}{}` ".format(
            #             ftp,
            #             caminho_diretorio,
            #         )
            #         + "falhou porque o arquivo baixado está corrompido."
            #     )

            logger.info("Descompactando arquivo DBC...")
            arquivo_dbf_caminho = Path(diretorio_temporario, arquivo_dbf_nome)
            dbc2dbf(str(arquivo_dbc), str(arquivo_dbf_caminho))
            logger.info("Lendo arquivo DBF...")
            arquivo_dbf = DBF(
                arquivo_dbf_caminho,
                encoding="iso-8859-1",
                load=False,
                parserclass=LeitorCamposDBF,
                **kwargs,
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
