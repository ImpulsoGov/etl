# SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Ferramentas para obter habilitações dos estabelecimentos no SCNES."""


import os
import re
from datetime import date
from typing import Final, Generator

import janitor  # noqa: F401  # nopycln: import
import numpy as np
import pandas as pd
import roman
from frozendict import frozendict
from prefect import flow, task
from sqlalchemy.orm import Session
from uuid6 import uuid7

from impulsoetl import __VERSION__
from impulsoetl.comum.datas import agora_gmt_menos3, periodo_por_data
from impulsoetl.comum.geografias import id_sus_para_id_impulso
from impulsoetl.loggers import habilitar_suporte_loguru, logger
from impulsoetl.utilitarios.bd import carregar_dataframe
from impulsoetl.utilitarios.datasus_ftp import extrair_dbc_lotes

DE_PARA_HABILITACOES: Final[frozendict] = frozendict(
    {
        "CNES": "estabelecimento_id_scnes",
        "CODUFMUN": "estabelecimento_municipio_id_sus",
        "REGSAUDE": "estabelecimento_regiao_saude_id_sus",
        "MICR_REG": "estabelecimento_microrregiao_saude_id_sus",
        "DISTRSAN": "estabelecimento_distrito_sanitario_id_sus",
        "DISTRADM": "estabelecimento_distrito_administrativo_id_sus",
        "TPGESTAO": "estabelecimento_gestao_condicao_id_scnes",
        "PF_PJ": "estabelecimento_personalidade_juridica_id_scnes",
        "CPF_CNPJ": "estabelecimento_id_cpf_cnpj",
        "NIV_DEP": "estabelecimento_mantido",
        "CNPJ_MAN": "estabelecimento_mantenedora_id_cnpj",
        "ESFERA_A": "estabelecimento_esfera_id_scnes",
        "ATIVIDAD": "estabelecimento_atividade_ensino_id_scnes",
        "RETENCAO": "estabelecimento_tributos_retencao_id_scnes",
        "NATUREZA": "estabelecimento_natureza_id_scnes",
        "CLIENTEL": "estabelecimento_fluxo_id_scnes",
        "TP_UNID": "estabelecimento_tipo_id_scnes",
        "TURNO_AT": "estabelecimento_turno_id_scnes",
        "NIV_HIER": "estabelecimento_hierarquia_id_scnes",
        "TERCEIRO": "estabelecimento_terceiro",
        "COD_CEP": "estabelecimento_cep",
        "VINC_SUS": "atendimento_sus",
        "TP_PREST": "prestador_tipo_id_fca",
        "SGRUPHAB": "habilitacao_id_scnes",
        "COMPETEN": "periodo_data_inicio",
        "CMPT_INI": "vigencia_data_inicio",
        "CMPT_FIM": "vigencia_data_fim",
        "DTPORTAR": "portaria_data",
        "PORTARIA": "portaria_nome",
        "MAPORTAR": "portaria_periodo_data_inicio",
        "NULEITOS": "leitos_quantidade",
        "NAT_JUR": "estabelecimento_natureza_juridica_id_scnes",
    },
)

TIPOS_HABILITACOES: Final[frozendict] = frozendict(
    {
        "id": "object",
        "unidade_geografica_id": "object",
        "periodo_id": "object",
        "estabelecimento_id_scnes": "object",
        "estabelecimento_municipio_id_sus": "object",
        "estabelecimento_regiao_saude_id_sus": "object",
        "estabelecimento_microrregiao_saude_id_sus": "object",
        "estabelecimento_distrito_sanitario_id_sus": "object",
        "estabelecimento_distrito_administrativo_id_sus": "object",
        "estabelecimento_gestao_condicao_id_scnes": "object",
        "estabelecimento_personalidade_juridica_id_scnes": "object",
        "estabelecimento_id_cpf_cnpj": "object",
        "estabelecimento_mantido": "boolean",
        "estabelecimento_mantenedora_id_cnpj": "object",
        "estabelecimento_esfera_id_scnes": "object",
        "estabelecimento_tributos_retencao_id_scnes": "object",
        "estabelecimento_natureza_id_scnes": "object",
        "estabelecimento_fluxo_id_scnes": "object",
        "estabelecimento_atividade_ensino_id_scnes": "object",
        "estabelecimento_tipo_id_scnes": "object",
        "estabelecimento_turno_id_scnes": "object",
        "estabelecimento_hierarquia_id_scnes": "object",
        "estabelecimento_terceiro": "boolean",
        "atendimento_sus": "boolean",
        "prestador_tipo_id_fca": "object",
        "habilitacao_id_scnes": "object",
        "vigencia_data_inicio": "datetime64[ns]",
        "vigencia_data_fim": "datetime64[ns]",
        "portaria_data": "datetime64[ns]",
        "portaria_nome": "object",
        "portaria_periodo_data_inicio": "datetime64[ns]",
        "leitos_quantidade": "int64",
        "periodo_data_inicio": "datetime64[ns]",
        "estabelecimento_natureza_juridica_id_scnes": "object",
        "estabelecimento_cep": "object",
        "criacao_data": "datetime64[ns]",
        "atualizacao_data": "datetime64[ns]",
    },
)

COLUNAS_DATA_AAAAMM: Final[list[str]] = [
    "periodo_data_inicio",
    "vigencia_data_inicio",
    "vigencia_data_fim",
    "portaria_periodo_data_inicio",
]

COLUNAS_DATA_AAAAMMDD: Final[list[str]] = ["portaria_data"]

COLUNAS_NUMERICAS: Final[list[str]] = [
    nome_coluna
    for nome_coluna, tipo_coluna in TIPOS_HABILITACOES.items()
    if tipo_coluna.lower() == "int64" or tipo_coluna.lower() == "float64"
]


def _para_booleano(valor: str) -> bool | float:
    """Transforma um valor binário '0' ou '1' em booleano. Suporta NaNs."""
    if valor == "0":
        return False
    elif valor == "1":
        return True
    else:
        return np.nan


def _romano_para_inteiro(texto: str) -> str | float:
    if pd.isna(texto):
        return np.nan
    try:
        return str(roman.fromRoman(texto))
    except roman.InvalidRomanNumeralError:
        return texto


def extrair_habilitacoes(
    uf_sigla: str,
    periodo_data_inicio: date,
    passo: int = 10000,
) -> Generator[pd.DataFrame, None, None]:
    """Extrai registros de habilitações de estabelecimentos do FTP do DataSUS.

    Argumentos:
        uf_sigla: Sigla da Unidade Federativa cujos estabelecimentos se pretende
            obter.
        periodo_data_inicio: Dia de início da competência desejada,
            representado como um objeto [`datetime.date`][].
        passo: Número de registros que devem ser convertidos em DataFrame a
            cada iteração.

    Gera:
        A cada iteração, devolve um objeto [`pandas.DataFrames`][] com um
        trecho do arquivo de procedimentos ambulatoriais lido e convertido.

    [`pandas.DataFrame`]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    [`datetime.date`]: https://docs.python.org/3/library/datetime.html#date-objects
    """

    return extrair_dbc_lotes(
        ftp="ftp.datasus.gov.br",
        caminho_diretorio="/dissemin/publicos/CNES/200508_/Dados/HB",
        arquivo_nome="HB{uf_sigla}{periodo_data_inicio:%y%m}.dbc".format(
            uf_sigla=uf_sigla,
            periodo_data_inicio=periodo_data_inicio,
        ),
        passo=passo,
    )


@task(
    name="Transformar Habilitações",
    description=(
        "Transforma os dados dos arquivos de disseminação de habilitações dos "
        + "estabelecimentos de saúde a partir do repositório público do "
        + "Sistema do Cadastro Nacional de Estabelecimentos de Saúde do SUS."
    ),
    tags=["saude_mental", "scnes", "habilitacoes", "transformacao"],
    retries=0,
    retry_delay_seconds=None,
)
def transformar_habilitacoes(
    sessao: Session,
    habilitacoes: pd.DataFrame,
) -> pd.DataFrame:
    """Transforma um `DataFrame` de habilitações do SCNES.

        Argumentos:
            sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
                acessar a base de dados da ImpulsoGov.
            habilitações: [`DataFrame`][] contendo os dados a serem transformados
                (conforme retornado pela função
                [`pysus.online_data.CNES.download()`][] com o argumento
                `group='PF'`).
    ß
        Retorna:
            Um [`DataFrame`][] com dados de habilitações de estabelecimentos tratados para
            inserção no banco de dados da ImpulsoGov.

        [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
        [`DataFrame`]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
        [`pysus.online_data.CNES.download()`]: http://localhost:9090/@https://github.com/AlertaDengue/PySUS/blob/600c61627b7998a1733b71ac163b3de71324cfbe/pysus/online_data/CNES.py#L28
    """
    habilitar_suporte_loguru()
    logger.info(
        "Transformando DataFrame com {num_registros} habilitações "
        + "de estabelecimentos do SCNES.",
        num_registros=len(habilitacoes),
    )
    logger.debug(
        "Memória ocupada pelo DataFrame original:  {memoria_usada:.2f} mB.",
        memoria_usada=habilitacoes.memory_usage(deep=True).sum() / 10**6,
    )
    habilitacoes_transformado = (
        habilitacoes  # noqa: WPS221  # ignorar linha complexa no pipeline
        # renomear colunas
        .rename_columns(function=lambda col: col.strip())
        .rename_columns(DE_PARA_HABILITACOES)
        # adicionar datas de inserção e atualização
        .add_column("criacao_data", agora_gmt_menos3())
        .add_column("atualizacao_data", agora_gmt_menos3())
        # processar colunas com datas
        .transform_columns(
            COLUNAS_DATA_AAAAMM,
            function=lambda dt: pd.to_datetime(
                dt,
                format="%Y%m",
                errors="coerce",
            ),
        )
        .transform_columns(
            COLUNAS_DATA_AAAAMMDD,
            function=lambda dt: pd.to_datetime(
                dt,
                format="%d/%m/%Y",
                errors="coerce",
            ),
        )
        # limpar e completar códigos de região e distrito de saúde
        .transform_column(
            "estabelecimento_regiao_saude_id_sus",
            _romano_para_inteiro,
        )
        .transform_column(
            "estabelecimento_regiao_saude_id_sus",
            lambda id_sus: (
                re.sub("[^0-9]", "", id_sus) if pd.notna(id_sus) else np.nan
            ),
        )
        .transform_columns(
            [
                "estabelecimento_regiao_saude_id_sus",
                "estabelecimento_distrito_sanitario_id_sus",
                "estabelecimento_distrito_administrativo_id_sus",
            ],
            lambda id_sus: (id_sus.zfill(4) if pd.notna(id_sus) else np.nan),
        )
        .transform_column(
            "estabelecimento_microrregiao_saude_id_sus",
            lambda id_sus: (id_sus.zfill(6) if pd.notna(id_sus) else np.nan),
        )
        # tratar como NA colunas com valores nulos
        .replace("", np.nan)
        .transform_columns(
            [
                "estabelecimento_regiao_saude_id_sus",
                "estabelecimento_microrregiao_saude_id_sus",
                "estabelecimento_distrito_sanitario_id_sus",
                "estabelecimento_distrito_administrativo_id_sus",
                "estabelecimento_id_cpf_cnpj",
                "estabelecimento_mantenedora_id_cnpj",
            ],
            function=lambda elemento: (
                np.nan
                if pd.notna(elemento)
                and all(digito == "0" for digito in elemento)
                else elemento
            ),
        )
        # processar colunas lógicas
        .transform_column(
            "estabelecimento_mantido",
            function=lambda elemento: True if elemento == "1" else False,
        )
        .transform_columns(
            [
                "estabelecimento_terceiro",
                "atendimento_sus",
            ],
            function=_para_booleano,
        )
        # adicionar id
        .add_column("id", str())
        .transform_column("id", function=lambda _: uuid7().hex)
        # adicionar id do periodo
        .transform_column(
            "periodo_data_inicio",
            function=lambda dt: periodo_por_data(sessao=sessao, data=dt).id,
            dest_column_name="periodo_id",
        )
        # adicionar id da unidade geografica
        .transform_column(
            "estabelecimento_municipio_id_sus",
            function=lambda id_sus: id_sus_para_id_impulso(
                sessao=sessao,
                id_sus=id_sus,
            ),
            dest_column_name="unidade_geografica_id",
        )
        # garantir tipos
        .change_type(
            # HACK: ver https://github.com/pandas-dev/pandas/issues/25472
            COLUNAS_NUMERICAS,
            "float",
        )
        .astype(TIPOS_HABILITACOES)
    )
    logger.debug(
        "Memória ocupada pelo DataFrame transformado: {memoria_usada:.2f} mB.",
        memoria_usada=(
            habilitacoes_transformado.memory_usage(deep=True).sum() / 10**6
        ),
    )
    return habilitacoes_transformado


@flow(
    name="Obter Habilitações",
    description=(
        "Extrai, transforma e carrega os dados dos arquivos de disseminação "
        + "das habilitações dos estabelecimentos a partir do repositório "
        + "público do Sistema do Cadastro Nacional de Estabelecimentos de "
        + "Saúde do SUS."
    ),
    retries=0,
    retry_delay_seconds=None,
    version=__VERSION__,
    validate_parameters=False,
)
def obter_habilitacoes(
    sessao: Session,
    uf_sigla: str,
    periodo_data_inicio: date,
    tabela_destino: str,
    teste: bool = False,
    **kwargs,
) -> None:
    """Baixa, transforma e carrega dados de habilitações.

    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov.
        uf_sigla: Sigla da Unidade Federativa onde se encontram os
            estabelecimentos cujas habilitações se pretende obter.
        periodo_data_inicio: Dia de início da competência desejada,
            representado como um objeto [`datetime.date`][].
        tabela_destino: nome da tabela de destino, qualificado com o nome do
            schema (formato `nome_do_schema.nome_da_tabela`).
        teste: Indica se as modificações devem ser de fato escritas no banco de
            dados (`False`, padrão). Caso seja `True`, as modificações são
            adicionadas à uma transação, e podem ser revertidas com uma chamada
            posterior ao método [`Session.rollback()`][] da sessão gerada com o
            SQLAlchemy.

    [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
    [`sqlalchemy.engine.Row`]: https://docs.sqlalchemy.org/en/14/core/connections.html#sqlalchemy.engine.Row
    [`datetime.date`]: https://docs.python.org/3/library/datetime.html#date-objects
    """
    habilitar_suporte_loguru()
    logger.info(
        "Iniciando captura de habilitacoes da Unidade "
        + "Federativa '{}' na competencia de {:%m/%Y}.",
        uf_sigla,
        periodo_data_inicio,
    )

    # obter tamanho do lote de processamento
    passo = int(os.getenv("IMPULSOETL_LOTE_TAMANHO", 100000))

    habilitacoes_lotes = extrair_habilitacoes(
        uf_sigla=uf_sigla,
        periodo_data_inicio=periodo_data_inicio,
        passo=passo,
    )

    contador = 0
    for habilitacoes_lote in habilitacoes_lotes:
        habilitacoes_transformada = transformar_habilitacoes(
            sessao=sessao,
            habilitacoes=habilitacoes_lote,
        )

        carregamento_status = carregar_dataframe(
            sessao=sessao,
            df=habilitacoes_transformada,
            tabela_destino=tabela_destino,
            passo=None,
            teste=teste,
        )
        if carregamento_status != 0:
            raise RuntimeError(
                "Execução interrompida em razão de um erro no "
                + "carregamento."
            )
        contador += len(habilitacoes_transformada)
        if teste and contador > 1000:
            logger.info("Execução interrompida para fins de teste.")
            break

    if teste:
        logger.info("Desfazendo alterações realizadas durante o teste...")
        sessao.rollback()
        logger.info("Todas transações foram desfeitas com sucesso!")
