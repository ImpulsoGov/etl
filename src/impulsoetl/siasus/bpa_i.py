# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Obtém dados dos Boletins de Produção Ambulatorial individualizados (BPA-i).
"""


from __future__ import annotations

import os
from datetime import date
from typing import Final, Generator

import janitor  # noqa: F401  # nopycln: import
import numpy as np
import pandas as pd
from frozendict import frozendict
from sqlalchemy.orm import Session
from uuid6 import uuid7

from impulsoetl.comum.datas import agora_gmt_menos3, periodo_por_data
from impulsoetl.comum.geografias import id_sus_para_id_impulso
from impulsoetl.loggers import logger
from impulsoetl.utilitarios.bd import carregar_dataframe
from impulsoetl.utilitarios.datasus_ftp import extrair_dbc_lotes

DE_PARA_BPA_I: Final[frozendict] = frozendict(
    {
        "CODUNI": "estabelecimento_id_cnes",
        "GESTAO": "gestao_unidade_geografica_id",
        "CONDIC": "gestao_condicao_id_siasus",
        "UFMUN": "unidade_geografica_id_sus",
        "TPUPS": "estabelecimento_tipo_id_sigtap",
        "TIPPRE": "prestador_tipo_id_sigtap",
        "MN_IND": "estabelecimento_mantido",
        "CNPJCPF": "estabelecimento_cnpj",
        "CNPJMNT": "mantenedora_cnpj",
        "CNPJ_CC": "receptor_credito_cnpj",
        "DT_PROCESS": "processamento_periodo_data_inicio",
        "DT_ATEND": "realizacao_periodo_data_inicio",
        "PROC_ID": "procedimento_id_sigtap",
        "TPFIN": "financiamento_tipo_id_sigtap",
        "SUBFIN": "financiamento_subtipo_id_sigtap",
        "COMPLEX": "complexidade_id_siasus",
        "AUTORIZ": "autorizacao_id_siasus",
        "CNSPROF": "profissional_cns",
        "CBOPROF": "profissional_ocupacao_id_cbo",
        "CIDPRI": "condicao_principal_id_cid10",
        "CATEND": "carater_atendimento_id_siasus",
        "CNS_PAC": "usuario_cns_criptografado",
        "DTNASC": "usuario_data_nascimento",
        "TPIDADEPAC": "usuario_idade_tipo_id_sigtap",
        "IDADEPAC": "usuario_idade",
        "SEXOPAC": "usuario_sexo_id_sigtap",
        "RACACOR": "usuario_raca_cor_id_siasus",
        "MUNPAC": "usuario_residencia_municipio_id_sus",
        "QT_APRES": "quantidade_apresentada",
        "QT_APROV": "quantidade_aprovada",
        "VL_APRES": "valor_apresentado",
        "VL_APROV": "valor_aprovado",
        "UFDIF": "atendimento_residencia_ufs_distintas",
        "MNDIF": "atendimento_residencia_municipios_distintos",
        "ETNIA": "usuario_etnia_id_sus",
        "NAT_JUR": "estabelecimento_natureza_juridica_id_cnes",
    },
)

TIPOS_BPA_I: Final[frozendict] = frozendict(
    {
        "estabelecimento_id_cnes": "object",
        "gestao_unidade_geografica_id": "object",
        "gestao_condicao_id_siasus": "object",
        "unidade_geografica_id_sus": "object",
        "estabelecimento_tipo_id_sigtap": "object",
        "prestador_tipo_id_sigtap": "object",
        "estabelecimento_mantido": "bool",
        "estabelecimento_cnpj": "object",
        "mantenedora_cnpj": "object",
        "receptor_credito_cnpj": "object",
        "processamento_periodo_data_inicio": "datetime64[ns]",
        "realizacao_periodo_data_inicio": "datetime64[ns]",
        "procedimento_id_sigtap": "object",
        "financiamento_tipo_id_sigtap": "object",
        "financiamento_subtipo_id_sigtap": "object",
        "complexidade_id_siasus": "object",
        "autorizacao_id_siasus": "object",
        "profissional_cns": "object",
        "profissional_ocupacao_id_cbo": "object",
        "condicao_principal_id_cid10": "object",
        "carater_atendimento_id_siasus": "object",
        "usuario_cns_criptografado": "object",
        "usuario_data_nascimento": "datetime64[ns]",
        "usuario_idade_tipo_id_sigtap": "object",
        "usuario_idade": "Int64",
        "usuario_sexo_id_sigtap": "object",
        "usuario_raca_cor_id_siasus": "object",
        "usuario_residencia_municipio_id_sus": "object",
        "quantidade_apresentada": "Int64",
        "quantidade_aprovada": "Int64",
        "valor_apresentado": "Float64",
        "valor_aprovado": "Float64",
        "atendimento_residencia_ufs_distintas": "bool",
        "atendimento_residencia_municipios_distintos": "bool",
        "usuario_etnia_id_sus": "object",
        "estabelecimento_natureza_juridica_id_cnes": "object",
        "id": "str",
        "periodo_id": "str",
        "unidade_geografica_id": "str",
        "criacao_data": "datetime64[ns]",
        "atualizacao_data": "datetime64[ns]",
    },
)

COLUNAS_DATA_AAAAMMDD: Final[list[str]] = [
    "usuario_data_nascimento",
]

COLUNAS_DATA_AAAAMM: Final[list[str]] = [
    "realizacao_periodo_data_inicio",
    "processamento_periodo_data_inicio",
]

COLUNAS_NUMERICAS: Final[list[str]] = [
    nome_coluna
    for nome_coluna, tipo_coluna in TIPOS_BPA_I.items()
    if tipo_coluna.lower() == "int64" or tipo_coluna.lower() == "float64"
]


def extrair_bpa_i(
    uf_sigla: str,
    periodo_data_inicio: date,
    passo: int = 10000,
) -> Generator[pd.DataFrame, None, None]:
    """Extrai registros de Boletins de Produção Ambulatorial do FTP do DataSUS.

    Argumentos:
        uf_sigla: Sigla da Unidade Federativa cujos procedimentos se pretende
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
        caminho_diretorio="/dissemin/publicos/SIASUS/200801_/Dados",
        arquivo_nome="BI{uf_sigla}{periodo_data_inicio:%y%m}.dbc".format(
            uf_sigla=uf_sigla,
            periodo_data_inicio=periodo_data_inicio,
        ),
        passo=passo,
    )


def transformar_bpa_i(
    sessao: Session,
    bpa_i: pd.DataFrame,
) -> pd.DataFrame:
    """Transforma um `DataFrame` de BPA-i obtido do FTP público do DataSUS."""
    logger.info(
        "Transformando DataFrame com {num_registros_bpa_i} registros de BPAi.",
        num_registros_bpa_i=len(bpa_i),
    )
    logger.debug(
        "Memória ocupada pelo DataFrame original:  {memoria_usada:.2f} mB.",
        memoria_usada=bpa_i.memory_usage(deep=True).sum() / 10 ** 6,
    )
    bpa_i_transformada = (
        bpa_i  # noqa: WPS221  # ignorar linha complexa no pipeline
        # renomear colunas
        .rename_columns(function=lambda col: col.strip())
        .rename_columns(DE_PARA_BPA_I)
        # processar colunas com datas
        .transform_columns(
            # corrigir datas com dígito 0 substituído por espaço
            COLUNAS_DATA_AAAAMMDD,
            function=lambda dt: dt.replace(" ", "0"),
        )
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
                format="%Y%m%d",  # noqa: WPS323
                errors="coerce",
            ),
        )
        # processar colunas lógicas
        .transform_column(
            "estabelecimento_mantido",
            function=lambda elemento: True if elemento == "M" else False,
        )
        .transform_columns(
            [
                "atendimento_residencia_ufs_distintas",
                "atendimento_residencia_municipios_distintos",
            ],
            function=lambda elemento: True if elemento == "1" else False,
        )
        # tratar como NA colunas com valores nulos
        .replace("", np.nan)
        .transform_columns(
            [
                "mantenedora_cnpj",
                "receptor_credito_cnpj",
                "financiamento_subtipo_id_sigtap",
                "condicao_principal_id_cid10",
                "autorizacao_id_siasus",
            ],
            function=lambda elemento: (
                np.nan
                if all(digito == "0" for digito in elemento)
                else elemento
            ),
        )
        # adicionar id
        .add_column("id", str())
        .transform_column("id", function=lambda _: uuid7().hex)
        # adicionar id do periodo
        .transform_column(
            "realizacao_periodo_data_inicio",
            function=lambda dt: periodo_por_data(sessao=sessao, data=dt).id,
            dest_column_name="periodo_id",
        )
        # adicionar id da unidade geografica
        .transform_column(
            "unidade_geografica_id_sus",
            function=lambda id_sus: id_sus_para_id_impulso(
                sessao=sessao,
                id_sus=id_sus,
            ),
            dest_column_name="unidade_geografica_id",
        )
        # adicionar datas de inserção e atualização
        .add_column("criacao_data", agora_gmt_menos3())
        .add_column("atualizacao_data", agora_gmt_menos3())
        # garantir tipos
        .change_type(
            # HACK: ver https://github.com/pandas-dev/pandas/issues/25472
            COLUNAS_NUMERICAS,
            "float",
        )
        .astype(TIPOS_BPA_I)
    )
    logger.debug(
        "Memória ocupada pelo DataFrame transformado: {memoria_usada:.2f} mB.",
        memoria_usada=(
            bpa_i_transformada.memory_usage(deep=True).sum() / 10 ** 6
        ),
    )
    return bpa_i_transformada


def obter_bpa_i(
    sessao: Session,
    uf_sigla: str,
    periodo_data_inicio: date,
    tabela_destino: str,
    teste: bool = False,
    **kwargs,
) -> None:
    """Baixa, transforma e carrega dados de BPA-i a partir do DataSUS.

    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov.
        uf_sigla: Sigla da Unidade Federativa cujos BPA-i's se pretende obter.
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
    logger.info(
        "Iniciando captura de BPA-i's para Unidade Federativa "
        + "Federativa '{}' na competencia de {:%m/%Y}.",
        uf_sigla,
        periodo_data_inicio,
    )

    # obter tamanho do lote de processamento
    passo = int(os.getenv("IMPULSOETL_LOTE_TAMANHO", 100000))

    bpa_i_lotes = extrair_bpa_i(
        uf_sigla=uf_sigla,
        periodo_data_inicio=periodo_data_inicio,
        passo=passo,
    )

    contador = 0
    for bpa_i_lote in bpa_i_lotes:
        bpa_i_transformada = transformar_bpa_i(
            sessao=sessao,
            bpa_i=bpa_i_lote,
        )

        carregamento_status = carregar_dataframe(
            sessao=sessao,
            df=bpa_i_transformada,
            tabela_destino=tabela_destino,
            passo=None,
            teste=teste,
        )
        if carregamento_status != 0:
            raise RuntimeError(
                "Execução interrompida em razão de um erro no "
                + "carregamento."
            )
        contador += len(bpa_i_lote)
        if teste and contador > 1000:
            logger.info("Execução interrompida para fins de teste.")
            break

    if teste:
        logger.info("Desfazendo alterações realizadas durante o teste...")
        sessao.rollback()
        logger.info("Todas transações foram desfeitas com sucesso!")
