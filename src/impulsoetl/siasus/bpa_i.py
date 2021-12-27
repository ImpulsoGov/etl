# SPDX-FileCopyrightText: 2021 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Obtém dados dos Boletins de Produção Ambulatorial individualizados (BPA-i).
"""

from __future__ import annotations

import json
import uuid
from typing import Final

import janitor  # noqa: F401  # nopycln: import
import numpy as np
import pandas as pd
from frozendict import frozendict
from pysus.online_data.SIA import download
from sqlalchemy.orm import Session

from impulsoetl.comum.datas import periodo_por_data
from impulsoetl.comum.geografias import id_sus_para_id_impulso
from impulsoetl.loggers import logger
from impulsoetl.siasus.modelos import bpa_i as tabela_destino

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
        .transform_column("id", function=lambda _: uuid.uuid4().hex)
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


def carregar_bpa_i(
    sessao: Session,
    bpa_i_transformada: pd.DataFrame,
    passo: int = 1000,
) -> int:
    """Carrega os dados de um arquivo de disseminação de BPAi no BD da Impulso.

    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov.
        bpa_i_transformada: [`DataFrame`][] contendo os dados a serem
            carregados na tabela de destino, já no formato utilizado pelo banco
            de dados da ImpulsoGov (conforme retornado pela função
            [`transformar_bpa_i()`][]).
        passo: Indica quantos registros devem ser enviados para a base de dados
            de cada vez.

    Retorna:
        Código de saída do processo de carregamento. Se o carregamento
        for bem sucedido, o código de saída será `0`.

    [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
    [`DataFrame`]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    [`transformar_bpa_i()`]: impulsoetl.siasus.bpa_i.transformar_bpa_i
    """

    tabela_nome = tabela_destino.key
    num_registros = len(bpa_i_transformada)
    logger.info(
        "Carregando {num_registros} registros de BPA-i para a tabela"
        "`{tabela_nome}`...",
        num_registros=num_registros,
        tabela_nome=tabela_nome,
    )

    logger.info("Processando dados para JSON e de volta para um dicionário...")
    registros = json.loads(
        bpa_i_transformada.to_json(
            orient="records",
            date_format="iso",
        )
    )

    conector = sessao.connection()

    # Iterar por fatias do total de registro. Isso é necessário porque
    # executar todas as inserções em uma única operação acarretaria um consumo
    # proibitivo de memória
    contador = 0
    while contador <= num_registros:
        logger.info(
            "Enviando registros para a tabela de destino "
            "({contador} de {num_registros})...",
            contador=contador,
            num_registros=num_registros,
        )
        subconjunto_registros = registros[
            contador : min(num_registros, contador + passo)
        ]
        requisicao_insercao = tabela_destino.insert().values(
            subconjunto_registros,
        )
        try:
            conector.execute(requisicao_insercao)
        except Exception as err:
            mensagem_erro = str(err)
            if len(mensagem_erro) > 500:
                mensagem_erro = mensagem_erro[:500]
            logger.error(mensagem_erro)
            breakpoint()
            sessao.rollback()
            return 1

        contador += passo

    logger.info(
        "Carregamento concluído para a tabela `{tabela_nome}`: "
        + "adicionadas {linhas_adicionadas} novas linhas.",
        tabela_nome=tabela_nome,
        linhas_adicionadas=num_registros,
    )

    return 0


def obter_bpa_i(
    sessao: Session,
    uf_sigla: str,
    ano: int,
    mes: int,
    teste: bool = False,
    **kwargs,
) -> None:
    """Baixa, transforma e carrega dados de BPA-i a partir do DataSUS.

    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov.
        uf_sigla: Sigla da Unidade Federativa cujos BPA-i's se pretende obter.
        ano: Ano dos BPA-i's que se pretende obter.
        mes: Mês das BPA-i's que se pretende obter.
        teste: Indica se as modificações devem ser de fato escritas no banco de
            dados (`False`, padrão). Caso seja `True`, as modificações são
            adicionadas à uma transação, e podem ser revertidas com uma chamada
            posterior ao método [`Session.rollback()`][] da sessão gerada com o
            SQLAlchemy.

    [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
    [`sqlalchemy.engine.Row`]: https://docs.sqlalchemy.org/en/14/core/connections.html#sqlalchemy.engine.Row
    """
    logger.info(
        "Iniciando captura de BPA-i's para Unidade Federativa "
        + "'{uf_sigla}' na competencia de {mes}/{ano}.",
        uf_sigla=uf_sigla,
        ano=ano,
        mes=mes,
    )
    logger.info("Fazendo download do FTP público do DataSUS...")
    bpa_i = download(uf_sigla, year=ano, month=mes, group=["BI"])

    # TODO: paralelizar transformação e carregamento de fatias do DataFrame
    # original
    bpa_i_transformada = transformar_bpa_i(sessao=sessao, bpa_i=bpa_i)

    passo = 10 if teste else 1000
    carregar_bpa_i(
        sessao=sessao,
        bpa_i_transformada=bpa_i_transformada,
        passo=passo,
    )
    if not teste:
        sessao.commit()
