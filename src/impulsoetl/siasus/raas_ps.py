# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Obtém dados dos Registros de Ações Ambulatoriais em Saúde (RAAS)."""

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

DE_PARA_RAAS_PS: Final[frozendict] = frozendict(
    {
        "CNES_EXEC": "estabelecimento_id_scnes",
        "GESTAO": "gestao_unidade_geografica_id_sus",
        "CONDIC": "gestao_condicao_id_siasus",
        "UFMUN": "unidade_geografica_id_sus",
        "TPUPS": "estabelecimento_tipo_id_sigtap",
        "TIPPRE": "prestador_tipo_id_sigtap",
        "MN_IND": "estabelecimento_mantido",
        "CNPJCPF": "estabelecimento_id_cnpj",
        "CNPJMNT": "mantenedora_id_cnpj",
        "DT_PROCESS": "processamento_periodo_data_inicio",
        "DT_ATEND": "realizacao_periodo_data_inicio",
        "CNS_PAC": "usuario_id_cns_criptografado",
        "DTNASC": "usuario_nascimento_data",
        "TPIDADEPAC": "usuario_idade_tipo_id_sigtap",
        "IDADEPAC": "usuario_idade",
        "NACION_PAC": "usuario_nacionalidade_id_sus",
        "SEXOPAC": "usuario_sexo_id_sigtap",
        "RACACOR": "usuario_raca_cor_id_siasus",
        "ETNIA": "usuario_etnia_id_sus",
        "MUNPAC": "usuario_residencia_municipio_id_sus",
        "MOT_COB": "desfecho_motivo_id_siasus",
        "DT_MOTCOB": "desfecho_data",
        "CATEND": "carater_atendimento_id_siasus",
        "CIDPRI": "condicao_principal_id_cid10",
        "CIDASSOC": "condicao_associada_id_cid10",
        "ORIGEM_PAC": "procedencia_id_siasus",
        "DT_INICIO": "raas_data_inicio",
        "DT_FIM": "raas_data_fim",
        "COB_ESF": "esf_cobertura",
        "CNES_ESF": "esf_estabelecimento_id_scnes",
        "DESTINOPAC": "desfecho_destino_id_siasus",
        "PA_PROC_ID": "procedimento_id_sigtap",
        "PA_QTDPRO": "quantidade_apresentada",
        "PA_QTDAPR": "quantidade_aprovada",
        "PA_SRV": "servico_id_sigtap",
        "PA_CLASS_S": "servico_classificacao_id_sigtap",
        "SIT_RUA": "usuario_situacao_rua",
        "TP_DROGA": "usuario_abuso_substancias",
        "LOC_REALIZ": "local_realizacao_id_siasus",
        "INICIO": "data_inicio",
        "FIM": "data_fim",
        "PERMANEN": "permanencia_duracao",
        "QTDATE": "quantidade_atendimentos",
        "QTDPCN": "quantidade_usuarios",
        "NAT_JUR": "estabelecimento_natureza_juridica_id_scnes",
    },
)

TIPOS_RAAS_PS: Final[frozendict] = frozendict(
    {
        "estabelecimento_id_scnes": "object",
        "gestao_unidade_geografica_id_sus": "object",
        "gestao_condicao_id_siasus": "object",
        "unidade_geografica_id_sus": "object",
        "estabelecimento_tipo_id_sigtap": "object",
        "prestador_tipo_id_sigtap": "object",
        "estabelecimento_mantido": "bool",
        "estabelecimento_id_cnpj": "object",
        "mantenedora_id_cnpj": "object",
        "processamento_periodo_data_inicio": "datetime64[ns]",
        "realizacao_periodo_data_inicio": "datetime64[ns]",
        "usuario_id_cns_criptografado": "object",
        "usuario_nascimento_data": "datetime64[ns]",
        "usuario_idade_tipo_id_sigtap": "object",
        "usuario_idade": "Int64",
        "usuario_nacionalidade_id_sus": "object",
        "usuario_sexo_id_sigtap": "object",
        "usuario_raca_cor_id_siasus": "object",
        "usuario_etnia_id_sus": "object",
        "usuario_residencia_municipio_id_sus": "object",
        "desfecho_motivo_id_siasus": "object",
        "desfecho_data": "datetime64[ns]",
        "carater_atendimento_id_siasus": "object",
        "condicao_principal_id_cid10": "object",
        "condicao_associada_id_cid10": "object",
        "procedencia_id_siasus": "object",
        "raas_data_inicio": "datetime64[ns]",
        "raas_data_fim": "datetime64[ns]",
        "esf_cobertura": "bool",
        "esf_estabelecimento_id_scnes": "object",
        "desfecho_destino_id_siasus": "object",
        "procedimento_id_sigtap": "object",
        "quantidade_apresentada": "Int64",
        "quantidade_aprovada": "Int64",
        "servico_id_sigtap": "object",
        "servico_classificacao_id_sigtap": "object",
        "usuario_situacao_rua": "bool",
        "usuario_abuso_substancias": "bool",
        "usuario_abuso_substancias_alcool": "bool",
        "usuario_abuso_substancias_crack": "bool",
        "usuario_abuso_substancias_outras": "bool",
        "local_realizacao_id_siasus": "object",
        "data_inicio": "datetime64[ns]",
        "data_fim": "datetime64[ns]",
        # coluna de duração idealmente seria do tipo 'timedelta[ns]', mas esse
        # tipo não converte facilmente para o tipo INTERVEL do PostgreSQL;
        # ver https://stackoverflow.com/q/55516374/7733563
        "permanencia_duracao": "object",
        "quantidade_atendimentos": "Int64",
        "quantidade_usuarios": "Int64",
        "estabelecimento_natureza_juridica_id_scnes": "object",
        "id": str,
        "periodo_id": str,
        "unidade_geografica_id": str,
        "criacao_data": "datetime64[ns]",
        "atualizacao_data": "datetime64[ns]",
    },
)

COLUNAS_DATA_AAAAMMDD: Final[list[str]] = [
    "usuario_nascimento_data",
    "raas_data_inicio",
    "raas_data_fim",
    "data_inicio",
    "data_fim",
]

COLUNAS_DATA_AAAAMM: Final[list[str]] = [
    "realizacao_periodo_data_inicio",
    "processamento_periodo_data_inicio",
]

COLUNAS_NUMERICAS: Final[list[str]] = [
    nome_coluna
    for nome_coluna, tipo_coluna in TIPOS_RAAS_PS.items()
    if tipo_coluna == "Int64"
]


def extrair_raas_ps(
    uf_sigla: str,
    periodo_data_inicio: date,
    passo: int = 100000,
) -> Generator[pd.DataFrame, None, None]:
    """Extrai registros de RAAS Psicossociais do FTP do DataSUS.

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
        arquivo_nome="PS{uf_sigla}{periodo_data_inicio:%y%m}.dbc".format(
            uf_sigla=uf_sigla,
            periodo_data_inicio=periodo_data_inicio,
        ),
        passo=passo,
    )


def transformar_raas_ps(
    sessao: Session,
    raas_ps: pd.DataFrame,
    condicoes: str | None = None,
) -> pd.DataFrame:
    """Transforma um `DataFrame` de RAAS obtido do FTP público do DataSUS.

    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov.
        raas_ps: objeto [`pandas.DataFrame`][] contendo os dados de um arquivo
            de disseminação de Registros de Ações Ambulatoriais em Saúde -
            RAAS, conforme extraídos para uma unidade federativa e competência
            (mês) pela função [`extrair_raas()`][].
        condicoes: conjunto opcional de condições a serem aplicadas para
            filtrar os registros obtidos da fonte. O valor informado deve ser
            uma *string* com a sintaxe utilizada pelo método
            [`pandas.DataFrame.query()`][]. Por padrão, o valor do argumento é
            `None`, o que equivale a não aplicar filtro algum.

    Note:
        Para otimizar a performance, os filtros são aplicados antes de qualquer
        outra transformação nos dados, de forma que as condições fornecidas
        devem considerar que o nome, os tipos e os valores aparecem exatamente
        como registrados no arquivo de disseminação disponibilizado no FTP
        público do DataSUS. Verifique o [Informe Técnico][it-siasus] para mais
        informações.

    [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
    [`pandas.DataFrame`]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    [`extrair_bpa_i()`]: impulsoetl.siasus.raas.extrair_raas
    [`pandas.DataFrame.query()`]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.query.html
    [it-siasus]: https://drive.google.com/file/d/1DC5093njSQIhMHydYptlj2rMbrMF36y6
    """

    logger.info(
        "Transformando DataFrame com {num_registros_raas} registros de RAAS.",
        num_registros_raas=len(raas_ps),
    )

    # aplica condições de filtragem dos registros
    if condicoes:
        raas_ps = raas_ps.query(condicoes, engine="python")
        logger.info(
            "Registros após aplicar confições de filtragem: {num_registros}.",
            num_registros=len(raas_ps),
        )

    return (
        raas_ps  # noqa: WPS221  # ignorar linha complexa no pipeline
        # renomear colunas
        .rename_columns(function=lambda col: col.strip())
        .rename_columns(DE_PARA_RAAS_PS)
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
            ["usuario_situacao_rua", "esf_cobertura"],
            function=lambda elemento: True if elemento == "S" else False,
        )
        # processar coluna de uso de substâncias
        .transform_column(
            "usuario_abuso_substancias",
            function=lambda elemento: bool("A" in elemento),
            dest_column_name="usuario_abuso_substancias_alcool",
        )
        .transform_column(
            "usuario_abuso_substancias",
            function=lambda elemento: bool("C" in elemento),
            dest_column_name="usuario_abuso_substancias_crack",
        )
        .transform_column(
            "usuario_abuso_substancias",
            function=lambda elemento: bool("O" in elemento),
            dest_column_name="usuario_abuso_substancias_outras",
        )
        .transform_column(
            "usuario_abuso_substancias",
            function=lambda elemento: bool(len(elemento)),
        )
        # transformar coluna de duração
        .transform_column(
            "permanencia_duracao",
            function=lambda elemento: (
                "{} days".format(elemento) if elemento else np.nan
            ),
        )
        # tratar como NA colunas com valores nulos
        .replace("", np.nan)
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
        .astype(TIPOS_RAAS_PS)
    )
    breakpoint()


def obter_raas_ps(
    sessao: Session,
    uf_sigla: str,
    periodo_data_inicio: date,
    tabela_destino: str,
    teste: bool = False,
    **kwargs,
) -> None:
    """Baixa, transforma e carrega dados de RAAS-PS a partir do DataSUS.

    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov.
        uf_sigla: Sigla da Unidade Federativa cujas RAAS Psicossociais se
            pretende obter.
        periodo_data_inicio: Mês das RAAS Psicossociais que se pretende obter.
        tabela_destino: nome da tabela de destino, qualificado com o nome do
            schema (formato `nome_do_schema.nome_da_tabela`).
        teste: Indica se as modificações devem ser de fato escritas no banco de
            dados (`False`, padrão). Caso seja `True`, as modificações são
            adicionadas à uma transação, e podem ser revertidas com uma chamada
            posterior ao método [`Session.rollback()`][] da sessão gerada com o
            SQLAlchemy.
        \\*\\*kwargs: Parâmetros adicionais definidos no agendamento da
            captura. Atualmente, apenas o parâmetro `condicoes` (do tipo `str`)
            é aceito, e repassado como argumento na função
            [`transformar_raas_ps()`][].

    [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
    [`sqlalchemy.engine.Row`]: https://docs.sqlalchemy.org/en/14/core/connections.html#sqlalchemy.engine.Row
    [`transformar_raas_ps()`]: impulsoetl.siasus.bpa_i.transformar_raas_ps
    """
    logger.info(
        "Iniciando captura de RAAS-Psicossociais para Unidade Federativa "
        + "Federativa '{}' na competencia de {:%m/%Y}.",
        uf_sigla,
        periodo_data_inicio,
    )

    # obter tamanho do lote de processamento
    passo = int(os.getenv("IMPULSOETL_LOTE_TAMANHO", 100000))

    raas_ps_lotes = extrair_raas_ps(
        uf_sigla=uf_sigla,
        periodo_data_inicio=periodo_data_inicio,
        passo=passo,
    )

    contador = 0
    for raas_ps_lote in raas_ps_lotes:
        raas_ps_transformada = transformar_raas_ps(
            sessao=sessao,
            raas_ps=raas_ps_lote,
            condicoes=kwargs.get("condicoes"),
        )

        carregamento_status = carregar_dataframe(
            sessao=sessao,
            df=raas_ps_transformada,
            tabela_destino=tabela_destino,
            passo=None,
            teste=teste,
        )
        if carregamento_status != 0:
            raise RuntimeError(
                "Execução interrompida em razão de um erro no "
                + "carregamento."
            )
        contador += len(raas_ps_lote)
        if teste and contador > 1000:
            logger.info("Execução interrompida para fins de teste.")
            break

    if teste:
        logger.info("Desfazendo alterações realizadas durante o teste...")
        sessao.rollback()
        logger.info("Todas transações foram desfeitas com sucesso!")
