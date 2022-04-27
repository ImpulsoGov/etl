# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Obtém dados dos Registros de Ações Ambulatoriais em Saúde (RAAS)."""

from __future__ import annotations

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
from impulsoetl.utilitarios.bd import carregar_dataframe

DE_PARA_RAAS_PS: Final[frozendict] = frozendict(
    {
        "CNES_EXEC": "estabelecimento_id_cnes",
        "GESTAO": "gestao_unidade_geografica_id",
        "CONDIC": "gestao_condicao_id_siasus",
        "UFMUN": "unidade_geografica_id_sus",
        "TPUPS": "estabelecimento_tipo_id_sigtap",
        "TIPPRE": "prestador_tipo_id_sigtap",
        "MN_IND": "estabelecimento_mantido",
        "CNPJCPF": "estabelecimento_cnpj",
        "CNPJMNT": "mantenedora_cnpj",
        "DT_PROCESS": "processamento_periodo_data_inicio",
        "DT_ATEND": "realizacao_periodo_data_inicio",
        "CNS_PAC": "usuario_cns_criptografado",
        "DTNASC": "usuario_data_nascimento",
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
        "CNES_ESF": "esf_estabelecimento_id_cnes",
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
        "NAT_JUR": "estabelecimento_natureza_juridica_id_cnes",
    },
)

TIPOS_RAAS_PS: Final[frozendict] = frozendict(
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
        "processamento_periodo_data_inicio": "datetime64[ns]",
        "realizacao_periodo_data_inicio": "datetime64[ns]",
        "usuario_cns_criptografado": "object",
        "usuario_data_nascimento": "datetime64[ns]",
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
        "esf_estabelecimento_id_cnes": "object",
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
        "estabelecimento_natureza_juridica_id_cnes": "object",
        "id": str,
        "periodo_id": str,
        "unidade_geografica_id": str,
    },
)

COLUNAS_DATA_AAAAMMDD: Final[list[str]] = [
    "usuario_data_nascimento",
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


def transformar_raas_ps(
    sessao: Session,
    raas_ps: pd.DataFrame,
) -> pd.DataFrame:
    """Transforma um `DataFrame` de RAAS obtido do FTP público do DataSUS."""
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
        .astype(TIPOS_RAAS_PS)
    )


def obter_raas_ps(
    sessao: Session,
    uf_sigla: str,
    ano: int,
    mes: int,
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
        ano: Ano das RAAS Psicossociais que se pretende obter.
        mes: Mês das RAAS Psicossociais que se pretende obter.
        tabela_destino: nome da tabela de destino, qualificado com o nome do
            schema (formato `nome_do_schema.nome_da_tabela`).
        teste: Indica se as modificações devem ser de fato escritas no banco de
            dados (`False`, padrão). Caso seja `True`, as modificações são
            adicionadas à uma transação, e podem ser revertidas com uma chamada
            posterior ao método [`Session.rollback()`][] da sessão gerada com o
            SQLAlchemy.

    [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
    [`sqlalchemy.engine.Row`]: https://docs.sqlalchemy.org/en/14/core/connections.html#sqlalchemy.engine.Row
    """
    logger.info(
        "Iniciando captura de RAAS-Psicossociais para Unidade Federativa "
        + "'{uf_sigla}' na competencia de {mes}/{ano}.",
        uf_sigla=uf_sigla,
        ano=ano,
        mes=mes,
    )
    raas_ps = download(uf_sigla, year=ano, month=mes, group=["PS"])

    raas_ps_transformada = transformar_raas_ps(
        sessao=sessao,
        raas_ps=raas_ps,
    )
    sessao.commit()

    if teste:
        passo = 10
        pa_transformada = raas_ps_transformada.iloc[
            : min(1000, len(raas_ps_transformada)),
        ]
        if len(pa_transformada) == 1000:
            logger.warning(
                "Arquivo de RAAS truncado para 1000 registros para teste.",
            )
    else:
        passo = 10000

    carregamento_status = carregar_dataframe(
        sessao=sessao,
        df=raas_ps_transformada,
        tabela_destino=tabela_destino,
        passo=passo,
        teste=teste,
    )
    if teste or carregamento_status != 0:
        sessao.rollback()
    else:
        sessao.commit()
