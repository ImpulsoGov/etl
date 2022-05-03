# SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Obtém dados de procedimentos ambulatoriais registrados no SIASUS."""


from __future__ import annotations

import os
import uuid
from typing import Final

import janitor  # noqa: F401  # nopycln: import
import numpy as np
import pandas as pd
from frozendict import frozendict
from pysus.online_data.SIH import download
from sqlalchemy.orm import Session

from impulsoetl.comum.datas import periodo_por_data
from impulsoetl.comum.geografias import id_sus_para_id_impulso
from impulsoetl.loggers import logger
from impulsoetl.utilitarios.bd import carregar_dataframe

DE_PARA_AIH_RD: Final[frozendict] = frozendict(
    {
        "UF_ZI": "gestao_unidade_geografica_id_sus",
        "ANO_CMPT": "processamento_periodo_ano_inicio",
        "MES_CMPT": "processamento_periodo_mes_inicio",
        "ESPEC": "leito_especialidade_id_sigtap",
        "CGC_HOSP": "estabelecimento_cnpj",
        "N_AIH": "aih_id_sihsus",
        "IDENT": "aih_tipo_id_sihsus",
        "CEP": "usuario_residencia_cep",
        "MUNIC_RES": "usuario_residencia_municipio_id_sus",
        "NASC": "usuario_data_nascimento",
        "SEXO": "usuario_sexo_id_sigtap",
        "UTI_MES_TO": "uti_diarias",
        "MARCA_UTI": "uti_tipo_id_sihsus",
        "UTI_INT_TO": "unidade_intermediaria_diarias",
        "DIAR_ACOM": "acompanhante_diarias",
        "QT_DIARIAS": "diarias",
        "PROC_SOLIC": "procedimento_solicitado_id_sigtap",
        "PROC_REA": "procedimento_realizado_id_sigtap",
        "VAL_SH": "valor_servicos_hospitalares",
        "VAL_SP": "valor_servicos_profissionais",
        "VAL_TOT": "valor_total",
        "VAL_UTI": "valor_uti",
        "US_TOT": "valor_total_dolar",
        "DT_INTER": "aih_data_inicio",
        "DT_SAIDA": "aih_data_fim",
        "DIAG_PRINC": "condicao_principal_id_cid10",
        "DIAG_SECUN": "condicao_secundaria_id_cid10",
        "COBRANCA": "desfecho_motivo_id_sihsus",
        "NATUREZA": "estabelecimento_natureza_id_cnes",
        "NAT_JUR": "estabelecimento_natureza_juridica_id_cnes",
        "GESTAO": "gestao_condicao_id_sihsus",
        "IND_VDRL": "exame_vdrl",
        "MUNIC_MOV": "unidade_geografica_id_sus",
        "COD_IDADE": "usuario_idade_tipo_id_sigtap",
        "IDADE": "usuario_idade",
        "DIAS_PERM": "permanencia_duracao",
        "MORTE": "obito",
        "NACIONAL": "usuario_nacionalidade_id_sigtap",
        "CAR_INT": "carater_atendimento_id_sihsus",
        "HOMONIMO": "usuario_homonimo",
        "NUM_FILHOS": "usuario_filhos_quantidade",
        "INSTRU": "usuario_instrucao_id_sihsus",
        "CID_NOTIF": "condicao_notificacao_id_cid10",
        "CONTRACEP1": "usuario_contraceptivo_principal_id_sihsus",
        "CONTRACEP2": "usuario_contraceptivo_secundario_id_sihsus",
        "GESTRISCO": "gestacao_risco",
        "INSC_PN": "usuario_id_pre_natal",
        "SEQ_AIH5": "remessa_sequencial_aih5_id_sihsus",
        "CBOR": "usuario_ocupacao_id_cbo",
        "CNAER": "usuario_atividade_id_cnae",
        "VINCPREV": "usuario_vinculo_previdencia_id_sihsus",
        "GESTOR_COD": "autorizacao_gestor_motivo_id_sihsus",
        "GESTOR_TP": "autorizacao_gestor_tipo_id_sihsus",
        "GESTOR_CPF": "autorizacao_gestor_id_cpf",
        "GESTOR_DT": "autorizacao_gestor_data",
        "CNES": "estabelecimento_id_cnes",
        "CNPJ_MANT": "mantenedora_cnpj",
        "INFEHOSP": "infeccao_hospitalar",
        "CID_ASSO": "condicao_associada_id_cid10",
        "CID_MORTE": "condicao_obito_id_cid10",
        "COMPLEX": "complexidade_id_sihsus",
        "FINANC": "financiamento_tipo_id_sigtap",
        "FAEC_TP": "financiamento_subtipo_id_sigtap",
        "REGCT": "regra_contratual_id_cnes",
        "RACA_COR": "usuario_raca_cor_id_sihsus",
        "ETNIA": "usuario_etnia_id_sus",
        "SEQUENCIA": "remessa_sequencial_id_sihsus",
        "REMESSA": "remessa_id_sihsus",
        "AUD_JUST": "cns_ausente_justificativa_auditor",
        "SIS_JUST": "cns_ausente_justificativa_estabelecimento",
        "VAL_SH_FED": "valor_servicos_hospitalares_complemento_federal",
        "VAL_SP_FED": "valor_servicos_profissionais_complemento_federal",
        "VAL_SH_GES": "valor_servicos_hospitalares_complemento_local",
        "VAL_SP_GES": "valor_servicos_profissionais_complemento_local",
        "VAL_UCI": "valor_unidade_neonatal",
        "MARCA_UCI": "unidade_neonatal_tipo_id_sihsus",
        "DIAGSEC1": "condicao_secundaria_1_id_cid10",
        "DIAGSEC2": "condicao_secundaria_2_id_cid10",
        "DIAGSEC3": "condicao_secundaria_3_id_cid10",
        "DIAGSEC4": "condicao_secundaria_4_id_cid10",
        "DIAGSEC5": "condicao_secundaria_5_id_cid10",
        "DIAGSEC6": "condicao_secundaria_6_id_cid10",
        "DIAGSEC7": "condicao_secundaria_7_id_cid10",
        "DIAGSEC8": "condicao_secundaria_8_id_cid10",
        "DIAGSEC9": "condicao_secundaria_9_id_cid10",
        "TPDISEC1": "condicao_secundaria_1_tipo_id_sihsus",
        "TPDISEC2": "condicao_secundaria_2_tipo_id_sihsus",
        "TPDISEC3": "condicao_secundaria_3_tipo_id_sihsus",
        "TPDISEC4": "condicao_secundaria_4_tipo_id_sihsus",
        "TPDISEC5": "condicao_secundaria_5_tipo_id_sihsus",
        "TPDISEC6": "condicao_secundaria_6_tipo_id_sihsus",
        "TPDISEC7": "condicao_secundaria_7_tipo_id_sihsus",
        "TPDISEC8": "condicao_secundaria_8_tipo_id_sihsus",
        "TPDISEC9": "condicao_secundaria_9_tipo_id_sihsus",
    },
)

TIPOS_AIH_RD: Final[frozendict] = frozendict(
    {
        "gestao_unidade_geografica_id_sus": "object",
        "periodo_data_inicio": "datetime64[ns]",
        "leito_especialidade_id_sigtap": "object",
        "estabelecimento_cnpj": "object",
        "aih_id_sihsus": "object",
        "aih_tipo_id_sihsus": "object",
        "usuario_residencia_cep": "object",
        "usuario_residencia_municipio_id_sus": "object",
        "usuario_data_nascimento": "datetime64[ns]",
        "usuario_sexo_id_sigtap": "object",
        "uti_diarias": "int64",
        "uti_tipo_id_sihsus": "object",
        "unidade_intermediaria_diarias": "int64",
        "acompanhante_diarias": "int64",
        "diarias": "int64",
        "procedimento_solicitado_id_sigtap": "object",
        "procedimento_realizado_id_sigtap": "object",
        "valor_servicos_hospitalares": "object",
        "valor_servicos_profissionais": "object",
        "valor_total": "float64",
        "valor_uti": "float64",
        "valor_total_dolar": "float64",
        "aih_data_inicio": "datetime64[ns]",
        "aih_data_fim": "datetime64[ns]",
        "condicao_principal_id_cid10": "object",
        "condicao_secundaria_id_cid10": "object",
        "desfecho_motivo_id_sihsus": "object",
        "estabelecimento_natureza_id_cnes": "object",
        "estabelecimento_natureza_juridica_id_cnes": "object",
        "gestao_condicao_id_sihsus": "object",
        "exame_vdrl": "bool",
        "unidade_geografica_id_sus": "object",
        "usuario_idade_tipo_id_sigtap": "object",
        "usuario_idade": "int64",
        "permanencia_duracao": "int64",
        "obito": "bool",
        "usuario_nacionalidade_id_sigtap": "object",
        "carater_atendimento_id_sihsus": "object",
        "usuario_homonimo": "bool",
        "usuario_filhos_quantidade": "Int64",
        "usuario_instrucao_id_sihsus": "object",
        "condicao_notificacao_id_cid10": "object",
        "usuario_contraceptivo_principal_id_sihsus": "object",
        "usuario_contraceptivo_secundario_id_sihsus": "object",
        "gestacao_risco": "bool",
        "usuario_id_pre_natal": "object",
        "remessa_sequencial_aih5_id_sihsus": "object",
        "usuario_ocupacao_id_cbo": "object",
        "usuario_atividade_id_cnae": "object",
        "usuario_vinculo_previdencia_id_sihsus": "object",
        "autorizacao_gestor_motivo_id_sihsus": "object",
        "autorizacao_gestor_tipo_id_sihsus": "object",
        "autorizacao_gestor_id_cpf": "object",
        "autorizacao_gestor_data": "datetime64[ns]",
        "estabelecimento_id_cnes": "object",
        "mantenedora_cnpj": "object",
        "infeccao_hospitalar": "bool",
        "condicao_associada_id_cid10": "object",
        "condicao_obito_id_cid10": "object",
        "complexidade_id_sihsus": "object",
        "financiamento_tipo_id_sigtap": "object",
        "financiamento_subtipo_id_sigtap": "object",
        "regra_contratual_id_cnes": "object",
        "usuario_raca_cor_id_sihsus": "object",
        "usuario_etnia_id_sus": "object",
        "remessa_sequencial_id_sihsus": "object",
        "remessa_id_sihsus": "object",
        "cns_ausente_justificativa_auditor": "object",
        "cns_ausente_justificativa_estabelecimento": "object",
        "valor_servicos_hospitalares_complemento_federal": "float64",
        "valor_servicos_profissionais_complemento_federal": "float64",
        "valor_servicos_hospitalares_complemento_local": "float64",
        "valor_servicos_profissionais_complemento_local": "float64",
        "valor_unidade_neonatal": "float64",
        "unidade_neonatal_tipo_id_sihsus": "object",
        "condicao_secundaria_1_id_cid10": "object",
        "condicao_secundaria_2_id_cid10": "object",
        "condicao_secundaria_3_id_cid10": "object",
        "condicao_secundaria_4_id_cid10": "object",
        "condicao_secundaria_5_id_cid10": "object",
        "condicao_secundaria_6_id_cid10": "object",
        "condicao_secundaria_7_id_cid10": "object",
        "condicao_secundaria_8_id_cid10": "object",
        "condicao_secundaria_9_id_cid10": "object",
        "condicao_secundaria_1_tipo_id_sihsus": "object",
        "condicao_secundaria_2_tipo_id_sihsus": "object",
        "condicao_secundaria_3_tipo_id_sihsus": "object",
        "condicao_secundaria_4_tipo_id_sihsus": "object",
        "condicao_secundaria_5_tipo_id_sihsus": "object",
        "condicao_secundaria_6_tipo_id_sihsus": "object",
        "condicao_secundaria_7_tipo_id_sihsus": "object",
        "condicao_secundaria_8_tipo_id_sihsus": "object",
        "condicao_secundaria_9_tipo_id_sihsus": "object",
        "id": "object",
        "periodo_id": "object",
        "unidade_geografica_id": "object",
    },
)

COLUNAS_DATA_AAAAMMDD: Final[list[str]] = [
    "usuario_data_nascimento",
    "aih_data_inicio",
    "aih_data_fim",
    "autorizacao_gestor_data",
]

COLUNAS_NUMERICAS: Final[list[str]] = [
    nome_coluna
    for nome_coluna, tipo_coluna in TIPOS_AIH_RD.items()
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


def transformar_aih_rd(
    sessao: Session,
    aih_rd: pd.DataFrame,
) -> pd.DataFrame:
    """Transforma um `DataFrame` de autorizações de internação do SIHSUS."""
    logger.info(
        "Transformando DataFrame com {num_registros_aih_rd} procedimentos "
        + "ambulatoriais.",
        num_registros_aih_rd=len(aih_rd),
    )
    logger.debug(
        "Memória ocupada pelo DataFrame original:  {memoria_usada:.2f} mB.",
        memoria_usada=aih_rd.memory_usage(deep=True).sum() / 10 ** 6,
    )
    aih_rd_transformada = (
        aih_rd.select_columns(  # noqa: WPS221  # ignorar linha complexa no pipeline
            DE_PARA_AIH_RD.keys()
        )
        # renomear colunas
        .rename_columns(function=lambda col: col.strip())
        .rename_columns(DE_PARA_AIH_RD)
        # processar colunas com datas
        .join_apply(
            lambda i: pd.Timestamp(
                int(i["processamento_periodo_ano_inicio"]),
                int(i["processamento_periodo_mes_inicio"]),
                1,
            ),
            new_column_name="periodo_data_inicio",
        )
        .remove_columns(
            [
                "processamento_periodo_ano_inicio",
                "processamento_periodo_mes_inicio",
            ]
        )
        .transform_columns(
            COLUNAS_DATA_AAAAMMDD,
            function=lambda dt: pd.to_datetime(
                dt,
                format="%Y%m%d",
                errors="coerce",
            ),
        )
        # tratar como NA colunas com valores nulos
        .replace("", np.nan)
        .change_type("usuario_filhos_quantidade", str)
        .transform_columns(
            [
                "uti_tipo_id_sihsus",
                "condicao_secundaria_id_cid10",
                "estabelecimento_natureza_id_cnes",
                "estabelecimento_natureza_juridica_id_cnes",
                "usuario_instrucao_id_sihsus",
                "condicao_notificacao_id_cid10",
                "usuario_contraceptivo_principal_id_sihsus",
                "usuario_contraceptivo_secundario_id_sihsus",
                "usuario_filhos_quantidade",
                "usuario_id_pre_natal",
                "usuario_ocupacao_id_cbo",
                "usuario_atividade_id_cnae",
                "usuario_vinculo_previdencia_id_sihsus",
                "autorizacao_gestor_motivo_id_sihsus",
                "autorizacao_gestor_tipo_id_sihsus",
                "autorizacao_gestor_id_cpf",
                "condicao_associada_id_cid10",
                "condicao_obito_id_cid10",
                "regra_contratual_id_cnes",
                "usuario_etnia_id_sus",
                "condicao_secundaria_1_tipo_id_sihsus",
                "condicao_secundaria_2_tipo_id_sihsus",
                "condicao_secundaria_3_tipo_id_sihsus",
                "condicao_secundaria_4_tipo_id_sihsus",
                "condicao_secundaria_5_tipo_id_sihsus",
                "condicao_secundaria_6_tipo_id_sihsus",
                "condicao_secundaria_7_tipo_id_sihsus",
                "condicao_secundaria_8_tipo_id_sihsus",
                "condicao_secundaria_9_tipo_id_sihsus",
            ],
            function=lambda elemento: (
                np.nan
                if pd.notna(elemento)
                and all(digito == "0" for digito in elemento)
                else elemento
            ),
        )
        # processar colunas lógicas
        .transform_columns(
            [
                "obito",
                "exame_vdrl",
                "usuario_homonimo",
                "gestacao_risco",
            ],
            function=_para_booleano,
        )
        # adicionar id
        .add_column("id", str())
        .transform_column("id", function=lambda _: uuid.uuid4().hex)
        # adicionar id do periodo
        .transform_column(
            "periodo_data_inicio",
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
        # HACK: ver https://github.com/pandas-dev/pandas/issues/25472
        .astype({col: "float" for col in COLUNAS_NUMERICAS})
        .astype(TIPOS_AIH_RD)
    )
    logger.debug(
        "Memória ocupada pelo DataFrame transformado: {memoria_usada:.2f} mB.",
        memoria_usada=(
            aih_rd_transformada.memory_usage(deep=True).sum() / 10 ** 6
        ),
    )
    return aih_rd_transformada


def obter_aih_rd(
    sessao: Session,
    uf_sigla: str,
    ano: int,
    mes: int,
    tabela_destino: str,
    teste: bool = False,
    **kwargs,
) -> None:
    """Baixa, transforma e carrega dados de procedimentos ambulatoriais.

    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov.
        uf_sigla: Sigla da Unidade Federativa cujas AIHs se pretende obter.
        ano: Ano das AIHs que se pretende obter.
        mes: Mês dos AIHs que se pretende obter.
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
        "Iniciando captura de autorizações de internações hospitalares para "
        + "Unidade Federativa '{uf_sigla}' na competencia de {mes}/{ano}.",
        uf_sigla=uf_sigla,
        ano=ano,
        mes=mes,
    )

    # obter tamanho do lote de processamento
    passo = int(os.getenv("IMPULSOETL_LOTE_TAMANHO", 100000))

    logger.info("Fazendo download do FTP público do DataSUS...")
    aih_rd = download(uf_sigla, year=ano, month=mes)

    # TODO: paralelizar transformação e carregamento de fatias do DataFrame
    # original
    aih_rd_transformada = transformar_aih_rd(sessao=sessao, aih_rd=aih_rd)
    sessao.commit()

    if teste:
        aih_rd_transformada = aih_rd_transformada.iloc[
            : min(1000, len(aih_rd_transformada)),
        ]
        if len(aih_rd_transformada) == 1000:
            logger.warning(
                "Arquivo de autorizações hospitalares truncado para 1000 "
                + "registros para fins de teste."
            )

    carregamento_status = carregar_dataframe(
        sessao=sessao,
        df=aih_rd_transformada,
        tabela_destino=tabela_destino,
        passo=passo,
        teste=teste,
    )
    if teste or carregamento_status != 0:
        sessao.rollback()
    else:
        sessao.commit()
