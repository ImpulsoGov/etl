# SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Obtém notificações de agravos de violência interpessoal ou autoprovocada."""


from __future__ import annotations

import os
import re
from datetime import date
from ftplib import error_perm
from typing import Final, Generator
from urllib.error import URLError

import janitor  # noqa: F401  # nopycln: import
import numpy as np
import pandas as pd
from frozendict import frozendict
from sqlalchemy.orm import Session
from uuid6 import uuid7

from impulsoetl.comum.condicoes_saude import e_cid10, remover_ponto_cid10
from impulsoetl.comum.datas import agora_gmt_menos3
from impulsoetl.comum.geografias import id_sim_para_id_impulso
from impulsoetl.loggers import logger
from impulsoetl.utilitarios.bd import carregar_dataframe
from impulsoetl.utilitarios.datasus_ftp import extrair_dbc_lotes

DE_PARA_AGRAVOS_VIOLENCIA: Final[frozendict] = frozendict(
    {
        "TP_NOT": "tipo_id_sinan",
        "ID_AGRAVO": "condicao_principal_id_cid10",
        "DT_NOTIFIC": "notificacao_data",
        "SEM_NOT": "notificacao_semana_epidemiologica_id_sinan",
        "NU_ANO": "notificacao_ano",
        "SG_UF_NOT": "notificacao_uf_id_ibge",
        "ID_MUNICIP": "notificacao_municipio_id_sus",
        "ID_REGIONA": "notificacao_regiao_saude_id_sus",
        "TP_UNI_EXT": "notificacao_estabelecimento_tipo_id_sinan",
        "NM_UNI_EXT": "notificacao_estabelecimento_nome",
        "CO_UNI_EXT": "notificacao_estabelecimento_id_sinan",
        "ID_UNIDADE": "notificacao_estabelecimento_id_scnes",
        "ID_RG_RESI": "usuario_residencia_regiao_saude_id_sus",
        "DT_OCOR": "ocorrencia_data",
        "SEM_PRI": "ocorrencia_semana_epidemiologica_id_sinan",
        "DT_NASC": "usuario_nascimento_data",
        "NU_IDADE_N": "usuario_idade_id_sinan",
        "CS_SEXO": "usuario_sexo_id_sinan",
        "CS_GESTANT": "usuario_gestacao_idade_id_sinan",
        "CS_RACA": "usuario_raca_cor_id_sinan",
        "CS_ESCOL_N": "usuario_escolaridade_id_sinan",
        "SG_UF": "usuario_residencia_uf_id_ibge",
        "ID_MN_RESI": "usuario_residencia_municipio_id_sus",
        "ID_PAIS": "usuario_residencia_pais_id_sigtap",
        "NDUPLIC": "duplicado",
        "DT_INVEST": "_nao_documentado_dt_invest",
        "ID_OCUPA_N": "usuario_ocupacao_id_cbo2002",
        "SIT_CONJUG": "usuario_estado_civil_id_sinan",
        "DEF_TRANS": "usuario_deficiencia_possui",
        "DEF_FISICA": "usuario_deficiencia_fisica_id_sinan",
        "DEF_MENTAL": "usuario_deficiencia_mental_id_sinan",
        "DEF_VISUAL": "usuario_deficiencia_visual_id_sinan",
        "DEF_AUDITI": "usuario_deficiencia_auditiva_id_sinan",
        "TRAN_MENT": "usuario_transtorno_mental_id_sinan",
        "TRAN_COMP": "usuario_transtorno_comportamento_id_sinan",
        "DEF_OUT": "usuario_deficiencia_outras_id_sinan",
        "DEF_ESPEC": "usuario_deficiencia_outras_descricao",
        "SG_UF_OCOR": "ocorrencia_uf_id_ibge",
        "ID_MN_OCOR": "ocorrencia_municipio_id_sus",
        "HORA_OCOR": "ocorrencia_hora",
        "LOCAL_OCOR": "ocorrencia_local_tipo_id_sinan",
        "LOCAL_ESPE": "ocorrencia_local_outros_descricao",
        "OUT_VEZES": "ocorreu_outras_vezes",
        "LES_AUTOP": "autoprovocada",
        "VIOL_FISIC": "violencia_fisica",
        "VIOL_PSICO": "violencia_psicologica_moral",
        "VIOL_TORT": "violencia_tortura",
        "VIOL_SEXU": "violencia_sexual",
        "VIOL_TRAF": "violencia_trafico_pessoas",
        "VIOL_FINAN": "violencia_financeira_economica",
        "VIOL_NEGLI": "violencia_negligencia_abandono",
        "VIOL_INFAN": "violencia_trabalho_infantil",
        "VIOL_LEGAL": "violencia_intervencao_legal",
        "VIOL_OUTR": "violencia_outras",
        "VIOL_ESPEC": "violencia_outras_descricao",
        "AG_FORCA": "meio_forca_corporal",
        "AG_ENFOR": "meio_enforcamento",
        "AG_OBJETO": "meio_objeto_contundente",
        "AG_CORTE": "meio_objeto_perfuro_cortante",
        "AG_QUENTE": "meio_objeto_quente",
        "AG_ENVEN": "meio_envenenamento",
        "AG_FOGO": "meio_arma_fogo",
        "AG_AMEACA": "meio_ameaca",
        "AG_OUTROS": "meio_outros",
        "AG_ESPEC": "meio_outros_descricao",
        "SEX_ASSEDI": "violencia_sexual_assedio",
        "SEX_ESTUPR": "violencia_sexual_estupro",
        "SEX_PUDOR": "_nao_documentada_sex_pudor",
        "SEX_PORNO": "violencia_sexual_pornografia_infantil",
        "SEX_EXPLO": "violencia_sexual_exploracao",
        "SEX_OUTRO": "violencia_sexual_outra",
        "SEX_ESPEC": "violencia_sexual_outra_descricao",
        "PEN_ORAL": "_nao_documentada_pen_oral",
        "PEN_ANAL": "_nao_documentada_pen_anal",
        "PEN_VAGINA": "_nao_documentada_pen_vaginal",
        "PROC_DST": "procedimentos_realizados_profilaxia_dst",
        "PROC_HIV": "procedimentos_realizados_profilaxia_hiv",
        "PROC_HEPB": "procedimentos_realizados_profilaxia_hepatite_b",
        "PROC_SANG": "procedimentos_realizados_coleta_sangue",
        "PROC_SEMEN": "procedimentos_realizados_coleta_semen",
        "PROC_VAGIN": "procedimentos_realizados_coleta_secrecao_vaginal",
        "PROC_CONTR": "procedimentos_realizados_contracepcao_emergencia",
        "PROC_ABORT": "procedimentos_realizados_aborto_legal",
        "CONS_ABORT": "_nao_documentado_cons_abort",
        "CONS_GRAV": "_nao_documentado_cons_grav",
        "CONS_DST": "_nao_documentado_cons_dst",
        "CONS_SUIC": "_nao_documentado_cons_suic",
        "CONS_MENT": "_nao_documentado_cons_ment",
        "CONS_COMP": "_nao_documentado_cons_comp",
        "CONS_ESTRE": "_nao_documentado_cons_estre",
        "CONS_OUTR": "_nao_documentado_cons_outr",
        "CONS_ESPEC": "_nao_documentado_cons_espec",
        "LESAO_NAT": "_nao_documentado_lesao_nat",
        "LESAO_ESPE": "_nao_documentado_lesao_espe",
        "LESAO_CORP": "_nao_documentado_lesao_corp",
        "NUM_ENVOLV": "envolvidos_numero_id_sinan",
        "REL_SEXUAL": "_nao_documentado_rel_sexual",
        "REL_PAI": "autor_relacao_pai",
        "REL_MAE": "autor_relacao_mae",
        "REL_PAD": "autor_relacao_padrasto",
        "REL_CONJ": "autor_relacao_conjuge",
        "REL_EXCON": "autor_relacao_exconjuge",
        "REL_NAMO": "autor_relacao_namorado",
        "REL_EXNAM": "autor_relacao_exnamorado",
        "REL_FILHO": "autor_relacao_filho",
        "REL_DESCO": "autor_relacao_desconhecido",
        "REL_IRMAO": "autor_relacao_irmao",
        "REL_CONHEC": "autor_relacao_amigo_conhecido",
        "REL_CUIDA": "autor_relacao_cuidador",
        "REL_PATRAO": "autor_relacao_patrao",
        "REL_INST": "autor_relacao_institucional",
        "REL_POL": "autor_relacao_agente_lei",
        "REL_PROPRI": "autor_relacao_propria_pessoa",
        "REL_OUTROS": "autor_relacao_outras",
        "REL_ESPEC": "autor_relacao_outras_descricao",
        "AUTOR_SEXO": "autor_sexo_id_sinan",
        "AUTOR_ALCO": "autor_alcoolizado",
        "ENC_SAUDE": "encaminhamentos_rede_saude",
        "ENC_TUTELA": "_nao_documentado_enc_tutela",
        "ENC_VARA": "_nao_documentado_enc_vara",
        "ENC_ABRIGO": "_nao_documentado_enc_abrigo",
        "ENC_SENTIN": "_nao_documentado_enc_sentin",
        "ENC_DEAM": "_nao_documentado_enc_deam",
        "ENC_DPCA": "_nao_documentado_enc_dpca",
        "ENC_DELEG": "_nao_documentado_enc_deleg",
        "ENC_MPU": "_nao_documentado_enc_mpu",
        "ENC_MULHER": "_nao_documentado_enc_mulher",
        "ENC_CREAS": "_nao_documentado_enc_creas",
        "ENC_IML": "_nao_documentado_enc_iml",
        "ENC_OUTR": "_nao_documentado_enc_outr",
        "ENC_ESPEC": "_nao_documentado_enc_espec",
        "REL_TRAB": "relacao_trabalho",
        "REL_CAT": "emissao_cat_id_sinan",
        "CIRC_LESAO": "circunstancia_id_cid10",
        "CLASSI_FIN": "_nao_documentado_classi_fin",
        "EVOLUCAO": "_nao_documentado_evolucao",
        "DT_OBITO": "_nao_documentado_dt_obito",
        "REL_MAD": "autor_relacao_madrasta",
        "TPUNINOT": "_nao_documentado_tpuninot",
        "ORIENT_SEX": "usuario_orientacao_sexual_id_sinan",
        "IDENT_GEN": "usuario_genero_id_sinan",
        "VIOL_MOTIV": "violencia_motivacao_id_sinan",
        "CICL_VID": "_nao_documentado_cicl_vid",
        "REDE_SAU": "_nao_documentado_rede_sau",
        "ASSIST_SOC": "encaminhamentos_assistencia_social",
        "REDE_EDUCA": "encaminhamentos_educacao",
        "ATEND_MULH": "encaminhamentos_atendimento_mulher",
        "CONS_TUTEL": "encaminhamentos_conselho_tutelar",
        "CONS_IDO": "encaminhamentos_conselho_idoso",
        "DELEG_IDOS": "encaminhamentos_delegacia_idoso",
        "DIR_HUMAN": "encaminhamentos_centro_direitos_humanos",
        "MPU": "encaminhamentos_ministerio_publico",
        "DELEG_CRIA": "encaminhamentos_delegacia_crianca_adolescente",
        "DELEG_MULH": "encaminhamentos_delegacia_mulher",
        "DELEG": "encaminhamentos_delegacia_outra",
        "INFAN_JUV": "encaminhamentos_justica_infancia_juventude",
        "DEFEN_PUBL": "encaminhamentos_defensoria_publica",
        "DT_ENCERRA": "conclusao_data",
    },
)

DE_PARA_AGRAVOS_VIOLENCIA_ADICIONAIS: Final[frozendict] = frozendict(
    {
        "NU_NOTIFIC": "id_sinan",
        "ZONA": "usuario_residencia_tipologia_id_sinan",
        "ZONA_OCOR": "ocorrencia_tipologia_id_sinan",
        "DT_DIGITA": "_nao_documentado_dt_digita",
        "DT_TRANSUS": "_nao_documentado_dt_transus",
        "DT_TRANSDM": "_nao_documentado_dt_transdm",
        "DT_TRANSSM": "_nao_documentado_dt_transsm",
        "DT_TRANSRM": "_nao_documentado_dt_transrm",
        "DT_TRANSRS": "_nao_documentado_dt_transrs",
        "DT_TRANSSE": "_nao_documentado_dt_transse",
        "NU_LOTE_V": "_nao_documentado_nu_lote_v",
        "NU_LOTE_H": "_nao_documentado_nu_lote_h",
        "IDENT_MICR": "_nao_documentado_ident_micr",
    }
)

TIPOS_AGRAVOS_VIOLENCIA: Final[frozendict] = frozendict(
    {
        "id_sinan": "object",
        "tipo_id_sinan": "object",
        "condicao_principal_id_cid10": "object",
        "notificacao_data": "datetime64[ns]",
        "notificacao_semana_epidemiologica_id_sinan": "object",
        "notificacao_ano": "Int64",
        "notificacao_uf_id_ibge": "object",
        "notificacao_municipio_id_sus": "object",
        "notificacao_regiao_saude_id_sus": "object",
        "notificacao_estabelecimento_tipo_id_sinan": "object",
        "notificacao_estabelecimento_nome": "object",
        "notificacao_estabelecimento_id_sinan": "object",
        "notificacao_estabelecimento_id_scnes": "object",
        "usuario_residencia_regiao_saude_id_sus": "object",
        "ocorrencia_data": "datetime64[ns]",
        "ocorrencia_semana_epidemiologica_id_sinan": "object",
        "usuario_nascimento_data": "datetime64[ns]",
        "usuario_idade_id_sinan": "string",
        "usuario_sexo_id_sinan": "object",
        "usuario_gestacao_idade_id_sinan": "object",
        "usuario_raca_cor_id_sinan": "object",
        "usuario_escolaridade_id_sinan": "object",
        "usuario_residencia_uf_id_ibge": "object",
        "usuario_residencia_municipio_id_sus": "object",
        "usuario_residencia_pais_id_sigtap": "object",
        "duplicado": "bool",
        "_nao_documentado_dt_invest": "object",
        "usuario_ocupacao_id_cbo2002": "object",
        "usuario_estado_civil_id_sinan": "object",
        "usuario_deficiencia_possui": "bool",
        "usuario_deficiencia_fisica_id_sinan": "object",
        "usuario_deficiencia_mental_id_sinan": "object",
        "usuario_deficiencia_visual_id_sinan": "object",
        "usuario_deficiencia_auditiva_id_sinan": "bool",
        "usuario_transtorno_mental_id_sinan": "object",
        "usuario_transtorno_comportamento_id_sinan": "object",
        "usuario_deficiencia_outras_id_sinan": "object",
        "usuario_deficiencia_outras_descricao": "object",
        "ocorrencia_uf_id_ibge": "object",
        "ocorrencia_municipio_id_sus": "object",
        "ocorrencia_hora": "object",
        "ocorrencia_local_tipo_id_sinan": "object",
        "ocorrencia_local_outros_descricao": "object",
        "ocorreu_outras_vezes": "bool",
        "autoprovocada": "bool",
        "violencia_fisica": "bool",
        "violencia_psicologica_moral": "bool",
        "violencia_tortura": "bool",
        "violencia_sexual": "bool",
        "violencia_trafico_pessoas": "bool",
        "violencia_financeira_economica": "bool",
        "violencia_negligencia_abandono": "bool",
        "violencia_trabalho_infantil": "bool",
        "violencia_intervencao_legal": "bool",
        "violencia_outras": "bool",
        "violencia_outras_descricao": "object",
        "meio_forca_corporal": "bool",
        "meio_enforcamento": "bool",
        "meio_objeto_contundente": "bool",
        "meio_objeto_perfuro_cortante": "bool",
        "meio_objeto_quente": "bool",
        "meio_envenenamento": "bool",
        "meio_arma_fogo": "bool",
        "meio_ameaca": "bool",
        "meio_outros": "bool",
        "meio_outros_descricao": "object",
        "violencia_sexual_assedio": "bool",
        "violencia_sexual_estupro": "bool",
        "_nao_documentada_sex_pudor": "object",
        "violencia_sexual_pornografia_infantil": "bool",
        "violencia_sexual_exploracao": "bool",
        "violencia_sexual_outra": "bool",
        "violencia_sexual_outra_descricao": "object",
        "_nao_documentada_pen_oral": "object",
        "_nao_documentada_pen_anal": "object",
        "_nao_documentada_pen_vaginal": "object",
        "procedimentos_realizados_profilaxia_dst": "bool",
        "procedimentos_realizados_profilaxia_hiv": "bool",
        "procedimentos_realizados_profilaxia_hepatite_b": "bool",
        "procedimentos_realizados_coleta_sangue": "bool",
        "procedimentos_realizados_coleta_semen": "bool",
        "procedimentos_realizados_coleta_secrecao_vaginal": "bool",
        "procedimentos_realizados_contracepcao_emergencia": "bool",
        "procedimentos_realizados_aborto_legal": "bool",
        "_nao_documentado_cons_abort": "object",
        "_nao_documentado_cons_grav": "object",
        "_nao_documentado_cons_dst": "object",
        "_nao_documentado_cons_suic": "object",
        "_nao_documentado_cons_ment": "object",
        "_nao_documentado_cons_comp": "object",
        "_nao_documentado_cons_estre": "object",
        "_nao_documentado_cons_outr": "object",
        "_nao_documentado_cons_espec": "object",
        "_nao_documentado_lesao_nat": "object",
        "_nao_documentado_lesao_espe": "object",
        "_nao_documentado_lesao_corp": "object",
        "envolvidos_numero_id_sinan": "object",
        "_nao_documentado_rel_sexual": "object",
        "autor_relacao_pai": "bool",
        "autor_relacao_mae": "bool",
        "autor_relacao_padrasto": "bool",
        "autor_relacao_conjuge": "bool",
        "autor_relacao_exconjuge": "bool",
        "autor_relacao_namorado": "bool",
        "autor_relacao_exnamorado": "bool",
        "autor_relacao_filho": "bool",
        "autor_relacao_desconhecido": "bool",
        "autor_relacao_irmao": "bool",
        "autor_relacao_amigo_conhecido": "bool",
        "autor_relacao_cuidador": "bool",
        "autor_relacao_patrao": "bool",
        "autor_relacao_institucional": "bool",
        "autor_relacao_agente_lei": "bool",
        "autor_relacao_propria_pessoa": "bool",
        "autor_relacao_outras": "bool",
        "autor_relacao_outras_descricao": "object",
        "autor_sexo_id_sinan": "object",
        "autor_alcoolizado": "bool",
        "encaminhamentos_rede_saude": "bool",
        "_nao_documentado_enc_tutela": "object",
        "_nao_documentado_enc_vara": "object",
        "_nao_documentado_enc_abrigo": "object",
        "_nao_documentado_enc_sentin": "object",
        "_nao_documentado_enc_deam": "object",
        "_nao_documentado_enc_dpca": "object",
        "_nao_documentado_enc_deleg": "object",
        "_nao_documentado_enc_mpu": "object",
        "_nao_documentado_enc_mulher": "object",
        "_nao_documentado_enc_creas": "object",
        "_nao_documentado_enc_iml": "object",
        "_nao_documentado_enc_outr": "object",
        "_nao_documentado_enc_espec": "object",
        "relacao_trabalho": "bool",
        "emissao_cat_id_sinan": "object",
        "circunstancia_id_cid10": "object",
        "_nao_documentado_classi_fin": "object",
        "_nao_documentado_evolucao": "object",
        "_nao_documentado_dt_obito": "object",
        "autor_relacao_madrasta": "bool",
        "_nao_documentado_tpuninot": "object",
        "usuario_orientacao_sexual_id_sinan": "object",
        "usuario_genero_id_sinan": "object",
        "violencia_motivacao_id_sinan": "object",
        "_nao_documentado_cicl_vid": "object",
        "_nao_documentado_rede_sau": "object",
        "encaminhamentos_assistencia_social": "bool",
        "encaminhamentos_educacao": "bool",
        "encaminhamentos_atendimento_mulher": "bool",
        "encaminhamentos_conselho_tutelar": "bool",
        "encaminhamentos_conselho_idoso": "bool",
        "encaminhamentos_delegacia_idoso": "bool",
        "encaminhamentos_centro_direitos_humanos": "bool",
        "encaminhamentos_ministerio_publico": "bool",
        "encaminhamentos_delegacia_crianca_adolescente": "bool",
        "encaminhamentos_delegacia_mulher": "bool",
        "encaminhamentos_delegacia_outra": "bool",
        "encaminhamentos_justica_infancia_juventude": "bool",
        "encaminhamentos_defensoria_publica": "bool",
        "conclusao_data": "datetime64[ns]",
        "id": "object",
        "periodo_id": "object",
        "unidade_geografica_id": "object",
        "criacao_data": "datetime64[ns]",
        "atualizacao_data": "datetime64[ns]",
        "usuario_residencia_tipologia_id_sinan": "object",
        "ocorrencia_tipologia_id_sinan": "object",
        "_nao_documentado_dt_digita": "object",
        "_nao_documentado_dt_transus": "object",
        "_nao_documentado_dt_transdm": "object",
        "_nao_documentado_dt_transsm": "object",
        "_nao_documentado_dt_transrm": "object",
        "_nao_documentado_dt_transrs": "object",
        "_nao_documentado_dt_transse": "object",
        "_nao_documentado_nu_lote_v": "object",
        "_nao_documentado_nu_lote_h": "object",
        "_nao_documentado_ident_micr": "object",
    },
)

COLUNAS_DATA: Final[list[str]] = [
    "notificacao_data",
    "ocorrencia_data",
    "usuario_nascimento_data",
    "conclusao_data",
]

COLUNAS_NUMERICAS: Final[list[str]] = [
    nome_coluna
    for nome_coluna, tipo_coluna in TIPOS_AGRAVOS_VIOLENCIA.items()
    if tipo_coluna.lower() == "int64" or tipo_coluna.lower() == "float64"
]

COLUNAS_BOOLEANAS: Final[list[str]] = [
    nome_coluna
    for nome_coluna, tipo_coluna in TIPOS_AGRAVOS_VIOLENCIA.items()
    if tipo_coluna.lower() == "bool"
]


def _para_booleano(valor: str) -> bool | float:
    """Transforma um valor binário '1' ou '2' em booleano. Suporta NaNs."""
    if valor == "1":
        return True
    elif valor == "2":
        return False
    else:
        return np.nan


def extrair_agravos_violencia(
    periodo_data_inicio: date,
    passo: int = 100000,
) -> Generator[pd.DataFrame, None, None]:
    """Extrai registros de notificações de agravo de violências do SINAN.

    Argumentos:
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

    try:
        return extrair_dbc_lotes(
            ftp="ftp.datasus.gov.br",
            caminho_diretorio="/dissemin/publicos/SINAN/DADOS/FINAIS/",
            arquivo_nome="VIOLBR{periodo_data_inicio:%y}.dbc".format(
                periodo_data_inicio=periodo_data_inicio,
            ),
            passo=passo,
        )
    except (error_perm, URLError):
        return extrair_dbc_lotes(
            ftp="ftp.datasus.gov.br",
            caminho_diretorio="/dissemin/publicos/SINAN/DADOS/PRELIM/",
            arquivo_nome="VIOLBR{periodo_data_inicio:%y}.dbc".format(
                periodo_data_inicio=periodo_data_inicio,
            ),
            passo=passo,
        )


def transformar_agravos_violencia(
    sessao: Session,
    agravos_violencia: pd.DataFrame,
    periodo_id: str,
    condicoes: str | None = None,
) -> pd.DataFrame:
    """Transforma um `DataFrame` de notificações de violência do SINAN.

    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov.
        agravos_violencia: objeto [`pandas.DataFrame`][] contendo os dados de
            um arquivo de disseminação de notificações de agravos relacionados
            a violências, conforme extraídos para uma competência (ano) pela
            função [`extrair_agravas_violencia()`][].
        periodo_id: Identificador único do período de referência do arquivo
            de disseminação no banco de dados da Impulso Gov, a ser
            acrescentado no DataFrame transformado.
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
        público do DataSUS. Verifique o [Informe Técnico][it-sim] para mais
        informações.

    [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
    [`pandas.DataFrame`]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    [`extrair_agravas_violencia()`]: impulsoetl.sinan.violencia.extrair_agravas_violencia
    [`pandas.DataFrame.query()`]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.query.html
    [estrutura-sinan]: https://drive.google.com/file/d/18El-e7gTYa5iBpWIRiSSRiyCq0r-xgDN/view?usp=sharing
    """
    logger.info(
        "Transformando DataFrame com {num_registros_do} notificações de "
        + "agravos.",
        num_registros_do=len(agravos_violencia),
    )
    logger.debug(
        "Memória ocupada pelo DataFrame original: {memoria_usada:.2f} mB.",
        memoria_usada=(
            agravos_violencia.memory_usage(deep=True).sum() / 10**6
        ),
    )

    # aplica condições de filtragem dos registros
    if condicoes:
        agravos_violencia = agravos_violencia.query(condicoes, engine="python")
        logger.info(
            "Registros após aplicar condições de filtragem: {num_registros}.",
            num_registros=len(agravos_violencia),
        )

    # Junta nomes de colunas e tipos adicionais aos obrigatórios
    de_para = dict(
        DE_PARA_AGRAVOS_VIOLENCIA, **DE_PARA_AGRAVOS_VIOLENCIA_ADICIONAIS
    )

    # corrigir nomes de colunas mal formatados
    agravos_violencia = agravos_violencia.rename_columns(
        function=lambda col: col.strip().upper(),
    )

    agravos_violencia_transformada = (
        agravos_violencia  # noqa: WPS221  # ignorar linha complexa no pipeline
        # adicionar colunas faltantes, com valores vazios
        .add_columns(
            **{
                coluna: ""
                for coluna in DE_PARA_AGRAVOS_VIOLENCIA_ADICIONAIS.keys()
                if not (coluna in agravos_violencia.columns)
            }
        )
        # renomear colunas
        .rename_columns(de_para)
        # processar colunas com datas
        .transform_columns(
            COLUNAS_DATA,
            function=lambda dt: pd.to_datetime(
                dt,
                format="%Y-%m-%d",  # noqa: WPS323
                errors="coerce",
            ),
        )
        .transform_column(
            "ocorrencia_hora",
            function=lambda hora: (
                # TODO: Corrigir hora > 24
                hora[:2] + ":" + hora[2:4]
                if re.match(r"([01][0-9]|2[0-3])[0-5][0-9]", hora)
                else np.nan
            ),
        )
        # processar colunas lógicas
        .transform_columns(
            COLUNAS_BOOLEANAS,
            function=_para_booleano,
        )
        # processar colunas com CIDs
        .transform_columns(
            [
                "condicao_principal_id_cid10",
                "circunstancia_id_cid10",
            ],
            function=remover_ponto_cid10,
        )
        # tratar como NA colunas com valores nulos
        .replace("", np.nan)
        # corrigir leitura de coluna de códigos de idade
        .transform_column(
            "usuario_idade_id_sinan",
            lambda cod: str(int(cod)).zfill(4) if pd.notna(cod) else pd.NA,
        )
        # adicionar id
        .add_column("id", str())
        .transform_column("id", function=lambda _: uuid7().hex)
        # adicionar id do periodo
        .assign(periodo_id=periodo_id)
        # adicionar id da unidade geografica
        .transform_column(
            "notificacao_municipio_id_sus",
            function=lambda id_sim: id_sim_para_id_impulso(
                sessao=sessao,
                id_sim=id_sim,
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
        .astype(TIPOS_AGRAVOS_VIOLENCIA)
    )

    logger.debug(
        "Memória ocupada pelo DataFrame transformado: {memoria_usada:.2f} mB.",
        memoria_usada=(
            agravos_violencia_transformada.memory_usage(deep=True).sum()
            / 10**6
        ),
    )
    return agravos_violencia_transformada


def obter_agravos_violencia(
    sessao: Session,
    periodo_id: str,
    periodo_data_inicio: date,
    tabela_destino: str,
    teste: bool = False,
    **kwargs,
) -> None:
    """Baixa, transforma e carrega notificações de violência do SINAN.

    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da Impulso Gov.
        periodo_id: Identificador único do período de referência da notificação
            do agravo no banco de dados da Impulso Gov.
        periodo_data_inicio: Dia de início da competência desejada,
            representado como um objeto [`datetime.date`][].
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
            [`transformar_do()`][].

    [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
    [`sqlalchemy.engine.Row`]: https://docs.sqlalchemy.org/en/14/core/connections.html#sqlalchemy.engine.Row
    [`datetime.date`]: https://docs.python.org/3/library/datetime.html#date-objects
    [`transformar_do()`]: impulsoetl.sim.do.transformar_do
    """
    logger.info(
        "Iniciando captura de notificações de agravos no ano de {:%Y}.",
        periodo_data_inicio,
    )

    # obter tamanho do lote de processamento
    if teste:
        passo = 1000
    else:
        passo = int(os.getenv("IMPULSOETL_LOTE_TAMANHO", 100000))

    agravos_violencia_lotes = extrair_agravos_violencia(
        periodo_data_inicio=periodo_data_inicio,
        passo=passo,
    )

    contador = 0
    for agravos_violencia_lote in agravos_violencia_lotes:
        agravos_violencia_transformada = transformar_agravos_violencia(
            sessao=sessao,
            agravos_violencia=agravos_violencia_lote,
            periodo_id=periodo_id,
            condicoes=kwargs.get("condicoes"),
        )

        carregamento_status = carregar_dataframe(
            sessao=sessao,
            df=agravos_violencia_transformada,
            tabela_destino=tabela_destino,
            passo=None,
            teste=teste,
        )
        if carregamento_status != 0:
            raise RuntimeError(
                "Execução interrompida em razão de um erro no "
                + "carregamento."
            )
        contador += len(agravos_violencia_lote)
        if teste and contador >= 1000:
            logger.info("Execução interrompida para fins de teste.")
            break

    if teste:
        logger.info("Desfazendo alterações realizadas durante o teste...")
        sessao.rollback()
        logger.info("Todas transações foram desfeitas com sucesso!")
