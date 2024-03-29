# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT

"""Processa relatório de financiamento para o formato usado no BD."""

from typing import Final

import pandas as pd
import numpy as np
from frozendict import frozendict
from prefect import task
from sqlalchemy.orm import Session

from impulsoetl.comum.geografias import id_sus_para_id_impulso
from impulsoetl.loggers import habilitar_suporte_loguru, logger

TIPOS_EGESTOR_FINANCIAMENTO: Final[frozendict] = frozendict(
    {
        "uf_sigla": str,
        "municipio_nome": str,
        "municipio_id_sus": str,
        "periodo_data_inicio": str,
        "periodo_quadrimestre": str,
        "academias_credenciadas": "int64",
        "academias_pagas": "int64",
        "pagamento_total": float,
        "pagamento_total_acs": float,
        "pagamento_adicional": float,
        "pagamento_desconto": float,
        "pagamento_microscopista_regular": float,
        "pagamento_microscopista_extra": float,
        "pagamento_eabp_estadual": float,
        "pagamento_eabp_municipal": float,
        "pagamento_equipes_adolescentes_socioeducacao": float,
        "pagamento_pse_estadual": float,
        "pagamento_pse_municipal": float,
        "acs": "int64",
        "pagamento_acs_regular": float,
        "pagamento_acs_extra": float,
        "equipes_esfrb_credenciadas": "int64",
        "equipes_esfrb_homologadas": "int64",
        "equipes_esfrb_pagas": "int64",
        "pagamento_esfrb": float,
        "pagamento_implantacao_esfrb": float,
        "embarcacoes": "int64",
        "unidades_apoio": "int64",
        "microscopistas": "int64",
        "auxiliares_enfermagem": "int64",
        "auxiliares_saude_bucal": "int64",
        "professor_nivel_superior": "int64",
        "pagamento_componentes_extras": float,
        "pagamento_adicional_componentes_extras": float,
        "pagamento_desconto_componentes_extras": float,
        "equipes_ecr_credenciadas": "int64",
        "equipes_ecr_homologadas": "int64",
        "equipes_ecr_mod1_pagas": "int64",
        "equipes_ecr_mod2_pagas": "int64",
        "equipes_ecr_mod3_pagas": "int64",
        "medicos_homologados": "int64",
        "enfermeiros_homologados": "int64",
        "cirurgioes_dentistas_homologados": "int64",
        "medicos_pagos": "int64",
        "enfermeiros_pagos": "int64",
        "cirurgioes_dentistas_pagos": "int64",
        "pagamento_medicos": float,
        "pagamento_enfermeiros": float,
        "pagamentos_cirurgioes_dentistas": float,
        "equipes_esb_40h_credenciadas": "int64",
        "equipes_esb_ch_diferenciada_credenciadas": "int64",
        "equipes_esb_40h_homologadas": "int64",
        "equipes_esb_chd_homologadas": "int64",
        "equipes_esb_40h_mod1_pagas": "int64",
        "equipes_esb_40h_mod2_pagas": "int64",
        "equipes_esb_chd_30h_pagas": "int64",
        "equipes_esb_chd_20h_pagas": "int64",
        "equipes_esb_40h_mod1_quilombolas_pagas": "int64",
        "equipes_esb_40h_mod2_quilombolas_pagas": "int64",
        "equipes_esb_implantacao": "int64",
        "pagamento_esb_custeio": float,
        "pagamento_esb_implantacao": float,
        "pagamento_esb_adicional": float,
        "pagamento_esb_desconto": float,
        "uom_credenciada": "int64",
        "uom_homologada": "int64",
        "uom_paga": "int64",
        "pagamento_custeio_uom": float,
        "pagamento_implantacao_uom": float,
        "pagamento_adicional_uom": float,
        "pagamento_desconto_uom": float,
        "pagamento_ceo_estadual": float,
        "pagamento_ceo_municipal": float,
        "pagamento_lrpd_estadual": float,
        "pagamento_lrpd_municipal": float,
        "usf_60h_homologadas": "int64",
        "usf_60h_sb_homologadas": "int64",
        "usf_75h_sb_homologadas": "int64",
        "usf_60h_simplificado_homologadas": "int64",
        "usf_60h_pagas": "int64",
        "usf_60h_sb_pagas": "int64",
        "usf_75h_sb_pagas": "int64",
        "usf_60h_simplificado_pagas": "int64",
        "pagamento_custeio": float,
        "pagamento_implantacao": float,
        "ubsf_credenciadas": "int64",
        "ubsf_homologadas": "int64",
        "ubsf_pagas": "int64",
        "pagamento_ubsf_custeio": float,
        "pagamento_ubsf_adicional": float,
        "pagamento_ubsf_desconto": float,
        "microscopistas": "int64",
        "auxiliares_enfermagem": "int64",
        "auxiliares_saude_bucal": "int64",
        "professor_nivel_superior": "int64",
        "pagamento_componentes_extras": float,
        "pagamento_adicional_componentes_extras": float,
        "pagamento_desconto_componentes_extras": float,
        "agentes_acs": "int64",
        "pagamento_extra": float,
        "pagamento_ajuste_adicional": float,
        "pagamento_ajuste_desconto": float,
        "equipes_homologadas": "int64",
        "equipes_pagas": "int64",
        "isf_nota": float,
        "esf": "int64",
        "eap_30h": "int64",
        "eap_20h": "int64",
        "pagamento_equipes_novas": float,
        "pagamento_desempenho": float,
        "pagamento_potencial": float,
        "esf_novas": "int64",
        "eap_30h_novas": "int64",
        "eap_20h_novas": "int64",
        "competencia_cnes": str,
        "municipio_tipologia": str,
        "municipio_populacao": "int64",
        "esf_credenciadas": "int64",
        "eap_credenciadas": "int64",
        "esf_homologadas": "int64",
        "eap_homologadas": "int64",
        "esf_pagas": "int64",
        "eap_20h_pagas": "int64",
        "eap_30h_pagas": "int64",
        "cadastro_esf_por_eap": "int64",
        "cadastro_potencial": "int64",
        "esf_quadrimestre": "int64",
        "eap_20h_quadrimestre": "int64",
        "eap_30h_quadrimestre": "int64",
        "populacao_vulneravel": "int64",
        "populacao_nao_vulneravel": "int64",
        "cadastro_pagamento": "int64",
        "valor_ponto_vulneravel": float,
        "valor_ponto_nao_vulneravel": float,
        "valor_ponto_por_equipe": float,
        "esf_completas": "int64",
        "esf_75_incompletas": "int64",
        "esf_50_incompletas": "int64",
        "esf_25_incompletas": "int64",
        "eap_20h_completas": "int64",
        "eap_20h_incompletas": "int64",
        "eap_30h_completas": "int64",
        "eap_30h_incompletas": "int64",
        "pagamento_total_equipes": float,
        "esf_novas_completas": "int64",
        "esf_novas_75_incompletas": "int64",
        "esf_novas_50_incompletas": "int64",
        "esf_novas_25_incompletas": "int64",
        "eap_20h_novas_completas": "int64",
        "eap_20h_novas_incompletas": "int64",
        "eap_30h_novas_completas": "int64",
        "eap_30h_novas_incompletas": "int64",
        "cadastro_potencial_equipes_novas": "int64",
        "pagamento_por_equipe": float,
        "pagamento_total_equipes_novas": float,
        "ecr": "int64",
        "ecr_cadastros": "int64",
        "ecr_ponto": float,
        "ecr_pagamento": float,
        "eapp": "int64",
        "eapp_cadastros": "int64",
        "eapp_ponto": float,
        "eapp_pagamento": float,
        "esfr": "int64",
        "easfr_cadastro_vulneravel": "int64",
        "easfr_cadastro_nao_vulneravel": "int64",
        "easfr_ponto_vulneravel": float,
        "easfr_ponto_nao_vulneravel": float,
        "easfr_pagamento_vulneravel": float,
        "easfr_pagamento_nao_vulneravel": float,
        "pagamento_capitacao_ponderada": float,
        "pagamento_complementacao_financeira": float,
    },
)


EGESTOR_FINANCIAMENTO_COLUNAS: Final[dict[str, str]] = {
    "UF": "uf_sigla",
    "MUNICÍPIOS": "municipio_nome",
    "MUNICÍPIO": "municipio_nome",
    "Municípios": "municipio_nome",
    "Município": "municipio_nome",
    "IBGE": "municipio_id_sus",
    "Parcela": "periodo_data_inicio",
    "Quadrimestre de Referência": "periodo_quadrimestre",
    "Qt. Academia Credenciado": "academias_credenciadas",
    "Qt. Academia Pago": "academias_pagas",
    "Valor Academia": "pagamento_total",
    "AJUSTE*": "pagamento_adicional",
    "Unnamed: 9": "pagamento_desconto",
    "Competência Financeira": "periodo_data_inicio",
    "Microscopista": "pagamento_microscopista_regular",
    """Microscopista
(Parcela extra)""": "pagamento_microscopista_extra",
    "Equipe Prisional": "pagamento_eabp_estadual",
    "Unnamed: 8": "pagamento_eabp_municipal",
    "Custeio Adolescentes em atendimento socioeducativo": "pagamento_equipes_adolescentes_socioeducacao",
    "PSE Estadual": "pagamento_pse_estadual",
    "PSE Municipal": "pagamento_pse_municipal",
    "Qt. ACS": "acs",
    "ACS": "pagamento_acs_regular",
    "Unnamed: 6": "pagamento_eabp_municipal",
    """ACS
(Parcela Extra)""": "pagamento_acs_extra",
    "Qt. ESFRB Credenciado": "equipes_esfrb_credenciadas",
    "Qt. ESFRB Homologado": "equipes_esfrb_homologadas",
    "Qt. ESFRB Pago": "equipes_esfrb_pagas",
    "Valor Custeio ESFRB": "pagamento_esfrb",
    "Valor Implantação ESFRB": "pagamento_implantacao_esfrb",
    "Unnamed: 11": "pagamento_desconto",
    "Componente Extra": "embarcacoes",
    "Unnamed: 13": "unidades_apoio",
    "Unnamed: 14": "microscopistas",
    "Unnamed: 15": "auxiliares_enfermagem",
    "Unnamed: 16": "auxiliares_saude_bucal",
    "Unnamed: 17": "professor_nivel_superior",
    "Valor Custeio Extras Ribeirinha": "pagamento_componentes_extras",
    "AJUSTE*.1": "pagamento_adicional_componentes_extras",
    "Unnamed: 20": "pagamento_desconto_componentes_extras",
    "Qt. eCR Credenciadas": "equipes_ecr_credenciadas",
    "Qt. eCR Homologadas": "equipes_ecr_homologadas",
    "Qt. eCR Modalidade I Pagas": "equipes_ecr_mod1_pagas",
    "Qt. eCR Modalidade II Pagas": "equipes_ecr_mod2_pagas",
    "Qt. eCR Modalidade III Pagas": "equipes_ecr_mod3_pagas",
    "Valor": "pagamento_total",
    "Unnamed: 12": "pagamento_desconto",
    "Qt. Médico Residente Homologado": "medicos_homologados",
    "Qt. Enfermeiro Residente Homologado": "enfermeiros_homologados",
    "Qt. Cirurgão-Dentista Residente Homologado": "cirurgioes_dentistas_homologados",
    "Qt. Médico Residente Pago": "medicos_pagos",
    "Qt. Enfermeiro Residente Pago": "enfermeiros_pagos",
    "Qt. Cirurgão-Dentista Residente Pago": "cirurgioes_dentistas_pagos",
    "Valor do pagamento Médico Residente": "pagamento_medicos",
    "Valor do pagamento Enfermeiro Residente": "pagamento_enfermeiros",
    "Valor do pagamento Cirurgião-Dentista Residente": "pagamentos_cirurgioes_dentistas",
    "Qt. ESB 40h Credenciada": "equipes_esb_40h_credenciadas",
    "Qt.ESB CH diferenciada Credenciada": "equipes_esb_ch_diferenciada_credenciadas",
    "Qt. ESB 40h Homologadas": "equipes_esb_40h_homologadas",
    "Qt.ESB CH diferenciada Homologadas": "equipes_esb_chd_homologadas",
    "Qt. ESB 40h Modalidade I Pagas": "equipes_esb_40h_mod1_pagas",
    "Qt. ESB 40h Modalidade II Pagas": "equipes_esb_40h_mod2_pagas",
    "Qt. ESB CH diferenciada - 30h semanais Pagas": "equipes_esb_chd_30h_pagas",
    "Qt. ESB CH diferenciada - 20h semanais Pagas": "equipes_esb_chd_20h_pagas",
    "Qt. ESB 40h Modalidade I - Quilombolas/ Assentados Pagas": "equipes_esb_40h_mod1_quilombolas_pagas",
    "Qt. ESB 40h Modalidade II - Quilombolas/ Assentados Pagas": "equipes_esb_40h_mod2_quilombolas_pagas",
    "Qt. ESB Implantação (somente ESB 40h)": "equipes_esb_implantacao",
    "Valor custeio ESB": "pagamento_esb_custeio",
    "Valor Implantação (somente ESB 40h)": "pagamento_esb_implantacao",
    "Unnamed: 19": "pagamento_esb_desconto",
    "Qt. UOM Credenciada": "uom_credenciada",
    "Qt. UOM Homologada": "uom_homologada",
    "Qt. UOM Paga": "uom_paga",
    "Valor de Custeio da UOM": "pagamento_custeio_uom",
    "Valor de Implantação da UOM": "pagamento_implantacao_uom",
    "Unnamed: 26": "pagamento_desconto_uom",
    "Valor de CEO Estadual": "pagamento_ceo_estadual",
    "Valor de CEO Municipal": "pagamento_ceo_municipal",
    "Valor de LRPD Estadual": "pagamento_lrpd_estadual",
    "Valor de LRPD Municipal": "pagamento_lrpd_municipal",
    "AJUSTE*.2": "pagamento_adicional_ceo_lrpd",
    "Unnamed: 32": "pagamento_desconto_ceo_lrpd",
    "Qt. USF 60h Homologados": "usf_60h_homologadas",
    "Qt. USF 60h com SB Homologados": "usf_60h_sb_homologadas",
    "Qt. USF 75h com SB Homologados": "usf_75h_sb_homologadas",
    "Qt. USF 60h Simplificado Homologados": "usf_60h_simplificado_homologadas",
    "Qt. USF 60h Pagos": "usf_60h_pagas",
    "Qt. USF 60h com SB Pagos": "usf_60h_sb_pagas",
    "Qt. USF 75h com SB Pagos": "usf_75h_sb_pagas",
    "Qt. USF 60h Simplificado Pagos": "usf_60h_simplificado_pagas",
    "Valor total do Custeio": "pagamento_custeio",
    "Valor total de Implantação": "pagamento_implantacao",
    "Qt. UBSF Credenciado": "ubsf_credenciadas",
    "Qt. UBSF Homologado": "ubsf_homologadas",
    "Qt. UBSF Pago": "ubsf_pagas",
    "Valor Custeio UBSF ": "pagamento_ubsf_custeio",
    "Unnamed: 10": "pagamento_ubsf_desconto",
    "Valor Custeio Extras Fluvial": "pagamento_componentes_extras",
    "Unnamed: 19": "pagamento_desconto_componentes_extras",
    "Qt. ACS (95% e 5%)": "agentes_acs",
    "Valor ACS (95% e 5%)": "pagamento_total_acs",
    "VALOR TOTAL": "pagamento_total",
    "Parcela Extra": "pagamento_extra",
    "Quantidade de equipes homologdas": "equipes_homologadas",
    "Quantidade de equipes pagas": "equipes_pagas",
    "Valor ": "pagamento_total",
    "Nota do ISF": "isf_nota",
    "Quantitativo de equipes homologadas e validas no SCNES no quadrimestre avaliado": "esf",
    "Valor do pagamento por desempenho - ISF": "pagamento_desempenho",
    "Valor  referente a 100% dos indicadores - Portaria nº 166, de 27 de janeiro de 2021": "pagamento_adicional_100_meta",
    "Quantitativo de equipes novas* homologadas e validas no SCNES na competência": "esf_novas",
    "VALOR PAGAMENTO POR DESEMPENHO - EQUIPES NOVAS*": "pagamento_equipes_novas",
    "VALOR TOTAL": "pagamento_total",
    "Comp. CNES": "competencia_cnes",
    "Tipologia": "municipio_tipologia",
    "População": "municipio_populacao",
    "Nº de equipes CREDENCIADAS": "esf_credenciadas",
    "Nº de equipes HOMOLOGADAS": "esf_homologadas",
    "Nº de equipes PAGAS": "esf_pagas",
    "Cálculo Quadrimestre": "cadastro_esf_por_eap",
    "Unnamed: 18": "eap_20h_quadrimestre",
    "Unnamed: 21": "populacao_nao_vulneravel",
    "Unnamed: 22": "cadastro_pagamento",
    "Unnamed: 23": "valor_ponto_vulneravel",
    "Unnamed: 24": "valor_ponto_nao_vulneravel",
    "Unnamed: 25": "valor_ponto_por_equipe",
    "Unnamed: 26": "pagamento_por_equipe_quadrimestre",
    "Composição das equipes para pagamento na competência": "esf_completas",
    "Unnamed: 28": "esf_75_incompletas",
    "Unnamed: 29": "esf_50_incompletas",
    "Unnamed: 30": "esf_25_incompletas",
    "Unnamed: 31": "eap_20h_completas",
    "Unnamed: 32": "eap_20h_incompletas",
    "Unnamed: 33": "eap_30h_completas",
    "Unnamed: 34": "eap_30h_incompletas",
    "Unnamed: 35": "pagamento_total_equipes",
    """Equipes Novas*
*no caso de cadastro de eSF ou eAP no SCNES ref""": "esf_novas_completas",
    "Equipes Novas* \n*no caso de cadastro de eSF ou eAP no SCNES referente a um novo credenciamento, o incentivo financeiro do pagamento da capitação será transferido mensalmente até o 2º recálculo, considerando o resultado de 100%  do alcance dos indicadores, conforme parágrafo único do art. 12-E do Título II da Pt Consolidação n6/2017.": "esf_novas_completas",
    "Unnamed: 37": "esf_novas_75_incompletas",
    "Unnamed: 38": "esf_novas_50_incompletas",
    "Unnamed: 39": "esf_novas_25_incompletas",
    "Unnamed: 40": "eap_20h_novas_completas",
    "Unnamed: 41": "eap_20h_novas_incompletas",
    "Unnamed: 42": "eap_30h_novas_completas",
    "Unnamed: 43": "eap_30h_novas_incompletas",
    "Unnamed: 44": "cadastro_potencial_equipes_novas",
    "Unnamed: 45": "pagamento_por_equipe",
    "Unnamed: 46": "pagamento_total_equipes_novas",
    "Equipes Específicas": "ecr",
    "Unnamed: 48": "ecr_cadastros",
    "Unnamed: 49": "ecr_ponto",
    "Unnamed: 50": "ecr_pagamento",
    "Unnamed: 51": "eapp",
    "Unnamed: 52": "eapp_cadastros",
    "Unnamed: 53": "eapp_ponto",
    "Unnamed: 54": "eapp_pagamento",
    "Unnamed: 55": "esfr",
    "Unnamed: 56": "easfr_cadastro_vulneravel",
    "Unnamed: 57": "easfr_cadastro_nao_vulneravel",
    "Unnamed: 58": "easfr_ponto_vulneravel",
    "Unnamed: 59": "easfr_ponto_nao_vulneravel",
    "Unnamed: 60": "easfr_pagamento_vulneravel",
    "Unnamed: 61": "easfr_pagamento_nao_vulneravel",
    "Valor da Capitação Ponderada": "pagamento_capitacao_ponderada",
    "Valor da Complementação Financeira": "pagamento_complementacao_financeira",
    "Unnamed: 65": "pagamento_desconto",
}

COLUNAS_NUMERICAS_DECIMAIS = [
    "pagamento_total",
    "pagamento_adicional",
    "pagamento_desconto",
    "pagamento_ajuste_desconto",
    "pagamento_esb_adicional",
    "pagamento_eabp_estadual",
    "pagamento_eabp_municipal",
    "pagamento_equipes_adolescentes_socioeducacao",
    "pagamento_pse_estadual",
    "pagamento_pse_municipal",
    "pagamento_acs_regular",
    "pagamento_acs_extra",
    "pagamento_desempenho",
    "pagamento_esfrb",
    "pagamento_implantacao_esfrb",
    "pagamento_componentes_extras",
    "pagamento_adicional_componentes_extras",
    "pagamento_desconto_componentes_extras",
    "pagamento_medicos",
    "pagamento_enfermeiros",
    "pagamentos_cirurgioes_dentistas",
    "pagamento_esb_custeio",
    "pagamento_esb_implantacao",
    "pagamento_esb_desconto",
    "pagamento_custeio_uom",
    "pagamento_implantacao_uom",
    "pagamento_adicional_uom",
    "pagamento_desconto_uom",
    "pagamento_ceo_estadual",
    "pagamento_ceo_municipal",
    "pagamento_lrpd_estadual",
    "pagamento_lrpd_municipal",
    "pagamento_adicional_ceo_lrpd",
    "pagamento_desconto_ceo_lrpd",
    "pagamento_custeio",
    "pagamento_implantacao",
    "pagamento_ubsf_custeio",
    "pagamento_ubsf_adicional",
    "pagamento_ubsf_desconto",
    "pagamento_total_acs",
    "pagamento_ajuste_adicional",
    "pagamento_adicional_100_meta",
    "pagamento_equipes_novas",
    "pagamento_extra",
    "pagamento_capitacao_ponderada",
    "pagamento_complementacao_financeira",
    "easfr_ponto_vulneravel",
    "easfr_ponto_nao_vulneravel",
    "easfr_pagamento_vulneravel",
    "easfr_pagamento_nao_vulneravel",
    "eapp_ponto",
    "pagamento_por_equipe",
    "pagamento_total_equipes_novas",
    "ecr_ponto",
    "valor_ponto_vulneravel",
    "valor_ponto_nao_vulneravel",
    "valor_ponto_por_equipe",
    "pagamento_total_equipes",
    "municipio_populacao",
    "esf_credenciadas",
    "eap_credenciadas",
    "esf_homologadas",
    "eap_homologadas",
    "esf_pagas",
    "eap_20h_pagas",
    "eap_30h_pagas",
    "cadastro_esf_por_eap",
    "cadastro_potencial",
    "esf_quadrimestre",
    "eap_20h_quadrimestre",
    "eap_30h_quadrimestre",
    "populacao_vulneravel",
    "populacao_nao_vulneravel",
    "cadastro_pagamento",
    "esf_completas",
    "esf_75_incompletas",
    "esf_50_incompletas",
    "esf_25_incompletas",
    "eap_20h_completas",
    "eap_20h_incompletas",
    "eap_30h_completas",
    "eap_30h_incompletas",
    "esf_novas_completas",
    "esf_novas_75_incompletas",
    "esf_novas_50_incompletas",
    "esf_novas_25_incompletas",
    "eap_20h_novas_completas",
    "eap_20h_novas_incompletas",
    "eap_30h_novas_completas",
    "eap_30h_novas_incompletas",
    "cadastro_potencial_equipes_novas",
    "ecr",
    "ecr_cadastros",
    "ecr_pagamento",
    "eapp",
    "eapp_cadastros",
    "eapp_pagamento",
    "esfr",
    "easfr_cadastro_vulneravel",
    "easfr_cadastro_nao_vulneravel",
]


def formata_valores_monetarios(
    df_extraido: pd.DataFrame,
) -> pd.DataFrame:

    for coluna in COLUNAS_NUMERICAS_DECIMAIS:
        df_coluna = []
        if coluna in df_extraido.columns:
            for string in df_extraido[coluna]:
                string = (
                    str(string)
                    .replace("R$ ", "")
                    .replace(".", "")
                    .replace(",", ".")
                )
                df_coluna.append(string)
            df_extraido[coluna] = df_coluna
    return df_extraido


def renomeia_colunas_repetidas(
    df_extraido: pd.DataFrame, aba: str
) -> pd.DataFrame:

    if aba in ["Ações Est. - SB"]:
        df_extraido.rename(
            columns={
                "AJUSTE*": "pagamento_esb_adicional",
                "AJUSTE*.1": "pagamento_adicional_uom",
                "Unnamed: 19": "pagamento_esb_desconto",
                "Unnamed: 16": "pagamento_desconto",
            },
            inplace=True,
        )
    elif aba in [
        "Ações Est. - Residência",
        "Ações Est. - SNH",
        "Ações Estratégicas - Outros",
    ]:
        df_extraido.rename(
            columns={
                "AJUSTE*": "pagamento_adicional",
                "Valor Total do Custeio": "pagamento_total",
                "Unnamed: 16": "pagamento_desconto",
                "Unnamed: 14": "pagamento_desconto",
            },
            inplace=True,
        )
    elif aba in ["ACS"]:
        df_extraido.rename(
            columns={
                "AJUSTE*": "pagamento_ajuste_adicional",
                "Unnamed: 9": "pagamento_ajuste_desconto",
            },
            inplace=True,
        )
    elif aba in ["Desempenho ISF"]:
        df_extraido.rename(
            columns={
                "Unnamed: 8": "eap_30h",
                "Unnamed: 9": "eap_20h",
                "Unnamed: 13": "eap_30h_novas",
                "Unnamed: 14": "eap_20h_novas",
                "Unnamed: 17": "pagamento_desconto",
            },
            inplace=True,
        )
    elif aba in ["Ações Est. - UBSF"]:
        df_extraido.rename(
            columns={
                "AJUSTE*": "pagamento_ubsf_adicional",
                "AJUSTE*.1": "pagamento_adicional_componentes_extras",
                "Unnamed: 12": "unidades_apoio",
                "Unnamed: 13": "microscopistas",
                "Unnamed: 14": "auxiliares_enfermagem",
                "Unnamed: 15": "auxiliares_saude_bucal",
                "Unnamed: 16": "professor_nivel_superior",
            },
            inplace=True,
        )
    elif aba in ["Capitação Ponderada"]:
        df_extraido.rename(
            columns={
                "Unnamed: 9": "eap_credenciadas",
                "Unnamed: 11": "eap_homologadas",
                "Unnamed: 13": "eap_20h_pagas",
                "Unnamed: 14": "eap_30h_pagas",
                "Unnamed: 16": "cadastro_potencial",
                "Unnamed: 17": "esf_quadrimestre",
                "Unnamed: 19": "eap_30h_quadrimestre",
                "Unnamed: 20": "populacao_vulneravel",
            },
            inplace=True,
        )
    return df_extraido


def garantir_tipos_dados(df_extraido: pd.DataFrame) -> pd.DataFrame:

    for coluna in TIPOS_EGESTOR_FINANCIAMENTO:
        if coluna in df_extraido.columns:
            df_tipos = dict(
                zip([coluna], [TIPOS_EGESTOR_FINANCIAMENTO[coluna]])
            )
            df_extraido.astype(df_tipos, errors="ignore").where(
                df_extraido.notna(), None
            )
            print(df_extraido[coluna])
            
    return df_extraido


@task(
    name="Transformar Relatórios de Financiamento",
    description=(
        "Transforma os dados dos relatórios de financiamento da Atenção "
        + "Primária à Saúde extraídos do eGestor Atenção Básica."
    ),
    tags=["aps", "egestor", "financiamento", "transformacao"],
    retries=0,
    retry_delay_seconds=None,
)
def tratamento_dados(
    sessao: Session,
    df_extraido: pd.DataFrame,
    aba: str,
    periodo_data_inicio: str,
    periodo_id: str,
) -> pd.DataFrame:
    """Trata dados capturados do relatório de financiamento APS do egestor

    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov.
        df_extraido: [`DataFrame`][] contendo os dados capturados no relatório de Indicadores do Sisab
        (conforme retornado pela função
            [`extrair_dados()`][]).
        aba: Nome da aba do relatório
        periodo_data_inicio: Data do mês da competência em referência
        periodo_id: Código de identificação do período

    Retorna:
        Objeto [`pandas.DataFrame`] com os dados enriquecidos e tratados.

            [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
            [`pandas.DataFrame`]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    """

    habilitar_suporte_loguru()
    logger.info("Iniciando o tratamento dos dados...")
    df_extraido.drop(df_extraido.index[0], inplace=True)

    logger.info("Renomeando colunas para padrão do banco...")
    df_extraido = renomeia_colunas_repetidas(df_extraido=df_extraido, aba=aba)
    df_extraido.rename(
        EGESTOR_FINANCIAMENTO_COLUNAS, axis="columns", inplace=True
    )
    df_extraido.drop(columns=["Unnamed: 0"], inplace=True)

    logger.info("Formantando campos númericos...")
    df_extraido = formata_valores_monetarios(df_extraido=df_extraido)

    logger.info("Enriquecendo tabela...")
    df_extraido["periodo_data_inicio"] = periodo_data_inicio
    df_extraido["periodo_id"] = periodo_id
    df_extraido["unidade_geografica_id"] = df_extraido[
        "municipio_id_sus"
    ].apply(
        lambda municipio_id_sus: id_sus_para_id_impulso(
            sessao=sessao, id_sus=municipio_id_sus
        )
    )

    logger.info("Checa os tipos dos dados ...")
    df_extraido = garantir_tipos_dados(df_extraido=df_extraido)

    df_extraido.reset_index(drop=True, inplace=True)
    logger.info("Dados transformados ...")

    return df_extraido
